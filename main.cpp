#include <QCoreApplication>
#include <QEvent>
#include <sstream>
#include <QMutex>
#include <QMutexLocker>
#include <iostream>
#include <QNetworkAccessManager>
#include <QNetworkReply>
#include <QDateTime>
#include <QJsonDocument>
#include <QJsonObject>
#include <QVariant>
#include <utility>
#include <QTextStream>
#include <exception>
#include <QMultiMap>
#include <QFile>


class Log : public QTextStream
{
public:
    Log() { setString(&str, QIODevice::WriteOnly); }
    template <typename ...T>
    Log(const char *fmt,T&&...args) {
        setString(&str, QIODevice::WriteOnly);
        QString s = QString::asprintf(fmt,std::forward<T>(args)...);
        (*this) << s;
    }
    virtual ~Log() { finishPrt(); }
protected:
    void finishPrt() {
        flush();
        setString(0);
        if (str.isNull()) return;
        QMutexLocker l(&mut);
        if (str.isEmpty() || !str.endsWith("\n")) str += "\n";
        std::cout << str.toUtf8().constData();
        str = QString::null;
    }

private:
    static QMutex mut;
    QString str;
};

class Fatal : public Log
{
public:
    Fatal() {}
    template <typename ... T>
    Fatal(const char *fmt, T&&...args) : Log(fmt, std::forward<T>(args)...) {}
    ~Fatal() {
        finishPrt();
        std::exit(1); // exit immediately
    }
};

/*static*/ QMutex Log::mut;


struct Block
{
    Block() : height(0), time(0) {}
    Block(unsigned h, const QString &hh, qint64 t) : height(h), hash(hh), time(t) {}
    unsigned height;
    QString hash;
    qint64 time;
};

typedef QMap<unsigned, Block> BlockMap;
typedef QMap<qint64, Block> BlockTimeMap;
typedef QMultiMap<qint64, Block> BlockTimeMultiMap;

class MainObj : public QObject
{
public:
    const int NDAYS;
    explicit MainObj(int ndays) : NDAYS(ndays) {}

protected:
    bool event(QEvent *event);
private:
    void appEntry();
    void getNext();
    void finished(QNetworkReply *);
    void processResults(const QJsonDocument &d);
    void printBlocks() const;
    void printStatsAndExit() const;
    void saveCsv() const;

    QNetworkAccessManager mgr;
    int daysLeft, nDupeTimes;
    QByteArray data;

    BlockMap blocks;
    BlockTimeMap blocksByTime;
    BlockTimeMultiMap blocksByTimeMulti;
};

bool MainObj::event(QEvent *event)
{
    if (event->type() == QEvent::User) {
        event->accept();
        appEntry();
        return true;
    }
    return QObject::event(event);
}

void MainObj::appEntry()
{
    Log() << "Connecting to blockchain.info to download last " << (daysLeft=NDAYS) << " days' worth of block times...";
    connect(&mgr,&QNetworkAccessManager::finished, this, [this](QNetworkReply*reply){finished(reply);});
    getNext();
}

void MainObj::getNext()
{
    Log("Received %d blocks so far, currently downloading blocks for day %d",blocksByTime.size(), daysLeft-NDAYS);
    qint64 ts = QDateTime::currentMSecsSinceEpoch();
    if (!blocksByTime.isEmpty()) {
        static const qint64 aday_ms = 60ll*60ll*24ll*1000ll;
        ts = blocksByTime.first().time*1000ll - aday_ms;
    } else {
        nDupeTimes = 0;
    }
    QString urlString = QString().sprintf("https://blockchain.info/blocks/%lld?format=json",ts);
    QNetworkReply *r = mgr.get(QNetworkRequest(QUrl(urlString)));
    connect(r, static_cast<void(QNetworkReply::*)(QNetworkReply::NetworkError)>(&QNetworkReply::error),this,[r](QNetworkReply::NetworkError c){Fatal("Got network error code: %d, exiting",int(c));});
    connect(r, &QIODevice::readyRead, this, [this,r]{
         data += r->readAll();
    });
}



void MainObj::finished(QNetworkReply *r)
{
    data += r->readAll();
//    Log("Got data length: %d\n%s\n", data.length(), data.constData());
    QJsonParseError e;
    QJsonDocument d = QJsonDocument::fromJson(data, &e);
    if (d.isNull()) {
        Fatal("error parsing JSON: %s", e.errorString().toLatin1().constData());
    } else {
        processResults(d);
        //printBlocks();
    }
    data.clear();
    r->deleteLater();
    if (--daysLeft > 0)
        getNext();
    else
        printStatsAndExit();
}

void MainObj::printStatsAndExit() const
{
    int nBlocks = blocksByTime.size() + nDupeTimes;
    double days = blocksByTime.empty() ? 0.0 : double(blocksByTime.last().time-blocksByTime.first().time)/60./60./24.;
    Log("Got %d blocks, spanning %g days, computing stats...",nBlocks, days);
    double avg = 0.;
    qint64 last = -1, min = nDupeTimes ? 0ll : LONG_MAX, max = -1;
    qint64 mycutoff = 7ll*60ll+30ll; // 7.5 mins
    qint64 cutoffdeltasums = 0ll, nsums = 0ll;
    for (const Block & b : blocksByTime) {
        if (last > -1) {
            qint64 delta = b.time-last;
            if (delta < 0LL) {
                Fatal("Block=%d delta=%lld! Aborting!", b.height, delta);
            } else {
                avg += double(delta) / double(nBlocks>0?nBlocks:1);
                if (delta < min) min = delta;
                if (delta > max) max = delta;
            }
            if (delta >= mycutoff) {
                cutoffdeltasums += delta-mycutoff;
                ++nsums;
            }
        }
        last = b.time;
    }
    Log("Avg time: %f mins, min=%f mins, max=%f mins", avg/60., min/60., max/60.);
    Log("Craig vs Peter R test -- cutoff time: %f mins, avg: %f mins", mycutoff/60., double(cutoffdeltasums/double(nsums))/60.);
    saveCsv();
    Log("Done.");
    qApp->exit(0);
}

void MainObj::saveCsv() const
{
    QFile f("blocks_sorted_by_height.csv"), f2("blocks_sorted_by_timestamp.csv");
    if (!f.open(QIODevice::WriteOnly))
        Fatal("Could not open %s in current directory for writing!",f.fileName().toUtf8().constData());
    f.write(QString().sprintf("#BlockHeight,BlockTimeUTC,BlockHash\n").toUtf8());
    for(const Block & b: blocks) {
        f.write(QString().sprintf("%d,%lld,%s\n",b.height,b.time,b.hash.toUtf8().constData()).toUtf8());
    }
    f.flush();
    f.close();
    if (!f2.open(QIODevice::WriteOnly))
        Fatal("Could not open %s in current directory for writing!",f2.fileName().toUtf8().constData());
    f2.write(QString().sprintf("#BlockTimeUTC,BlockHeight,BlockHash\n").toUtf8());
    for(const Block & b: blocks) {
        f2.write(QString().sprintf("%lld,%d,%s\n",b.time,b.height,b.hash.toUtf8().constData()).toUtf8());
    }
    f2.flush();
    f2.close();
    Log() << "Saved " << f.fileName() << " and " << f2.fileName() << " to the current directory";
}

void MainObj::processResults(const QJsonDocument &d)
{
    if (d.isObject()) {
        QVariantMap vm = d.object().toVariantMap();
        QList<QVariant> vl;
        if (!vm.contains("blocks") || (vl=vm["blocks"].toList()).isEmpty() ) {
            Fatal("Blocks array not found");
        } else {
            for(const QVariant & v : vl) {
                vm = v.toMap();
                if (vm.isEmpty()) {
                    Fatal("variantMap is empty");
                } else {
                    Block b;
                    bool ok1,ok2;
                    b.height = vm["height"].toUInt(&ok1);
                    b.hash = vm["hash"].toString();
                    b.time = vm["time"].toLongLong(&ok2);
                    bool main = vm["main_chain"].toBool();
                    if (!main) continue;
                    if (!ok1 || !ok2) Fatal("Parse error");
                    else {
                        if (blocks.contains(b.height))
                            Log("Dupe block found %d (dup2: time=%lld hash=%s / dup1: time=%lld hash=%s)", b.height
                                , b.time, b.hash.toUtf8().constData()
                                , blocks[b.height].time, blocks[b.height].hash.toUtf8().constData());
                        if (blocksByTime.contains(b.time)) {
                            Log("Dupe timestamp found %d (dup2: height=%d hash=%s / dup1: height=%d hash=%s)", b.time
                                , b.height, b.hash.toUtf8().constData()
                                , blocksByTime[b.time].height, blocksByTime[b.time].hash.toUtf8().constData());
                            ++nDupeTimes;
                        }
                        blocks[b.height] = b;
                        blocksByTime[b.time] = b;
                        blocksByTimeMulti.insert(b.time, b);
                    }
                }
            }
        }
    } else {
        Fatal("Unknown Json type");
    }
}

void MainObj::printBlocks() const
{
    for (auto & b : blocks) {
        Log() << b.height << ":" << b.hash << ":" << b.time;
    }
}

int main(int argc, char *argv[])
{
    int ndays;
    if (argc < 2 || (ndays=QString(argv[1]).toInt()) <= 0) {
        Log("Please pass the number of days' worth of blocks to download as the first argument");
        return 1;
    }
    MainObj obj(ndays);
    QCoreApplication app(argc, argv);
    app.postEvent(&obj, new QEvent(QEvent::User));
    return app.exec();
}
