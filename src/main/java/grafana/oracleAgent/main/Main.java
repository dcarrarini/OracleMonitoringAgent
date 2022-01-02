package grafana.oracleAgent.main;

import grafana.oracleAgent.db.MysqlDB;
import grafana.oracleAgent.db.OracleDB;
import grafana.oracleAgent.db.OracleQuery;
import it.sauronsoftware.cron4j.Scheduler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    static { System.setProperty("logback.configurationFile", "cfg/logback.xml");}
    private static final Logger log
            = LoggerFactory.getLogger(Main.class);
    static Properties configurator = PropertiesReader.getProperties();
    //Carico dbid
    public static String DBID = configurator.getProperty("DBID");
    //Carico le schedulazioni
    static String cronSchedulerTablespace = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-TABLESPACES");
    static String cronSchedulerTopConsuming = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-TOPCONSUMING");
    static String cronSchedulerTempBySession = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-TEMPBYSESSION");
    static String cronSchedulerSgaUsage = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-SGAUSAGE");
    static String cronSchedulerExportDatafileIO = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-DATAFILEIO");
    static String cronSchedulerExportServerInfo = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-SERVER-INFO");
    static String cronSchedulerArchiveLogMode = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-ARCHIVELOG-MODE");
    static String cronSchedulerSessionsDetails = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-SESSIONS-DETAILS");
    static String cronSchedulerRedoLog = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-REDO-LOG");
    static String cronSchedulerDBBasicInfo = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-DB-BASIC-INFO");
    static String cronSchedulerTablesInfo = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-TABLES-INFO");
    static String cronSchedulerTop10Tables = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-TOP-10-TABLES");
    static String cronSchedulerStaleStats = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-STALE-STATS");

    public static void main(String[] args)  {
        log.info("Oracle agent monitoring started");

        OracleDB odb = new OracleDB();
        Connection OracleConnection = odb.connect();
        MysqlDB mdb = new MysqlDB();
        Connection MysqlConnection = mdb.getMYSQLDBConnection();

        try {
            log.info("Oracle Connection MetaData:" + OracleConnection.getMetaData());
            log.info("MySQL Connection MetaData:" + MysqlConnection.getMetaData());
        } catch (SQLException e) {
            log.error("MainSQLException", e);
        }


        // Crea l'istanza dello scheduler.
        Scheduler schedulerTablespaces = new Scheduler();
        // Schedula un task, che sarà eseguito ogni minuto.
        schedulerTablespaces.schedule(cronSchedulerTablespace, new Runnable() {
            public void run() {
                OracleQuery otbs = new OracleQuery();
                otbs.exportTablespace(OracleConnection, MysqlConnection);
                log.info("schedulerTablespaces - run completed");
            }
        });


        Scheduler schedulerTopConsuming = new Scheduler();
        schedulerTopConsuming.schedule(cronSchedulerTopConsuming, new Runnable() {
            public void run() {
                OracleQuery otbs = new OracleQuery();
                otbs.exportSQLTOPConsumingMoreCPU(OracleConnection, MysqlConnection);
                otbs.exportTop10CPUconsumingSession(OracleConnection, MysqlConnection);
                otbs.exportCurrentActiveSession(OracleConnection, MysqlConnection);
                otbs.exportSgaUsage(OracleConnection, MysqlConnection);
                log.info("schedulerTopConsuming - run completed");
            }
        });

        Scheduler exporttempBySession = new Scheduler();
        exporttempBySession.schedule(cronSchedulerTempBySession, new Runnable() {
            public void run() {
                OracleQuery otbs = new OracleQuery();
                otbs.exporttempBySession(OracleConnection, MysqlConnection);
                log.info("exporttempBySession - run Completed");
            }
        });

        Scheduler exportSgaUsage = new Scheduler();
        exportSgaUsage.schedule(cronSchedulerSgaUsage, new Runnable() {
            public void run() {
                OracleQuery otbs = new OracleQuery();
                otbs.exporttempBySession(OracleConnection, MysqlConnection);
                log.info("exportSgaUsage - run Completed");
            }
        });

        Scheduler exportDatafileIO = new Scheduler();
        exportDatafileIO.schedule(cronSchedulerExportDatafileIO, new Runnable() {
            public void run() {
                OracleQuery otbs = new OracleQuery();
                otbs.exportDatafileIO(OracleConnection, MysqlConnection);
                log.info("exportDatafileIO - run Completed");
            }
        });

        Scheduler exportServerInfo = new Scheduler();
        exportDatafileIO.schedule(cronSchedulerExportServerInfo, new Runnable() {
            public void run() {
                OracleQuery otbs = new OracleQuery();
                otbs.exportServerInfo(OracleConnection, MysqlConnection);
                log.info("exportDatafileIO - run Completed");
            }
        });

        Scheduler exporterArchiveLogMode = new Scheduler();
        exporterArchiveLogMode.schedule(cronSchedulerArchiveLogMode, new Runnable() {
            public void run() {
                OracleQuery otbs = new OracleQuery();
                otbs.archiveLogMode(OracleConnection, MysqlConnection);
                //otbs.exporterRedoLOG(OracleConnection, MysqlConnection);
                log.info("exporterArchiveLogMode - run Completed");
            }
        });

        Scheduler exporterSessionsDetails = new Scheduler();
        exporterSessionsDetails.schedule(cronSchedulerSessionsDetails, new Runnable() {
            public void run() {
                OracleQuery otbs = new OracleQuery();
                otbs.exporterSessions(OracleConnection, MysqlConnection);
                otbs.exporterRunningSQL(OracleConnection, MysqlConnection);
                log.info("exporterSessionsDetails - run Completed");
            }
        });

        Scheduler exporterRedoLog = new Scheduler();
        exporterRedoLog.schedule(cronSchedulerRedoLog, new Runnable() {
            public void run() {
                OracleQuery otbs = new OracleQuery();
                otbs.exporterRedoLOG(OracleConnection, MysqlConnection);
                log.info("exporterRedoLog - run Completed");
            }
        });

        Scheduler exporterDBBasicInfo = new Scheduler();
        exporterDBBasicInfo.schedule(cronSchedulerDBBasicInfo, new Runnable() {
            public void run() {
                OracleQuery otbs = new OracleQuery();
                otbs.exporterDBInfo(OracleConnection, MysqlConnection);
                otbs.exporterDBVersion(OracleConnection, MysqlConnection);
                log.info("exporterRedoLog - run Completed");
            }
        });

        Scheduler exporterTablesInfo = new Scheduler();
        exporterTablesInfo.schedule(cronSchedulerTablesInfo, new Runnable() {
            public void run() {
                OracleQuery otbs = new OracleQuery();
                otbs.exporterTablesInfo(OracleConnection, MysqlConnection);
                log.info("exporterTablesInfo - run Completed");
            }
        });

        Scheduler exporterTop10Tables = new Scheduler();
        exporterTop10Tables.schedule(cronSchedulerTop10Tables, new Runnable() {
            public void run() {
                OracleQuery otbs = new OracleQuery();
                otbs.exporterTop10Tables(OracleConnection, MysqlConnection);
                log.info("exporterTop10Tables - run Completed");
            }
        });

        Scheduler exporterStaleStats = new Scheduler();
        exporterStaleStats.schedule(cronSchedulerStaleStats, new Runnable() {
            public void run() {
                OracleQuery otbs = new OracleQuery();
                otbs.exporterStaleStats(OracleConnection, MysqlConnection);
                log.info("exporterStaleStats - run Completed");
            }
        });


        // Avvia lo scheduler.
        schedulerTablespaces.start();
        schedulerTopConsuming.start();
        exporttempBySession.start();
        exportSgaUsage.start();
        exportDatafileIO.start();
        exporterArchiveLogMode.start();
        exporterSessionsDetails.start();
        exporterRedoLog.start();
        exporterDBBasicInfo.start();
        exporterTablesInfo.start();
        exporterTop10Tables.start();
        exporterStaleStats.start();
        /*
        // Lascia in esecuzione per dieci minuti.
        try {
            Thread.sleep(1000L * 60L * 10L);
        } catch (InterruptedException e) {
            log.error(e);
        }
        // Arresta lo scheduler.
        schedulerTablespaces.stop();
        */
    }


}
