package grafana.oracleAgent.main;

import grafana.oracleAgent.db.MysqlDB;
import grafana.oracleAgent.db.OracleDB;
import grafana.oracleAgent.db.OracleQuery;
import it.sauronsoftware.cron4j.Scheduler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Main {
    static Logger log = Logger.getLogger(Main.class.getName());
    static Properties configurator = PropertiesReader.getProperties();
    //Carico le schedulazioni
    static String cronSchedulerTablespace = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-TABLESPACES");
    static String cronSchedulerTopConsuming = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-TOPCONSUMING");
    static String cronSchedulerTempBySession = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-TEMPBYSESSION");
    static String cronSchedulerSgaUsage = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-SGAUSAGE");
    static String cronSchedulerExportDatafileIO = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-DATAFILEIO");
    static String cronSchedulerExportServerInfo = configurator.getProperty("CRONEXPRESSION-GRAFANA-ORACLE-AGENT-SERVER-INFO");

    public static void main(String[] args)  {
        PropertyConfigurator.configure("./cfg/log4j.properties");
        OracleDB odb = new OracleDB();
        Connection OracleConnection = odb.connect();
        MysqlDB mdb = new MysqlDB();
        Connection MysqlConnection = mdb.getMYSQLDBConnection();
        try {
            log.info("Oracle Connection MetaData:" + OracleConnection.getMetaData());
            log.info("MySQL Connection MetaData:" + MysqlConnection.getMetaData());
        } catch (SQLException e) {
            log.error(e);
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




        // Avvia lo scheduler.
        schedulerTablespaces.start();
        schedulerTopConsuming.start();
        exporttempBySession.start();
        exportSgaUsage.start();
        exportDatafileIO.start();

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
