package grafana.oracleAgent.db;

import grafana.oracleAgent.csv.TablespaceCSV;
import grafana.oracleAgent.main.PropertiesReader;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class OracleDB {
    static Properties configurator = PropertiesReader.getProperties();
    String ORACLE_JDBC_DRIVER = configurator.getProperty("ORACLE_JDBC_DRIVER");
    String ORACLE_DB_CONNECTION = configurator.getProperty("ORACLE_DB_CONNECTION");
    String ORACLE_DB_USER = configurator.getProperty("ORACLE_DB_USER");
    String ORACLE_DB_PASSWORD = configurator.getProperty("ORACLE_DB_PASSWORD");

    static Logger log = Logger.getLogger(OracleDB.class.getName());

    public Connection connect() {
        Connection dbConnection = null;
        try {
            Class.forName(ORACLE_JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
           log.error(e);
        }
        try {
            dbConnection = DriverManager.getConnection(ORACLE_DB_CONNECTION, ORACLE_DB_USER, ORACLE_DB_PASSWORD);
            return dbConnection;
        } catch (SQLException e) {
            log.error(e);
        }
        return dbConnection;
    }




}
