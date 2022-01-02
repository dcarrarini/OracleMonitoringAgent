package grafana.oracleAgent.db;

import grafana.oracleAgent.main.PropertiesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.Properties;

public class OracleDB {
    static Properties configurator = PropertiesReader.getProperties();
    String ORACLE_JDBC_DRIVER = configurator.getProperty("ORACLE_JDBC_DRIVER");
    String ORACLE_DB_CONNECTION = configurator.getProperty("ORACLE_DB_CONNECTION");
    String ORACLE_DB_USER = configurator.getProperty("ORACLE_DB_USER");
    String ORACLE_DB_PASSWORD = configurator.getProperty("ORACLE_DB_PASSWORD");

    private static final Logger log
            = LoggerFactory.getLogger(OracleDB.class);

    public Connection connect() {
        Connection dbConnection = null;
        try {
            Class.forName(ORACLE_JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
           log.error("OracleDB", e);
        }
        try {
            dbConnection = DriverManager.getConnection(ORACLE_DB_CONNECTION, ORACLE_DB_USER, ORACLE_DB_PASSWORD);
            return dbConnection;
        } catch (SQLException e) {
            log.error("OracleDB", e);
        }
        return dbConnection;
    }




}
