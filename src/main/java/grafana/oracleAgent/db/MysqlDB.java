package grafana.oracleAgent.db;

import grafana.oracleAgent.main.PropertiesReader;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class MysqlDB {
    static Properties configurator = PropertiesReader.getProperties();
    static Logger log = Logger.getLogger(MysqlDB.class.getName());
    static final String MYSQL_JDBC_DRIVER   = configurator.getProperty("MYSQL_JDBC_DRIVER");
    static final String MYSQL_DB_CONNECTION = configurator.getProperty("MYSQL_DB_CONNECTION");
    static final String MYSQL_DB_USER       = configurator.getProperty("MYSQL_DB_USER");
    static final String MYSQL_DB_PASSWORD   = configurator.getProperty("MYSQL_DB_PASSWORD");


    public static Connection getMYSQLDBConnection() {
        Connection dbConnection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.error(e);
        }
        try {
            dbConnection = DriverManager.getConnection(MYSQL_DB_CONNECTION, MYSQL_DB_USER, MYSQL_DB_PASSWORD);
            return dbConnection;
        } catch (SQLException e) {
            log.error(e);
        }
        return dbConnection;
    }


}
