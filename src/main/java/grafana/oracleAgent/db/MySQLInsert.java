package grafana.oracleAgent.db;

import grafana.oracleAgent.main.PropertiesReader;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class MySQLInsert {
    static Properties configurator = PropertiesReader.getProperties();
    static Logger log = Logger.getLogger(MySQLInsert.class.getName());

    public void insertTablespace(Connection MysqlConnection, String sqlInsert, int mode, String table){
        if (mode==2){
            String trucate = "truncate table "+table;
            PreparedStatement preparedStmt = null;
            try {
                preparedStmt = MysqlConnection.prepareStatement(trucate);
                preparedStmt.execute();
            } catch (SQLException e) {
                log.error(e);
            }
        }
        try {
            PreparedStatement preparedStmt = MysqlConnection.prepareStatement(sqlInsert);
            preparedStmt.execute();
        } catch (SQLException e) {
            log.error(sqlInsert);
            log.error(e);
        }
    }
}
