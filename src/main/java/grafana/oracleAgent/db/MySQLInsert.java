package grafana.oracleAgent.db;

import grafana.oracleAgent.csv.CSVExport;
import grafana.oracleAgent.main.PropertiesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class MySQLInsert {
    static Properties configurator = PropertiesReader.getProperties();
    private static final Logger log
            = LoggerFactory.getLogger(MySQLInsert.class);

    public void insertTablespace(Connection MysqlConnection, String sqlInsert, int mode, String table){
        if (mode==2){
            String trucate = "truncate table "+table;
            PreparedStatement preparedStmt = null;
            try {
                preparedStmt = MysqlConnection.prepareStatement(trucate);
                preparedStmt.execute();
            } catch (SQLException e) {
                log.error("MySQLInsert", e);
            }
        }
        try {
            PreparedStatement preparedStmt = MysqlConnection.prepareStatement(sqlInsert);
            preparedStmt.execute();
        } catch (SQLException e) {
            log.error(sqlInsert);
            log.error("MySQLInsert", e);
        }
    }
}
