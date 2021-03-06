package grafana.oracleAgent.db;

import grafana.oracleAgent.main.Main;
import grafana.oracleAgent.main.PropertiesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class OracleQuery {
    static Properties configurator = PropertiesReader.getProperties();
    private static final Logger log
            = LoggerFactory.getLogger(OracleQuery.class);

    //AVOID LITERAL - SONARLINT
    String datetime = "datetime";
    String tablespace = "tablespace";
    String username = "username";
    String program = "program";
    String status = "status";
    String sql_id = "sql_id";

    public void exportTablespace(Connection OracleConnection, Connection MysqlConnection) {
        //Connection OracleConnection = connect();
        String sqlQuery = "SELECT  to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') AS datetime,tablespace_name,megs_used \"UsedMb\",megs_alloc \"AllocatedMb\",MAX \"TotalMb\",100-round(megs_used*100/MAX) \"FreePercentage\" FROM (SELECT a.tablespace_name, ROUND (a.bytes_alloc / 1024 / 1024) megs_alloc, ROUND (NVL (b.bytes_free, 0) / 1024 / 1024) megs_free, ROUND ((a.bytes_alloc - NVL (b.bytes_free, 0)) / 1024 / 1024) megs_used, ROUND ((NVL (b.bytes_free, 0) / a.bytes_alloc) * 100) pct_free, 100 - ROUND ((NVL (b.bytes_free, 0) / a.bytes_alloc) * 100) pct_used, ROUND (maxbytes / 1048576) MAX FROM ( SELECT f.tablespace_name, SUM (f.bytes) bytes_alloc, SUM ( DECODE (f.autoextensible, 'YES', f.maxbytes, 'NO', f.bytes)) maxbytes FROM dba_data_files f GROUP BY tablespace_name) a, ( SELECT f.tablespace_name, SUM (f.bytes) bytes_free FROM dba_free_space f GROUP BY tablespace_name) b WHERE a.tablespace_name = b.tablespace_name(+) UNION ALL SELECT h.tablespace_name, ROUND (SUM (h.bytes_free + h.bytes_used) / 1048576) megs_alloc, ROUND ( SUM ( (h.bytes_free + h.bytes_used) - NVL (p.bytes_used, 0)) / 1048576) megs_free, ROUND (SUM (NVL (p.bytes_used, 0)) / 1048576) megs_used, ROUND ( ( SUM ( (h.bytes_free + h.bytes_used) - NVL (p.bytes_used, 0)) / SUM (h.bytes_used + h.bytes_free)) * 100) pct_free, 100 - ROUND ( ( SUM ( (h.bytes_free + h.bytes_used) - NVL (p.bytes_used, 0)) / SUM (h.bytes_used + h.bytes_free)) * 100) pct_used, ROUND ( SUM ( DECODE (f.autoextensible, 'YES', f.maxbytes, 'NO', f.bytes) / 1048576)) MAX FROM sys.v_$temp_space_header h, sys.v_$temp_extent_pool p, dba_temp_files f WHERE p.file_id(+) = h.file_id AND p.tablespace_name(+) = h.tablespace_name AND f.file_id = h.file_id AND f.tablespace_name = h.tablespace_name GROUP BY h.tablespace_name ORDER BY 1)";
        String sqlInsert = "INSERT INTO grafana_oracleagent.tablespace (extraction_date,Tablespace,UsedMb,AllocatedMb,TotalMb,FreePercentage, dbid)VALUES";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
          //SONAR
            //List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" + rs.getString("tablespace_name") +
                            "','" + rs.getString("UsedMb") + "','" + rs.getString("AllocatedMb") + "','" +
                            rs.getString("TotalMb") + "','" + rs.getString("FreePercentage") + "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                //call insert
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.tablespace");
            }
        } catch (SQLException e) {
            log.error("OracleQuery", e);
        }
    }

    public void exportSQLTOPConsumingMoreCPU(Connection OracleConnection, Connection MysqlConnection) {
        String sqlQuery = "SELECT  to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime,b.tablespace,ROUND(((b.blocks*p.value)/1024/1024),2)||'M' AS temp_size, a.inst_id as Instance, a.sid||','||a.serial# AS sid_serial, NVL(a.username, '(oracle)') AS username, a.program, a.status, a.sql_id, t.SQL_TEXT fulltext FROM gv$session a, gv$sort_usage b, gv$parameter p, v$sqltext t WHERE p.name = 'db_block_size' AND a.saddr = b.session_addr AND a.inst_id=b.inst_id AND a.inst_id=p.inst_id and t.sql_id=a.sql_id ORDER BY temp_size desc";
        String sqlInsert = "INSERT INTO grafana_oracleagent.exportSQLTOPConsumingMoreCPU (extraction_date,ospid,sid,serial,sql_id,sql_text,username,program,module,osuser,machine,status,cpu_usage_sec, dbid) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
          //SONAR
            //List<String> ExportList = new ArrayList<String>();
            //String CSVFile = configurator.getProperty("SQLTOPConsumingMoreCPUCSV");
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" + rs.getString(tablespace)
                            + "','" + rs.getString("temp_size") + "','" + rs.getString("Instance") + "','" +
                            rs.getString("sid_serial") + "','" + rs.getString(username) + "','" + rs.getString(program)
                            + "','" + rs.getString(status) + "','" + rs.getString(sql_id) + "','" + rs.getString("fulltext")+ "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.exportSQLTOPConsumingMoreCPU");
            }
        } catch (SQLException e) {
            log.error("OracleQuery", e);
        }
    }


    public void exportTop10CPUconsumingSession(Connection OracleConnection, Connection MysqlConnection) {
        String sqlQuery = "select rownum as rank, a.* from (SELECT to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime, v.sid,sess.Serial# sess_serial ,program, round(v.value / (100 * 60),3) CPU_Mins FROM v$statname s , v$sesstat v, v$session sess WHERE s.name = 'CPU used by this session' and sess.sid = v.sid and v.statistic#=s.statistic# and v.value>0 ORDER BY v.value DESC) a where rownum < 11";
        String sqlInsert = "INSERT INTO grafana_oracleagent.top10sessioncpu (extraction_date,rank,sid,sess_serial,program,CPU_Mins,dbid) VALUES ";

        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
          //SONAR
            //List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" + rs.getString("rank") + "','" + rs.getString("sid") + "','" +
                            rs.getString("sess_serial") + "','" + rs.getString(program) + "','" + rs.getString("CPU_Mins")+ "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 2, "grafana_oracleagent.top10sessioncpu");
            }
        } catch (SQLException e) {
            log.error("OracleQuery", e);
        }
    }

    public void exporttempBySession(Connection OracleConnection, Connection MysqlConnection) {
        String sqlQuery = "SELECT  to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime,b.tablespace, ROUND(((b.blocks*p.value)/1024/1024),2)||'M' AS temp_size, a.inst_id as Instance, a.sid||','||a.serial# AS sid_serial, NVL(a.username, '(oracle)') AS username, a.program, a.status, a.sql_id, t.SQL_TEXT fulltext FROM gv$session a, gv$sort_usage b, gv$parameter p, v$sqltext t WHERE p.name = 'db_block_size' AND a.saddr = b.session_addr AND a.inst_id=b.inst_id AND a.inst_id=p.inst_id and t.sql_id=a.sql_id ORDER BY temp_size desc";
        String sqlInsert = "INSERT INTO grafana_oracleagent.exportSQLTOPConsumingMoreCPU (extraction_date,tablespace,temp_size,Instance,sid_serial,username,program,status,sql_id,fulltext,dbid) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
          //SONAR
            //List<String> ExportList = new ArrayList<String>();
            //String CSVFile = configurator.getProperty("tempBySessionCSV");
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" + rs.getString(tablespace) + "," +
                            rs.getString("temp_size") + "," + rs.getString("Instance") + "," + rs.getString("sid_serial") + ","
                            + rs.getString(username) + "," + rs.getString(program) + "," + rs.getString(status) + "," + rs.getString(sql_id)
                            + "," + rs.getString("fulltext")+ "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.exportSQLTOPConsumingMoreCPU");
            }
        } catch (SQLException e) {
            log.error("OracleQuery", e);
        }
    }

    public void exportCurrentActiveSession(Connection OracleConnection, Connection MysqlConnection) {
        String sqlQuery = "SELECT to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime,(SELECT COUNT (*) FROM V$SESSION) active, VP.VALUE AS MAX_SESSION  FROM V$PARAMETER VP WHERE VP.NAME in ('sessions')";
        String sqlInsert = "INSERT INTO grafana_oracleagent.current_active_sessions (extraction_date,active_sessions, max_sessions,dbid) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
            //SONAR
            //List<String> ExportList = new ArrayList<String>();
            //String CSVFile = configurator.getProperty("tempBySessionCSV");
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" + rs.getString("active") + "','" +
                            rs.getString("max_session")+ "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.current_active_sessions");
            }

        } catch (SQLException e) {
            log.error("OracleQuery", e);
        }
    }

    public void exportServerInfo(Connection OracleConnection, Connection MysqlConnection) {
        String sqlQuery = "SELECT DISTINCT to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime, " +
                "       stat_name    component," +
                "       CASE" +
                "           WHEN stat_name = 'PHYSICAL_MEMORY_BYTES'" +
                "           THEN" +
                "               ROUND (VALUE / 1024 / 1024 / 1024)" +
                "           ELSE" +
                "               VALUE" +
                "       END          VALUE" +
                "  FROM dba_hist_osstat" +
                " WHERE stat_name IN" +
                "           ('NUM_CPU_CORES', 'NUM_CPU_SOCKETS', 'PHYSICAL_MEMORY_BYTES')";
        String sqlInsert = "INSERT INTO grafana_oracleagent.server_info (extraction_date,component, value,dbid) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
            //SONAR
            //List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" + rs.getString("component") + "','" +
                            rs.getString("value")+ "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 2, "grafana_oracleagent.server_info");
            }
        } catch (SQLException e) {
            log.error("OracleQuery", e);
        }
    }

    public void exportSgaUsage(Connection OracleConnection, Connection MysqlConnection) {
        String sqlQuery = "SELECT to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime, " +
                "        ROUND (used.bytes / 1024 / 1024, 2)     used_mb," +
                "       ROUND (free.bytes / 1024 / 1024, 2)     free_mb," +
                "       ROUND (tot.bytes / 1024 / 1024, 2)      total_mb" +
                "  FROM (SELECT SUM (bytes)     bytes" +
                "          FROM v$sgastat" +
                "         WHERE name != 'free memory') used," +
                "       (SELECT SUM (bytes)     bytes" +
                "          FROM v$sgastat" +
                "         WHERE name = 'free memory') free," +
                "       (SELECT SUM (bytes) bytes FROM v$sgastat) tot";
        String sqlInsert = "INSERT INTO grafana_oracleagent.sga_usage (extraction_date,free_mb, used_mb,total_mb,dbid) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
            //SONAR
            //List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" + rs.getString("free_mb") + "','" +
                            rs.getString("used_mb") + "','" + rs.getString("total_mb")+ "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.sga_usage");
            }
        } catch (SQLException e) {
            log.error("OracleQuery", e);
        }
    }

    public void exportDatafileIO(Connection OracleConnection, Connection MysqlConnection) {
        String sqlQuery = "SELECT to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime, (SELECT tf.name " +
                "                          FROM v$tempfile tf " +
                "                         WHERE tf.file# = ts.file#)    tablespace, " +
                "                       ts.phyrds                       physical_reads, " +
                "                       ts.phywrts                      physical_writes, " +
                "                       ts.phyblkrd                     physical_block_reads, " +
                "                       ts.phyblkwrt                    physical_block_writes, " +
                "                       ts.singleblkrds                 single_block_reads, " +
                "                       ts.readtim                      read_time, " +
                "                       ts.writetim                     write_time, " +
                "                       ts.singleblkrdtim               single_block_read_time, " +
                "                       ts.avgiotim                     avg_io_time, " +
                "                       ts.lstiotim                     last_io_time, " +
                "                       ts.miniotim                     min_io_time, " +
                "                       ts.maxiortm                     max_io_read_time, " +
                "                       ts.maxiowtm                     max_io_write_time " +
                "                  FROM gv$tempstat ts " +
                "                 WHERE ts.inst_id = USERENV ('instance')  UNION ALL SELECT TO_CHAR (SYSDATE, 'yyyy-mm-dd hh24:mi:ss') AS datetime, (SELECT df.name " +
                "                          FROM v$datafile df " +
                "                         WHERE df.file# = fs.file#), " +
                "                       fs.phyrds, " +
                "                       fs.phywrts, " +
                "                       fs.phyblkrd, " +
                "                       fs.phyblkwrt, " +
                "                       fs.singleblkrds, " +
                "                       fs.readtim, " +
                "                       fs.writetim, " +
                "                       fs.singleblkrdtim, " +
                "                       fs.avgiotim, " +
                "                       fs.lstiotim, " +
                "                       fs.miniotim, " +
                "                       fs.maxiortm, " +
                "                       fs.maxiowtm " +
                "                  FROM gv$filestat fs " +
                "                 WHERE fs.inst_id = USERENV ('instance') " +
                "                ORDER BY 1";
        String sqlInsert = "INSERT INTO grafana_oracleagent.export_datafile_io " +
                "(extraction_date,tablespace,physical_reads,physical_writes," +
                "physical_block_reads,physical_block_writes,single_block_reads," +
                "read_time,write_time,single_block_read_time,avg_io_time," +
                "last_io_time,min_io_time,max_io_read_time, dbid) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
            //SONAR
            //List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" + rs.getString(tablespace).replaceAll("\\\\","/") + "','" +
                            rs.getString("physical_reads") + "','" + rs.getString("physical_writes") + "','" +
                            rs.getString("physical_block_reads") + "','" + rs.getString("physical_block_writes") + "','" +
                            rs.getString("single_block_reads") + "','" + rs.getString("read_time") + "','" +
                            rs.getString("write_time") + "','" + rs.getString("single_block_read_time") + "','" +
                            rs.getString("avg_io_time") + "','" + rs.getString("last_io_time") + "','" +
                            rs.getString("min_io_time") + "','" + rs.getString("max_io_read_time")+ "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.export_datafile_io");
            }
        } catch (SQLException e) {
            log.error("OracleQuery", e);
        }
    }


    public void archiveLogMode(Connection OracleConnection, Connection MysqlConnection) {
        String sqlQuery = "select to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime,log_mode, decode(log_mode,'NOARCHIVELOG',0,1) log_enabled from v$database";
        String sqlInsert = "INSERT INTO grafana_oracleagent.dblogmode (extraction_date, log_mode,log_enabled,dbid) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
          //SONAR
            //List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" + rs.getString("log_mode") + "','" +
                            rs.getString("log_enabled")+ "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 2, "grafana_oracleagent.dblogmode");
            }
        } catch (SQLException e) {
            log.error(sqlInsert);
            log.error("OracleQuery", e);
        }
    }


    public void exporterSessions(Connection OracleConnection, Connection MysqlConnection) {
        String sqlQuery = "select to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime,username, sid, serial# serial,to_char(logon_time, 'yyyy-mm-dd hh24:mi:ss') logon_time, status,  machine, program, sql_id from v$session where username is not null";
        String sqlInsert = "INSERT INTO grafana_oracleagent.sessions (extraction_date, username,sid, serial,logon_time, status, machine, program,sql_id,dbid) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
          //SONAR
            //List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" + rs.getString(username) + "','" +
                            rs.getInt("sid") + "','" +
                            rs.getInt("serial") + "','" +
                            rs.getString("logon_time") + "','" +
                            rs.getString(status) + "','" +
                            rs.getString("machine") + "','" +
                            rs.getString(program) + "','" +
                            rs.getString(sql_id)+ "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.sessions");
            }
        } catch (SQLException e) {
            log.error(sqlInsert);
            log.error("OracleQuery", e);
        }
    }


    public void exporterRunningSQL(Connection OracleConnection, Connection MysqlConnection) {
        String sqlQuery = "select to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime,x.sid,x.serial# serial,x.username,x.sql_id,optimizer_mode,sql_text from v$sqlarea sqlarea,v$session x where  x.sql_hash_value = sqlarea.hash_value and x.sql_address = sqlarea.address and x.username is not null";
        String sqlInsert = "INSERT INTO grafana_oracleagent.running_sql (extraction_date,username,sid,serial,sql_id,optimizer_mode,sql_text,dbid) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
          //SONAR
            //List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" + rs.getString(username) + "','" +
                            rs.getInt("sid") + "','" +
                            rs.getInt("serial") + "','" +
                            rs.getString(sql_id) + "','" +
                            rs.getString("optimizer_mode") + "','" +
                            rs.getString("sql_text").replaceAll("'","''")+ "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.running_sql");
            }
        } catch (SQLException e) {
            log.error(sqlInsert);
            log.error("OracleQuery", e);
        }
    }

    public void exporterRedoLOG(Connection OracleConnection, Connection MysqlConnection) {
        log.info("exporterRedoLOG");
        String sqlQuery = "/* Formatted on 22/12/2021 07:59:20 (QP5 v5.371) */\n" +
                "SELECT TO_CHAR (SYSDATE, 'yyyy-mm-dd hh24:mi:ss')    AS datetime,\n" +
                "       l.group#                                      AS group_,\n" +
                "       l.thread#                                     thread,\n" +
                "       l.sequence#                                   sequence,\n" +
                "       l.bytes / 1024 / 1024                         AS mb,\n" +
                "       l.members,\n" +
                "       l.archived,\n" +
                "       l.status,\n" +
                "       DECODE (l.status, 'INACTIVE', 0, 1)           AS log_switch,\n" +
                "       CASE\n" +
                "           WHEN l.first_time IS NULL THEN '2999-01-01 00:00:00'\n" +
                "           ELSE TO_CHAR (l.first_time, 'yyyy-mm-dd hh24:mi:ss')\n" +
                "       END                                           first_time,\n" +
                "       CASE\n" +
                "           WHEN l.next_time IS NULL THEN '2999-01-01 00:00:00'\n" +
                "           ELSE TO_CHAR (l.next_time, 'yyyy-mm-dd hh24:mi:ss')\n" +
                "       END                                           next_time,\n" +
                "       f.TYPE,\n" +
                "       replace(f.MEMBER,'\\','/') member\n" +
                "  FROM v$log l JOIN v$logfile f ON (f.group# = l.group#)";
        String sqlInsert = "INSERT INTO grafana_oracleagent.redo_log (extraction_date,log_group,thread,sequence,size_mb,members,archived,status,log_switch,first_time,next_time, type, logfile,dbid) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
          //SONAR
            //List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" + rs.getInt("group_") + "','" +
                            rs.getInt("thread") + "','" +
                            rs.getInt("sequence") + "','" +
                            rs.getInt("mb") + "','" +
                            rs.getInt("members") + "','" +
                            rs.getString("archived") + "','" +
                            rs.getString(status) + "','" +
                            rs.getInt("log_switch") + "','" +
                            rs.getString("first_time") + "','" +
                            rs.getString("next_time") + "','" +
                            rs.getString("type") + "','" +
                            rs.getString("member") + "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.redo_log");
            }
        } catch (SQLException e) {
            log.error(sqlInsert);
            log.error("OracleQuery", e);
        }
    }

    public void exporterDBInfo(Connection OracleConnection, Connection MysqlConnection) {
        log.info("exporterRedoLOG");
        String sqlQuery = "select to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime,name, platform_name from V$database";
        String sqlInsert = "INSERT INTO grafana_oracleagent.database_info (extraction_date,name,platform_name,dbid) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
          //SONAR
            //List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" + rs.getString("name") + "','" +
                            rs.getString("platform_name")+ "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 2, "grafana_oracleagent.database_info");
            }
        } catch (SQLException e) {
            log.error(sqlInsert);
            log.error("OracleQuery", e);
        }
    }

    public void exporterDBVersion(Connection OracleConnection, Connection MysqlConnection) {
        log.info("exporterRedoLOG");
        String sqlQuery = "SELECT to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime, banner FROM v$version WHERE banner LIKE 'Oracle%'";
        String sqlInsert = "INSERT INTO grafana_oracleagent.database_version (extraction_date,version,dbid) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
          //SONAR
            //List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" +
                            rs.getString("banner")+ "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 2, "grafana_oracleagent.database_version");
            }
        } catch (SQLException e) {
            log.error(sqlInsert);
            log.error("OracleQuery", e);
        }
    }

    public void exporterTablesInfo(Connection OracleConnection, Connection MysqlConnection) {
        log.info("exporterRedoLOG");
        String sqlQuery = "SELECT to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime, t.owner,\n" +
                "       t.table_name,\n" +
                "       t.tablespace_name,\n" +
                "       t.num_rows,\n" +
                "       to_char(t.last_analyzed, 'yyyy-mm-dd hh24:mi:ss') last_analyzed,\n" +
                "       ROUND (s.bytes / 1024 / 1024)     size_mb\n" +
                "  FROM dba_tables t, dba_segments s\n" +
                " WHERE     t.TABLE_NAME = s.SEGMENT_NAME\n" +
                "       AND (   t.tablespace_name NOT IN ('SYSTEM', 'SYSAUX', 'USERS')\n" +
                "            OR t.owner = 'EDM'" +
                ")";
        String sqlInsert = "INSERT INTO grafana_oracleagent.tables_info (extraction_date,owner,table_name, tablespace_name,num_rows,size_mb,last_analyzed,dbid) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
          //SONAR
            //List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" +
                            rs.getString("owner") + "','" +
                            rs.getString("table_name") + "','" +
                            rs.getString("tablespace_name") + "','" +
                            rs.getInt("num_rows") + "','" +
                            rs.getInt("size_mb") + "','" +
                            rs.getString("last_analyzed")+ "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.tables_info");
            }
        } catch (SQLException e) {
            log.error(sqlInsert);
            log.error("OracleQuery", e);
        }
    }
    public void exporterTop10Tables(Connection OracleConnection, Connection MysqlConnection) {
        log.info("exporterRedoLOG");
        String sqlQuery = "select\n" +
                "        *\n" +
                "       from (select to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime, \n" +
                "      owner,\n" +
                "      segment_name,\n" +
                "      round(bytes/1024/1024) size_mb\n" +
                "   from\n" +
                "      dba_segments\n" +
                "   where\n" +
                "      segment_type = 'TABLE'\n" +
                "       AND (   tablespace_name NOT IN ('SYSTEM', 'SYSAUX', 'USERS')\n" +
                "            OR owner = 'EDM')\n" +
                "   order by\n" +
                "      bytes/1024/1024 desc)\n" +
                "where\n" +
                "   rownum <= 10";
        //log.info("sqlQuery:"+sqlQuery);
        String sqlInsert = "INSERT INTO grafana_oracleagent.top10tables (extraction_date,owner,table_name,size_mb,dbid) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
          //SONAR
            //List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" +
                            rs.getString("owner") + "','" +
                            rs.getString("segment_name") + "','" +
                            rs.getInt("size_mb")+ "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                //log.info("SQL:"+sqlInsert);
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.top10tables");
            }
        } catch (SQLException e) {
            log.error(sqlInsert);
            log.error("OracleQuery", e);
        }
    }

    public void exporterStaleStats(Connection OracleConnection, Connection MysqlConnection) {
        log.info("exporterRedoLOG");
        String sqlQuery = "SELECT TO_CHAR (SYSDATE, 'yyyy-mm-dd hh24:mi:ss')          AS datetime,\n" +
                "       s.owner,\n" +
                "       s.table_name,\n" +
                "       s.stale_stats,\n" +
                "       CASE WHEN stale_stats = 'YES' THEN 1 ELSE 0 END     stale_stats_boo,\n" +
                "       TO_CHAR (s.last_analyzed, 'yyyy-mm-dd hh24:mi:ss') as last_analyzed\n" +
                "  FROM dba_tab_statistics s, dba_tables t\n" +
                " WHERE     s.table_name = t.table_name\n" +
                "       AND t.owner = s.owner\n" +
                "       AND (   t.tablespace_name NOT IN ('SYSTEM', 'SYSAUX', 'USERS')\n" +
                "            OR t.owner = 'EDM')";
        //log.info("sqlQuery:"+sqlQuery);
        String sqlInsert = "INSERT INTO grafana_oracleagent.stale_stats (extraction_date,owner,table_name,stale_stats,stale_stats_boo,last_analyzed,dbid) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
          //SONAR
            //List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString(datetime) + "','" +
                            rs.getString("owner") + "','" +
                            rs.getString("table_name") + "','" +
                            rs.getString("stale_stats") + "','" +
                            rs.getInt("stale_stats_boo") + "','" +
                            rs.getString("last_analyzed")+ "','" + Main.sDBID + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                //log.info("SQL:"+sqlInsert);
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.top10tables");
            }
        } catch (SQLException e) {
            log.error(sqlInsert);
            log.error("OracleQuery", e);
        }
    }
}
