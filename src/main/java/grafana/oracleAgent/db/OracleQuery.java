package grafana.oracleAgent.db;

import grafana.oracleAgent.csv.CSVExport;
import grafana.oracleAgent.csv.TablespaceCSV;
import grafana.oracleAgent.main.PropertiesReader;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class OracleQuery {
    static Properties configurator = PropertiesReader.getProperties();
    static Logger log = Logger.getLogger(OracleDB.class.getName());


    public void exportTablespace(Connection OracleConnection, Connection MysqlConnection) {
        //Connection OracleConnection = connect();
        String sqlQuery = "SELECT  to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') AS datetime,tablespace_name,megs_used \"UsedMb\",megs_alloc \"AllocatedMb\",MAX \"TotalMb\",100-round(megs_used*100/MAX) \"FreePercentage\" FROM (SELECT a.tablespace_name, ROUND (a.bytes_alloc / 1024 / 1024) megs_alloc, ROUND (NVL (b.bytes_free, 0) / 1024 / 1024) megs_free, ROUND ((a.bytes_alloc - NVL (b.bytes_free, 0)) / 1024 / 1024) megs_used, ROUND ((NVL (b.bytes_free, 0) / a.bytes_alloc) * 100) pct_free, 100 - ROUND ((NVL (b.bytes_free, 0) / a.bytes_alloc) * 100) pct_used, ROUND (maxbytes / 1048576) MAX FROM ( SELECT f.tablespace_name, SUM (f.bytes) bytes_alloc, SUM ( DECODE (f.autoextensible, 'YES', f.maxbytes, 'NO', f.bytes)) maxbytes FROM dba_data_files f GROUP BY tablespace_name) a, ( SELECT f.tablespace_name, SUM (f.bytes) bytes_free FROM dba_free_space f GROUP BY tablespace_name) b WHERE a.tablespace_name = b.tablespace_name(+) UNION ALL SELECT h.tablespace_name, ROUND (SUM (h.bytes_free + h.bytes_used) / 1048576) megs_alloc, ROUND ( SUM ( (h.bytes_free + h.bytes_used) - NVL (p.bytes_used, 0)) / 1048576) megs_free, ROUND (SUM (NVL (p.bytes_used, 0)) / 1048576) megs_used, ROUND ( ( SUM ( (h.bytes_free + h.bytes_used) - NVL (p.bytes_used, 0)) / SUM (h.bytes_used + h.bytes_free)) * 100) pct_free, 100 - ROUND ( ( SUM ( (h.bytes_free + h.bytes_used) - NVL (p.bytes_used, 0)) / SUM (h.bytes_used + h.bytes_free)) * 100) pct_used, ROUND ( SUM ( DECODE (f.autoextensible, 'YES', f.maxbytes, 'NO', f.bytes) / 1048576)) MAX FROM sys.v_$temp_space_header h, sys.v_$temp_extent_pool p, dba_temp_files f WHERE p.file_id(+) = h.file_id AND p.tablespace_name(+) = h.tablespace_name AND f.file_id = h.file_id AND f.tablespace_name = h.tablespace_name GROUP BY h.tablespace_name ORDER BY 1)";
        String sqlInsert = "INSERT INTO grafana_oracleagent.tablespace (extraction_date,Tablespace,UsedMb,AllocatedMb,TotalMb,FreePercentage)VALUES";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
            List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString("datetime") + "','" + rs.getString("tablespace_name") +
                            "','" + rs.getString("UsedMb") + "','" + rs.getString("AllocatedMb") + "','" +
                            rs.getString("TotalMb") + "','" + rs.getString("FreePercentage") + "')";
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
            log.error(e);
        }
    }

    public void exportSQLTOPConsumingMoreCPU(Connection OracleConnection, Connection MysqlConnection) {
        String sqlQuery = "SELECT  to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime,b.tablespace,ROUND(((b.blocks*p.value)/1024/1024),2)||'M' AS temp_size, a.inst_id as Instance, a.sid||','||a.serial# AS sid_serial, NVL(a.username, '(oracle)') AS username, a.program, a.status, a.sql_id, t.SQL_TEXT fulltext FROM gv$session a, gv$sort_usage b, gv$parameter p, v$sqltext t WHERE p.name = 'db_block_size' AND a.saddr = b.session_addr AND a.inst_id=b.inst_id AND a.inst_id=p.inst_id and t.sql_id=a.sql_id ORDER BY temp_size desc";
        String sqlInsert = "INSERT INTO grafana_oracleagent.exportSQLTOPConsumingMoreCPU (extraction_date,ospid,sid,serial,sql_id,sql_text,username,program,module,osuser,machine,status,cpu_usage_sec) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
            List<String> ExportList = new ArrayList<String>();
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
                    sqlInsert = sqlInsert + "('" + rs.getString("datetime") + "','" + rs.getString("tablespace")
                            + "','" + rs.getString("temp_size") + "','" + rs.getString("Instance") + "','" +
                            rs.getString("sid_serial") + "','" + rs.getString("username") + "','" + rs.getString("program")
                            + "','" + rs.getString("status") + "','" + rs.getString("sql_id") + "','" + rs.getString("fulltext") + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.exportSQLTOPConsumingMoreCPU");
            }
        } catch (SQLException e) {
            log.error(e);
        }
    }


    public void exportTop10CPUconsumingSession(Connection OracleConnection, Connection MysqlConnection) {
        String sqlQuery = "select rownum as rank, a.* from (SELECT to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime, v.sid,sess.Serial# sess_serial ,program, round(v.value / (100 * 60),3) CPU_Mins FROM v$statname s , v$sesstat v, v$session sess WHERE s.name = 'CPU used by this session' and sess.sid = v.sid and v.statistic#=s.statistic# and v.value>0 ORDER BY v.value DESC) a where rownum < 11";
        String sqlInsert = "INSERT INTO grafana_oracleagent.top10sessioncpu (extraction_date,rank,sid,sess_serial,program,CPU_Mins) VALUES ";

        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
            List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString("datetime") + "','" + rs.getString("rank") + "','" + rs.getString("sid") + "','" +
                            rs.getString("sess_serial") + "','" + rs.getString("program") + "','" + rs.getString("CPU_Mins") + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 2, "grafana_oracleagent.top10sessioncpu");
            }
        } catch (SQLException e) {
            log.error(e);
        }
    }
/*
    public void exportTopCPUConsumingSessionInLast10Min(Connection OracleConnection) {
        String sqlQuery = "select * from (select to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime, session_id, session_serial# sess_serial, count(*) count from v$active_session_history where session_state= 'ON CPU' and sample_time >= sysdate - interval '10' minute group by session_id, session_serial# order by count(*) desc)";
        try (Statement statement = OracleConnection.createStatement()) {
            ResultSet rs = statement.executeQuery(sqlQuery);
            List<String> ExportList = new ArrayList<String>();
            String CSVFile = configurator.getProperty("TopCPUConsumingSessionInLast10MinCSV");
            while (rs.next()) {
                ExportList.add(rs.getString("datetime") + "," + rs.getString("session_id") + "," +
                        rs.getString("sess_serial") + "," + rs.getString("count"));
            }
            CSVExport tCSV = new CSVExport(ExportList, CSVFile);
            tCSV.writeCSV();
        } catch (SQLException e) {
            log.error(e);
        }
    }
*/

    public void exporttempBySession(Connection OracleConnection, Connection MysqlConnection) {
        String sqlQuery = "SELECT  to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime,b.tablespace, ROUND(((b.blocks*p.value)/1024/1024),2)||'M' AS temp_size, a.inst_id as Instance, a.sid||','||a.serial# AS sid_serial, NVL(a.username, '(oracle)') AS username, a.program, a.status, a.sql_id, t.SQL_TEXT fulltext FROM gv$session a, gv$sort_usage b, gv$parameter p, v$sqltext t WHERE p.name = 'db_block_size' AND a.saddr = b.session_addr AND a.inst_id=b.inst_id AND a.inst_id=p.inst_id and t.sql_id=a.sql_id ORDER BY temp_size desc";
        String sqlInsert = "INSERT INTO grafana_oracleagent.exportSQLTOPConsumingMoreCPU (extraction_date,tablespace,temp_size,Instance,sid_serial,username,program,status,sql_id,fulltext) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
            List<String> ExportList = new ArrayList<String>();
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
                    sqlInsert = sqlInsert + "('" + rs.getString("datetime") + "','" + rs.getString("tablespace") + "," +
                            rs.getString("temp_size") + "," + rs.getString("Instance") + "," + rs.getString("sid_serial") + ","
                            + rs.getString("username") + "," + rs.getString("program") + "," + rs.getString("status") + "," + rs.getString("sql_id")
                            + "," + rs.getString("fulltext") + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.exportSQLTOPConsumingMoreCPU");
            }
        } catch (SQLException e) {
            log.error(e);
        }
    }

    public void exportCurrentActiveSession(Connection OracleConnection, Connection MysqlConnection) {
        String sqlQuery = "SELECT to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime,(SELECT COUNT (*) FROM V$SESSION) active, VP.VALUE AS MAX_SESSION  FROM V$PARAMETER VP WHERE VP.NAME in ('sessions')";
        String sqlInsert = "INSERT INTO grafana_oracleagent.current_active_sessions (extraction_date,active_sessions, max_sessions) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
            List<String> ExportList = new ArrayList<String>();
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
                    sqlInsert = sqlInsert + "('" + rs.getString("datetime") + "','" + rs.getString("active") + "','" +
                            rs.getString("max_session") + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.current_active_sessions");
            }

        } catch (SQLException e) {
            log.error(e);
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
        String sqlInsert = "INSERT INTO grafana_oracleagent.server_info (extraction_date,component, value) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
            List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString("datetime") + "','" + rs.getString("component") + "','" +
                            rs.getString("value") + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 2, "grafana_oracleagent.server_info");
            }
        } catch (SQLException e) {
            log.error(e);
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
        String sqlInsert = "INSERT INTO grafana_oracleagent.sga_usage (extraction_date,free_mb, used_mb,total_mb) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
            List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString("datetime") + "','" + rs.getString("free_mb") + "','" +
                            rs.getString("used_mb") + "','" + rs.getString("total_mb") + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.sga_usage");
            }
        } catch (SQLException e) {
            log.error(e);
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
                "last_io_time,min_io_time,max_io_read_time) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
            List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString("datetime") + "','" + rs.getString("tablespace").replaceAll("\\\\","/") + "','" +
                            rs.getString("physical_reads") + "','" + rs.getString("physical_writes") + "','" +
                            rs.getString("physical_block_reads") + "','" + rs.getString("physical_block_writes") + "','" +
                            rs.getString("single_block_reads") + "','" + rs.getString("read_time") + "','" +
                            rs.getString("write_time") + "','" + rs.getString("single_block_read_time") + "','" +
                            rs.getString("avg_io_time") + "','" + rs.getString("last_io_time") + "','" +
                            rs.getString("min_io_time") + "','" + rs.getString("max_io_read_time") + "')";
                    if (last < size) {
                        sqlInsert = sqlInsert + ",";
                    }
                    last++;
                }
                MySQLInsert mySQLInsert = new MySQLInsert();
                mySQLInsert.insertTablespace(MysqlConnection, sqlInsert, 1, "grafana_oracleagent.export_datafile_io");
            }
        } catch (SQLException e) {
            log.error(e);
        }
    }


    public void archiveLogMode(Connection OracleConnection, Connection MysqlConnection) {
        String sqlQuery = "select to_char(SYSDATE, 'yyyy-mm-dd hh24:mi:ss') as datetime,log_mode, decode(log_mode,'NOARCHIVELOG',0,1) log_enabled from v$database";
        String sqlInsert = "INSERT INTO grafana_oracleagent.dblogmode (extraction_date, log_mode,log_enabled) VALUES ";
        try (Statement statement = OracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = statement.executeQuery(sqlQuery);
            List<String> ExportList = new ArrayList<String>();
            int last = 1;
            int size = 0;
            if (rs != null) {
                rs.last();    // moves cursor to the last row
                size = rs.getRow(); // get row id
            }
            rs.beforeFirst();
            if (size > 0) {
                while (rs.next()) {
                    sqlInsert = sqlInsert + "('" + rs.getString("datetime") + "','" + rs.getString("log_mode") + "','" +
                            rs.getString("log_enabled") + "')";
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
            log.error(e);
        }
    }

}
