--Create database
CREATE DATABASE `grafana_oracleagent` /*!40100 DEFAULT CHARACTER SET latin1 */;

--Create tables
CREATE TABLE `current_active_sessions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `extraction_date` datetime NOT NULL,
  `active_sessions` int(11) DEFAULT NULL,
  `max_sessions` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`,`extraction_date`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `dblogmode` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `extraction_date` datetime DEFAULT NULL,
  `log_mode` varchar(45) DEFAULT NULL,
  `log_enabled` int(11) DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

SELECT * FROM grafana_oracleagent.dblogmode;CREATE TABLE `export_datafile_io` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `extraction_date` datetime NOT NULL,
  `tablespace` varchar(250) DEFAULT NULL,
  `physical_reads` int(11) DEFAULT NULL,
  `physical_writes` int(11) DEFAULT NULL,
  `physical_block_reads` int(11) DEFAULT NULL,
  `physical_block_writes` int(11) DEFAULT NULL,
  `single_block_reads` int(11) DEFAULT NULL,
  `read_time` int(11) DEFAULT NULL,
  `write_time` int(11) DEFAULT NULL,
  `single_block_read_time` int(11) DEFAULT NULL,
  `avg_io_time` int(11) DEFAULT NULL,
  `last_io_time` int(11) DEFAULT NULL,
  `min_io_time` int(11) DEFAULT NULL,
  `max_io_read_time` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`,`extraction_date`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `exportsqltopconsumingmorecpu` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `extraction_date` datetime NOT NULL,
  `ospid` varchar(45) DEFAULT NULL,
  `sid` varchar(45) DEFAULT NULL,
  `serial` varchar(45) DEFAULT NULL,
  `sql_id` varchar(45) DEFAULT NULL,
  `sql_text` varchar(32000) DEFAULT NULL,
  `username` varchar(45) DEFAULT NULL,
  `program` varchar(45) DEFAULT NULL,
  `module` varchar(45) DEFAULT NULL,
  `osuser` varchar(45) DEFAULT NULL,
  `machine` varchar(45) DEFAULT NULL,
  `status` varchar(45) DEFAULT NULL,
  `cpu_usage_sec` float DEFAULT NULL,
  PRIMARY KEY (`id`,`extraction_date`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `exporttempbysession` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `extraction_date` datetime NOT NULL,
  `tablespace` varchar(45) DEFAULT NULL,
  `temp_size` float DEFAULT NULL,
  `instance` varchar(45) DEFAULT NULL,
  `sid_serial` varchar(45) DEFAULT NULL,
  `username` varchar(45) DEFAULT NULL,
  `program` varchar(45) DEFAULT NULL,
  `status` varchar(45) DEFAULT NULL,
  `sql_id` varchar(45) DEFAULT NULL,
  `fulltext` varchar(32000) DEFAULT NULL,
  PRIMARY KEY (`id`,`extraction_date`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `running_sql` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `extraction_date` datetime DEFAULT NULL,
  `sid` int(11) DEFAULT NULL,
  `serial` int(11) DEFAULT NULL,
  `username` varchar(45) DEFAULT NULL,
  `sql_id` varchar(45) DEFAULT NULL,
  `optimizer_mode` varchar(45) DEFAULT NULL,
  `sql_text` longtext,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `server_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `extraction_date` datetime NOT NULL,
  `component` varchar(100) DEFAULT NULL,
  `value` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`,`extraction_date`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `sessions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `extraction_date` datetime DEFAULT NULL,
  `username` varchar(45) DEFAULT NULL,
  `sid` int(11) DEFAULT NULL,
  `serial` int(11) DEFAULT NULL,
  `logon_time` datetime DEFAULT NULL,
  `status` varchar(45) DEFAULT NULL,
  `machine` varchar(45) DEFAULT NULL,
  `program` varchar(45) DEFAULT NULL,
  `sql_id` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `sga_usage` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `extraction_date` datetime NOT NULL,
  `free_mb` int(11) DEFAULT NULL,
  `used_mb` int(11) DEFAULT NULL,
  `total_mb` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`,`extraction_date`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `tablespace` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `extraction_date` datetime NOT NULL,
  `Tablespace` varchar(45) DEFAULT NULL,
  `UsedMb` float DEFAULT NULL,
  `AllocatedMb` float DEFAULT NULL,
  `TotalMb` float DEFAULT NULL,
  `FreePercentage` float DEFAULT NULL,
  PRIMARY KEY (`id`,`extraction_date`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `top10sessioncpu` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `extraction_date` datetime NOT NULL,
  `rank` varchar(45) DEFAULT NULL,
  `sid` float DEFAULT NULL,
  `sess_serial` varchar(45) DEFAULT NULL,
  `program` varchar(45) DEFAULT NULL,
  `cpu_mins` float DEFAULT NULL,
  PRIMARY KEY (`id`,`extraction_date`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

--Create Views
CREATE ALGORITHM=UNDEFINED DEFINER=`graf`@`%` SQL SECURITY DEFINER VIEW `v_actual_current_active_sessions` AS select `current_active_sessions`.`extraction_date` AS `extraction_date`,`current_active_sessions`.`active_sessions` AS `active_sessions`,`current_active_sessions`.`max_sessions` AS `max_sessions` from `current_active_sessions` where (`current_active_sessions`.`extraction_date` = (select max(`current_active_sessions`.`extraction_date`) from `current_active_sessions`));

CREATE ALGORITHM=UNDEFINED DEFINER=`graf`@`%` SQL SECURITY DEFINER VIEW `v_actual_sga_usage` AS select `sga_usage`.`extraction_date` AS `extraction_date`,'SGA' AS `SGA`,`sga_usage`.`free_mb` AS `free_mb`,`sga_usage`.`used_mb` AS `used_mb`,`sga_usage`.`total_mb` AS `total_mb`,round(((`sga_usage`.`free_mb` * 100) / `sga_usage`.`total_mb`),0) AS `free_perc` from `sga_usage` where (`sga_usage`.`extraction_date` = (select max(`sga_usage`.`extraction_date`) from `sga_usage`));

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v_current_tablespace` AS select `tablespace`.`id` AS `id`,`tablespace`.`extraction_date` AS `extraction_date`,`tablespace`.`Tablespace` AS `Tablespace`,`tablespace`.`UsedMb` AS `UsedMb`,`tablespace`.`AllocatedMb` AS `AllocatedMb`,`tablespace`.`TotalMb` AS `TotalMb`,`tablespace`.`FreePercentage` AS `FreePercentage` from `tablespace` where (`tablespace`.`extraction_date` = (select max(`tablespace`.`extraction_date`) from `tablespace`));

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v_physical_reads` AS select `export_datafile_io`.`extraction_date` AS `time`,`export_datafile_io`.`tablespace` AS `metric`,`export_datafile_io`.`physical_reads` AS `physical_reads` from `export_datafile_io` order by `export_datafile_io`.`extraction_date`;

CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v_top10sessioncpu` AS select `top10sessioncpu`.`extraction_date` AS `extraction_date`,concat('SID:',`top10sessioncpu`.`sid`,'\n								SERIAL:',`top10sessioncpu`.`sess_serial`) AS `info`,`top10sessioncpu`.`cpu_mins` AS `cpu_mins` from `top10sessioncpu` order by `top10sessioncpu`.`cpu_mins` desc;
