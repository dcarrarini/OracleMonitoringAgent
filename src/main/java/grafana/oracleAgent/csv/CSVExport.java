package grafana.oracleAgent.csv;

import grafana.oracleAgent.main.PropertiesReader;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import java.util.List;
import java.util.Properties;


public class CSVExport {
    static Properties configurator = PropertiesReader.getProperties();
    static Logger log = Logger.getLogger(TablespaceCSV.class.getName());
    List<String> ExportList;
    String CSVFile;

    public CSVExport(List<String> exportList, String CSVFile) {
        ExportList = exportList;
        this.CSVFile = CSVFile;
    }

    public void writeCSV() {
        try {
            File file = new File(CSVFile);
            int i = 0;
            int y = ExportList.size();
            for (String sRow : ExportList) {
                String row = sRow + "\n";
                if (i == 0) {
                    row = "\n" + sRow + "\n";
                } else if (i == ExportList.size() - 1) {
                    row = sRow;
                }
                FileUtils.writeStringToFile(file, row, true);
                i++;
            }
        } catch (IOException e) {
            log.error(e);
        }
    }
}
