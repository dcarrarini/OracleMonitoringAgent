package grafana.oracleAgent.csv;

import grafana.oracleAgent.main.PropertiesReader;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Properties;


public class CSVExport {
    static Properties configurator = PropertiesReader.getProperties();
    private static final Logger log
            = LoggerFactory.getLogger(CSVExport.class);
    List<String> lsExportList;
    String sCSVFile;

    public CSVExport(List<String> exportList, String sCSVFile) {
        lsExportList = exportList;
        this.sCSVFile = sCSVFile;
    }

    public void writeCSV() {
        try {
            File file = new File(sCSVFile);
            int i = 0;
            //int y = ExportList.size();
            for (String sRow : lsExportList) {
                String row = sRow + "\n";
                if (i == 0) {
                    row = "\n" + sRow + "\n";
                } else if (i == lsExportList.size() - 1) {
                    row = sRow;
                }
                FileUtils.writeStringToFile(file, row, true);
                i++;
            }
        } catch (IOException e) {
            log.error("WriteCSV", e);
        }
    }
}
