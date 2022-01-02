package grafana.oracleAgent.csv;

import grafana.oracleAgent.main.PropertiesReader;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TablespaceCSV {

    static Properties configurator = PropertiesReader.getProperties();
    List<String> ExportList;
    String CSVFile;
    private static final Logger log
            = LoggerFactory.getLogger(TablespaceCSV.class);

    public TablespaceCSV(List<String> exportList, String tablespaceCSVFile) {
        ExportList = exportList;
        CSVFile = tablespaceCSVFile;
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
                }else if(i== ExportList.size()-1){
                    row = sRow;
                }
                FileUtils.writeStringToFile(file, row, true);
                i++;
            }
        } catch (IOException e) {
            log.error("TablespaceCSV", e);
        }
    }
}
