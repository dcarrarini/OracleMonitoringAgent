package grafana.oracleAgent.main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Enumeration;
import java.util.Properties;

public class PropertiesReader {
	private static final Logger log
			= LoggerFactory.getLogger(PropertiesReader.class);
	static public String LoadProp(String sKey) {
		String sValue = null;
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream("./cfg/config.properties");
			prop.load(input);
			sValue = (prop.getProperty(sKey));
		} catch (IOException ex) {
			log.error("PropertiesReader", ex);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					log.error("PropertiesReader", e);
				}
			}
		}
		return sValue;
	}

	static public Properties getProperties() {
		InputStream input = null;
		try {
			String sFileInput = "./cfg/config.properties";
			input = new FileInputStream(sFileInput);
			Properties prop = new Properties();
			prop.load(input);
			return prop;
		} catch (IOException ex) {
			log.error("PropertiesReader", ex);
			return null;
		}
	}

	static public String retriveValue(String sKey) {
		String sValue = null;
		Properties prop = new Properties();
		InputStream input = null;
		try {
			String sFileInput = "./cfg/config.properties";
			input = new FileInputStream(sFileInput);
			prop.load(input);
			sValue = (prop.getProperty(sKey));
			if (sValue == null) {
				sValue = PropertiesReader.retriveMultiValues(sKey);
			}
		} catch (IOException ex) {
			log.error("PropertiesReader", ex);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					log.error("PropertiesReader", e);
				}
			}
		}
		return sValue;
	}

	static public String retriveValue(String sKey, String sFileInput) {
		String sValue = null;
		Properties prop = new Properties();
		InputStream input = null;
		try {
			sFileInput = "./cfg/" + sFileInput;
			input = new FileInputStream(sFileInput);
			prop.load(input);
			sValue = (prop.getProperty(sKey));
		} catch (IOException ex) {
			log.error("PropertiesReader", ex);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					log.error("PropertiesReader", e);
				}
			}
		}
		return sValue;
	}

	static String retriveMultiValues(String sKey) {
		String sValue = null;
		Properties prop = new Properties();
		InputStream input = null;
		try {

			String sFileInput = "./cfg/config.properties";
			input = new FileInputStream(sFileInput);
			prop.load(input);
			for (Enumeration<?> e = prop.propertyNames(); e.hasMoreElements();) {
				String name = (String) e.nextElement();
				String value = prop.getProperty(name);
				if (name.startsWith("PropertiesFile")) {
					sValue = PropertiesReader.retriveValue(sKey, value);
					if (sValue != null && !sValue.isEmpty()) {
						break;
					}
				}
			}
		} catch (IOException ex) {
			log.error("PropertiesReader", ex);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					log.error("PropertiesReader", e);
				}
			}
		}
		return sValue;
	}

	static public void writeValue(String sKey, String sValue) {
		Properties prop = new Properties();
		OutputStream output = null;

		try {

			String sFileInput = LoadProp("propFile");
			output = new FileOutputStream(sFileInput, true);
			// set the properties value
			prop.setProperty(sKey, sValue);

			// save properties to project root folder
			prop.store(output, null);
			prop.containsKey(sKey);

		} catch (IOException io) {
			log.error("PropertiesReader", io);
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					log.error("PropertiesReader", e);
				}
			}
		}
	}
}
