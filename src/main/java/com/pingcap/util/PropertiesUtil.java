package com.pingcap.util;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;

public class PropertiesUtil {

    private static Logger logger = Logger.getLogger(PropertiesUtil.class);

    public static Properties getProperties() {
        InputStream inputStream;
        Properties properties = new Properties();
        try {
            inputStream = new BufferedInputStream(new FileInputStream("src/main/resources/importer.properties"));
            properties.load(inputStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info(String.format("Welcome to TiKV importer. Properties -> %s", properties.toString()));
        return properties;
    }

}
