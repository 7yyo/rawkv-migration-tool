package com.pingcap.util;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;

public class PropertiesUtil {

    public static Properties getProperties() {
        InputStream inputStream;
        Properties properties = new Properties();
        try {
            inputStream = new BufferedInputStream(new FileInputStream("/home/tidb/yuyang/importer.properties"));
//            inputStream = new BufferedInputStream(new FileInputStream("src/main/resources/importer.properties"));
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

}
