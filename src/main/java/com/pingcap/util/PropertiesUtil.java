package com.pingcap.util;

import java.io.*;
import java.util.Properties;

/**
 * @author yuyang
 */
public class PropertiesUtil {

    public static Properties getProperties(String filePath) {
        Properties properties = new Properties();
        try {
            InputStream inputStream = new BufferedInputStream(new FileInputStream(filePath));
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

}
