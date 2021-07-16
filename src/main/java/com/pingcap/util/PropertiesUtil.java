package com.pingcap.util;

import com.pingcap.enums.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

public class PropertiesUtil {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    public static Properties getProperties(String filePath) {
        Properties properties = new Properties();
        try {
            InputStream inputStream = new BufferedInputStream(new FileInputStream(filePath));
            properties.load(inputStream);
            logger.info(String.valueOf(properties));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

}
