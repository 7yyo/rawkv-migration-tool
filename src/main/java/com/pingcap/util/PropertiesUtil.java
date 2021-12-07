package com.pingcap.util;

import com.pingcap.enums.Model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesUtil {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    public static Map<String, String> getProperties(String filePath) {
        Properties properties = new Properties();
        try {
            InputStream inputStream = new BufferedInputStream(new FileInputStream(filePath));
            properties.load(inputStream);
            logger.info(String.valueOf(properties));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Map<String, String> propertiesMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String k = entry.getKey().toString();
            String v = entry.getValue().toString();
            propertiesMap.put(k, v);
        }
        return propertiesMap;
    }

    public static void checkConfig(Map<String, String> properties, String configName) {
        if (properties.get(configName) == null) {
            logger.error("Configuration item must be added={}", configName);
            System.exit(0);
        }
    }
    
    public static void checkConfigRollback(Map<String, String> properties) {
    	String rollbackMode = properties.get(Model.ROLLBACK);
    	if(null == rollbackMode)
    		return;
    	rollbackMode = rollbackMode.trim();
        if("".equals(rollbackMode)) {
            logger.error("Configuration {} of item don't is blank character", Model.ROLLBACK);
            System.exit(0);
        }
        if(0 >= Integer.parseInt(rollbackMode)) {
            logger.error("Configuration {} of item must be greater than 0", Model.ROLLBACK);
            System.exit(0);        	
        }
        return;
    }
}
