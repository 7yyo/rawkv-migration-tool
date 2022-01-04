package com.pingcap.util;

import com.pingcap.enums.Model;
import com.pingcap.task.TaskInterface;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

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
    
    //Cannot be empty and greater than zero
    public static void checkNaturalNumber(Map<String, String> properties,String paramName,boolean canEmpty) {
    	String rollbackMode = properties.get(paramName);
    	if(null == rollbackMode) {
    		if(canEmpty) {
    			return;
    		}
    		else {
                logger.error("Configuration {} of item must exist", paramName);
                System.exit(0);    			
    		}
    	}
    	rollbackMode = rollbackMode.trim();
        if("".equals(rollbackMode)) {
            logger.error("Configuration {} of item don't is blank character", paramName);
            System.exit(0);
        }
        if(0 >= Integer.parseInt(rollbackMode)) {
            logger.error("Configuration {} of item must be greater than 0", paramName);
            System.exit(0);        	
        }
        return;
    }
    
    public static synchronized void reloadConfiguration(ThreadPoolExecutor threadPoolFileScanner,TaskInterface cmdInterFace){
    	Map<String, String> oldProperties = cmdInterFace.getProperties();
    	String configFilePath = oldProperties.get(Model.SYS_CFG_PATH);
		Map<String, String> newProperties = PropertiesUtil.getProperties(configFilePath);
		int ret = cmpConfigUpdate(oldProperties,newProperties,Model.CORE_POOL_SIZE,cmdInterFace);
		if(0 != ret){
			threadPoolFileScanner.setCorePoolSize(ret);
		}
		ret = cmpConfigUpdate(oldProperties,newProperties,Model.MAX_POOL_SIZE,cmdInterFace);
		if(0 != ret){
			threadPoolFileScanner.setMaximumPoolSize(ret);
		}
		cmpConfigUpdate(oldProperties,newProperties,Model.INTERNAL_THREAD_POOL,cmdInterFace);
		cmpConfigUpdate(oldProperties,newProperties,Model.INTERNAL_MAXTHREAD_POOL,cmdInterFace);
		cmpConfigUpdate(oldProperties,newProperties,Model.BATCHS_PACKAGE_SIZE,cmdInterFace);
		cmpConfigUpdate(oldProperties,newProperties,Model.BATCH_SIZE,cmdInterFace);
    }
    
    private static int cmpConfigUpdate(Map<String, String> oldProperties,Map<String, String> newProperties,String itemText,TaskInterface cmdInterFace){
    	int oldVaue = Integer.parseInt(oldProperties.get(itemText));
		int newValue = Integer.parseInt(newProperties.get(itemText));
		if(newValue != oldVaue){
			cmdInterFace.getLogger().info("The configuration {} has been modified (old={},new={}), reload ...",itemText,oldVaue,newValue);
			oldProperties.put(itemText,""+newValue);
			return newValue;
		}
		return 0;
    }
}
