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
        FileInputStream fileInputStream = null;
        try {
        	fileInputStream = new FileInputStream(filePath);
            InputStream inputStream = new BufferedInputStream(fileInputStream);
            properties.load(inputStream);
            logger.info(String.valueOf(properties));
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Configuration file({}) loading error:{}", filePath,e.getMessage());
            System.exit(0); 	
        }
        finally{
        	if(null != fileInputStream){
        		try {
					fileInputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
        	}
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
    
    public static void checkNumberFromTo(Map<String, String> properties,String paramName,boolean canEmpty,int min,int max) {
    	String temp = properties.get(paramName);
    	if(null == temp) {
    		if(canEmpty) {
    			return;
    		}
    		else {
                logger.error("Configuration {} of item must exist", paramName);
                System.exit(0);    			
    		}
    	}
    	temp = temp.trim();
        if("".equals(temp)) {
            logger.error("Configuration {} of item don't is blank character", paramName);
            System.exit(0);
        }
        int num = Integer.parseInt(temp);
        if(min > num || max < num) {
            logger.error("Configuration {} of item from {} to {}", paramName,min,max);
            System.exit(0);        	
        }
        return;
    }
    
    public static synchronized void reloadConfiguration(ThreadPoolExecutor threadPoolFileScanner,TaskInterface cmdInterFace){
    	try{
	    	Map<String, String> oldProperties = cmdInterFace.getProperties();
	    	String configFilePath = oldProperties.get(Model.SYS_CFG_PATH);
			Map<String, String> newProperties = PropertiesUtil.getProperties(configFilePath);
			int poolSizeMax = cmpConfigUpdate(oldProperties,newProperties,Model.MAX_POOL_SIZE,cmdInterFace);
			int poolSize = cmpConfigUpdate(oldProperties,newProperties,Model.CORE_POOL_SIZE,cmdInterFace);
			if(poolSizeMax >= poolSize){
				if(0 != poolSizeMax){
					threadPoolFileScanner.setMaximumPoolSize(poolSizeMax);
				}
				if(0 != poolSize){
					threadPoolFileScanner.setCorePoolSize(poolSize);
				}
			}
			else{
				logger.error("Error configuration {} cannot be greater than {}",Model.CORE_POOL_SIZE,Model.MAX_POOL_SIZE);
			}
			cmpConfigUpdate(oldProperties,newProperties,Model.INTERNAL_THREAD_POOL,cmdInterFace);
			cmpConfigUpdate(oldProperties,newProperties,Model.INTERNAL_MAXTHREAD_POOL,cmdInterFace);
			cmpConfigUpdate(oldProperties,newProperties,Model.BATCHS_PACKAGE_SIZE,cmdInterFace);
			cmpConfigUpdate(oldProperties,newProperties,Model.BATCH_SIZE,cmdInterFace);
			cmpConfigUpdate(oldProperties,newProperties,Model.TASKSPEEDLIMIT,cmdInterFace);
    	}
    	catch(Exception e){
    		e.printStackTrace();
    	}
    }
    
    private static int cmpConfigUpdate(Map<String, String> oldProperties,Map<String, String> newProperties,String itemText,TaskInterface cmdInterFace){
    	int oldValue = Integer.parseInt(oldProperties.get(itemText));
		int newValue = Integer.parseInt(newProperties.get(itemText));
		if(newValue != oldValue){
			cmdInterFace.getLogger().info("The configuration {} has been modified (old={},new={}), reload ...",itemText,oldValue,newValue);
			oldProperties.put(itemText,""+newValue);
			return newValue;
		}
		return 0;
    }
    
}
