package com.pingcap.controller;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;

import com.pingcap.enums.Model;
import com.pingcap.task.TaskInterface;
import com.pingcap.util.JavaUtil;
import com.pingcap.util.PropertiesUtil;

public class ExeFactory {
	private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private TiSession tiSession = null;
    private Map<String, String> properties = null;
    private String task;
    
	public static ExeFactory getInstance(TiSession tiSession,String task,Map<String, String> properties) {
        if (StringUtils.isBlank(task)) {
            logger.error("{} cannot be empty.", Model.TASK);
            return new ExeFactory(null,task,properties);
        }
		return new ExeFactory(tiSession,task,properties);
	}
	
	public ExeFactory(TiSession tiSession,String task,Map<String, String> properties) {
		this.properties = properties;
		this.task = task;
		this.tiSession = tiSession;
	}

	public static void checkEnv(Map<String, String> properties) {
        PropertiesUtil.checkConfig(properties, Model.WRITE_TIMEOUT);
        PropertiesUtil.checkConfig(properties, Model.READ_TIMEOUT);
        PropertiesUtil.checkConfig(properties, Model.BATCH_WRITE_TIMEOUT);
        PropertiesUtil.checkConfig(properties, Model.BATCH_READ_TIMEOUT);
        PropertiesUtil.checkConfig(properties, Model.SCAN_TIMEOUT);
        PropertiesUtil.checkConfig(properties, Model.CLEAN_TIMEOUT);		
	}
	
    private TaskInterface getCmdInterface() {
    	String taskClass = TaskInterface.class.getPackage().getName()+"." + task;
    	if(!JavaUtil.hasClass(taskClass)) {
    		System.out.println("unknow function " + task);
    		System.exit(0);
    	}
        TaskInterface cmdInterFace = (TaskInterface) JavaUtil.newClazz( taskClass );
        cmdInterFace.setProperties(properties);
        return cmdInterFace;
    }
    
	public void run() {
		if(null != tiSession){
			FileScanner scanner = new FileScanner(task);
			scanner.run( tiSession , getCmdInterface() );
		}
	}

}
