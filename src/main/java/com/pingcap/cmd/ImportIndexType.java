package com.pingcap.cmd;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.pingcap.enums.Model;
import com.pingcap.util.PropertiesUtil;

public class ImportIndexType implements CmdInterface {
    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private long ttl = 0;
    
	public ImportIndexType() {
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	@Override
	public Logger getLogger() {
		return logger;
	}

	@Override
	public Logger getLoggerFail() {
		return logger;
	}

	@Override
	public Logger getLoggerProcess() {
		return logger;
	}

	@Override
	public void checkAllParameters(Map<String, String> properties) {
		CmdInterface.checkShareParameters(properties);
		
        PropertiesUtil.checkConfig(properties, Model.IMPORT_FILE_PATH);
        PropertiesUtil.checkConfig(properties, Model.TTL_SKIP_TYPE);
        PropertiesUtil.checkConfig(properties, Model.TTL_PUT_TYPE);
        PropertiesUtil.checkConfig(properties, Model.SCENES);
        PropertiesUtil.checkConfig(properties, Model.MODE);
        PropertiesUtil.checkConfig(properties, Model.ENV_ID);
        PropertiesUtil.checkConfig(properties, Model.APP_ID);
        PropertiesUtil.checkConfig(properties, Model.UPDATE_TIME);
        PropertiesUtil.checkNaturalNumber( properties, Model.TTL, false);
    	// check importer.in.rollback value must be greater than 0 or null
        PropertiesUtil.checkNaturalNumber( properties, Model.ROLLBACK, true);
	}

	@Override
	public boolean executeTikv(RawKVClient rawKvClient, HashMap<ByteString, ByteString> pairs,
			HashMap<ByteString, String> pairs_lines, boolean hasTtl) {
    	if(hasTtl) {
    		rawKvClient.batchPut(pairs);
    	}
    	else {
    		rawKvClient.batchPut(pairs,ttl);
    	}
    	return true;
	}

}
