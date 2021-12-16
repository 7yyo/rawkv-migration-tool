package com.pingcap.cmd;

import static com.pingcap.enums.Model.CHECK_SUM_LOG;
import static com.pingcap.enums.Model.CS_FAIL_LOG;
import static com.pingcap.enums.Model.MODE;
import static com.pingcap.enums.Model.SCENES;
import static com.pingcap.enums.Model.TTL_SKIP_TYPE;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.pingcap.enums.Model;
import com.pingcap.util.PropertiesUtil;

public class CheckSum implements CmdInterface {
    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger checkSumLog = LoggerFactory.getLogger(CHECK_SUM_LOG);
    private static final Logger csFailLog = LoggerFactory.getLogger(CS_FAIL_LOG);
    @SuppressWarnings("unused")
	private long ttl = 0;
    
	public CheckSum() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Logger getLogger() {
		return logger;
	}

	@Override
	public Logger getLoggerProcess() {
		return checkSumLog;
	}

	@Override
	public Logger getLoggerFail() {
		return csFailLog;
	}

	@Override
	public void checkAllParameters(Map<String, String> properties) {
		CmdInterface.checkShareParameters(properties);
		
        PropertiesUtil.checkConfig(properties, Model.IMPORT_FILE_PATH);
        PropertiesUtil.checkConfig(properties, Model.CHECK_SUM_THREAD_NUM);
        PropertiesUtil.checkConfig(properties, Model.CHECK_SUM_MOVE_PATH);
        PropertiesUtil.checkConfig(properties, MODE);
        PropertiesUtil.checkConfig(properties, SCENES);

        // Skip ttl type when check sum.
        PropertiesUtil.checkConfig(properties, TTL_SKIP_TYPE);
        // Skip ttl put when check sum.
        PropertiesUtil.checkConfig(properties, Model.TTL_PUT_TYPE);

	}

	@Override
	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	@Override
	public boolean executeTikv(RawKVClient rawKvClient, HashMap<ByteString, ByteString> pairs,
			HashMap<ByteString, String> pairs_lines, boolean hasTtl) {
		// TODO Auto-generated method stub
		return false;
	}

}
