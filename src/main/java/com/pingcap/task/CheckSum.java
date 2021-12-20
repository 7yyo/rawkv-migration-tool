package com.pingcap.task;

import static com.pingcap.enums.Model.CHECK_SUM_LOG;
import static com.pingcap.enums.Model.CS_FAIL_LOG;
import static com.pingcap.enums.Model.MODE;
import static com.pingcap.enums.Model.SCENES;
import static com.pingcap.enums.Model.TTL_SKIP_TYPE;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.pingcap.enums.Model;
import com.pingcap.util.JavaUtil;
import com.pingcap.util.PropertiesUtil;
import io.prometheus.client.Histogram;

public class CheckSum implements TaskInterface {
    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger checkSumLog = LoggerFactory.getLogger(CHECK_SUM_LOG);
    private static final Logger csFailLog = LoggerFactory.getLogger(CS_FAIL_LOG);
    private Map<String, String> properties = null;
    private String pid = JavaUtil.getPid();
    private Histogram CHECK_SUM_DURATION = Histogram.build().name("checksum_duration+"+pid).help("Check sum duration").labelNames("type").register();
    
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
	public Logger getLoggerAudit() {
		return checkSumLog;
	}

	@Override
	public Logger getLoggerFail() {
		return csFailLog;
	}

	@Override
	public void checkAllParameters(Map<String, String> properties) {
		TaskInterface.checkShareParameters(properties);
		
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
	public HashMap<ByteString, ByteString> executeTikv(RawKVClient rawKvClient, HashMap<ByteString, ByteString> pairs,
			HashMap<ByteString, String> pairs_lines, boolean hasTtl,String filePath) {
		// TODO Auto-generated method stub
		return pairs;
	}

	@Override
	public Histogram getHistogram() {
		return CHECK_SUM_DURATION;
	}

	@Override
	public void succeedWriteRowsLogger(String filePath, HashMap<ByteString, ByteString> pairs) {
		for(Entry<ByteString, ByteString> obj:pairs.entrySet()){
			getLogger().debug("File={}, key={}, value={}", filePath, obj.getKey().toStringUtf8(), obj.getValue().toStringUtf8());
		}
	}

	@Override
	public void faildWriteRowsLogger(HashMap<ByteString, ByteString> pairs) {
		for(Entry<ByteString, ByteString> obj:pairs.entrySet()){
			csFailLog.info(obj.getValue().toStringUtf8());
		}
	}
	
	@Override
	public void setProperties(Map<String, String> properties) {
        checkAllParameters(properties);
		this.properties = properties;
	}

	@Override
	public Map<String, String> getProperties() {
		return properties;
	}
	
}
