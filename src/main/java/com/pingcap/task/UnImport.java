package com.pingcap.task;

import static com.pingcap.enums.Model.REDO_FAIL_LOG;
import static com.pingcap.enums.Model.REDO_FILE_PATH;
import static com.pingcap.enums.Model.REDO_MOVE_PATH;
import static com.pingcap.enums.Model.REDO_TYPE;

import java.util.ArrayList;
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

public class UnImport implements TaskInterface {
    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger redoLog = LoggerFactory.getLogger(Model.REDO_LOG);
    private static final Logger redoFailLog = LoggerFactory.getLogger(REDO_FAIL_LOG);
    private Map<String, String> properties = null;
    private String pid = JavaUtil.getPid();
    private Histogram UNIMPORT_DURATION = Histogram.build().name("unimport_duration_"+pid).help("unimport duration").labelNames("type").register();
    
    @SuppressWarnings("unused")
	private long ttl = 0;
    
	public UnImport() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Logger getLogger() {
		return logger;
	}

	@Override
	public Logger getLoggerAudit() {
		return redoLog;
	}

	@Override
	public Logger getLoggerFail() {
		return redoFailLog;
	}

	@Override
	public void checkAllParameters(Map<String, String> properties) {
		TaskInterface.checkShareParameters(properties);
		
        // Redo file
        PropertiesUtil.checkConfig(properties, REDO_FILE_PATH);
        // Redo.log move path
        PropertiesUtil.checkConfig(properties, REDO_MOVE_PATH);
        // Redo data type, indexInfo or tempIndexInfo
        PropertiesUtil.checkConfig(properties, REDO_TYPE);
	}

	@Override
	public HashMap<ByteString, ByteString> executeTikv(RawKVClient rawKvClient, HashMap<ByteString, ByteString> pairs,
			HashMap<ByteString, String> pairs_lines, boolean hasTtl,String filePath) {
		rawKvClient.batchDelete(new ArrayList<>(pairs.keySet()));
		return pairs;
	}

	@Override
	public Histogram getHistogram() {
		return UNIMPORT_DURATION;
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
			redoFailLog.info(obj.getValue().toStringUtf8());
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
