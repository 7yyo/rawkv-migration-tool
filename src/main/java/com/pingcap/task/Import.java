package com.pingcap.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.pingcap.controller.FileScanner;
import com.pingcap.controller.ScannerInterface;
import com.pingcap.enums.Model;
import com.pingcap.util.PropertiesUtil;
import io.prometheus.client.Histogram;

public class Import implements TaskInterface {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger loggerFail = LoggerFactory.getLogger(Model.BP_FAIL_LOG);
    private static final Logger auditLog = LoggerFactory.getLogger(Model.AUDIT_LOG);
    private Map<String, String> properties = null;
    //private String pid = JavaUtil.getPid();
    private Histogram DURATION = Histogram.build().name("duration").help("Everything duration").labelNames("type").register();
    
    private long ttl = 0;

	public Import() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Logger getLogger() {
		return logger;
	}

	@Override
	public Logger getLoggerFail() {
		return loggerFail;
	}

	@Override
	public Logger getLoggerAudit() {
		return auditLog;
	}

	@Override
	public void checkAllParameters(Map<String, String> properties) {
		TaskInterface.checkShareParameters(properties);

		PropertiesUtil.checkConfig(properties, Model.CHECK_EXISTS_KEY);
        PropertiesUtil.checkConfig(properties, Model.IMPORT_FILE_PATH);
        PropertiesUtil.checkConfig(properties, Model.TTL_SKIP_TYPE);
        PropertiesUtil.checkConfig(properties, Model.TTL_PUT_TYPE);
        PropertiesUtil.checkConfig(properties, Model.SCENES);
        PropertiesUtil.checkConfig(properties, Model.MODE);
        PropertiesUtil.checkConfig(properties, Model.ENV_ID);
        PropertiesUtil.checkConfig(properties, Model.APP_ID);
        PropertiesUtil.checkConfig(properties, Model.UPDATE_TIME);
        
        PropertiesUtil.checkNaturalNumber( properties, Model.BATCHS_PACKAGE_SIZE, false);
        PropertiesUtil.checkNaturalNumber( properties, Model.TTL, false);
	}

	@Override
	public HashMap<ByteString, ByteString> executeTikv(RawKVClient rawKvClient, HashMap<ByteString, ByteString> pairs,
			HashMap<ByteString, String> pairs_lines, boolean hasTtl,String filePath,final Map<String, String> lineBlock) {
		long startTime = System.currentTimeMillis();
        List<Kvrpcpb.KvPair> kvHaveList = null;
        if (Model.ON.equals(properties.get(Model.CHECK_EXISTS_KEY))) {
            // Only json file skip exists key.
            ////String importMode = properties.get(Model.MODE);
            ////if (Model.JSON_FORMAT.equals(importMode)) {

                // For batch get to check exists kv
                ////List<ByteString> kvList = new ArrayList<>(pairs.keySet());
                Histogram.Timer batchGetTimer = REQUEST_LATENCY.labels("batch get").startTimer();
                try {
                    // Batch get from raw kv.
                    kvHaveList = rawKvClient.batchGet(new ArrayList<>(pairs.keySet()));
                } catch (Exception e) {
                	TaskInterface.BATCH_PUT_FAIL_COUNTER.labels("batch put fail").inc();
                    throw e;
                }
                finally{
                	batchGetTimer.observeDuration();
                }
                for (Kvrpcpb.KvPair kv : kvHaveList) {	
                	auditLog.info("Skip exists key={}, file={}, almost line={}", kv.getKey().toStringUtf8(), filePath, lineBlock.get(pairs_lines.get(kv.getKey())));
                	pairs.remove(kv.getKey());
                }
            ////}
        }
        Histogram.Timer batchPutTimer = REQUEST_LATENCY.labels("batch put").startTimer();
    	if(hasTtl) {
    		rawKvClient.batchPut(pairs);
    	}
    	else {
    		rawKvClient.batchPut(pairs,ttl);
    	}
    	batchPutTimer.observeDuration();
		logger.debug("executeTikv, File={}, linesSize={}, usedTime={}", filePath, pairs.size(), String.format("%.2f", (float)(System.currentTimeMillis()-startTime)/1000));
		return pairs;
	}

	@Override
	public Histogram getHistogram() {
		return DURATION;
	}

	@Override
	public void succeedWriteRowsLogger(String filePath, HashMap<ByteString, ByteString> pairs) {
		for(Entry<ByteString, ByteString> obj:pairs.entrySet()){
			logger.debug("File={}, key={}, value={}", filePath, obj.getKey().toStringUtf8(), obj.getValue().toStringUtf8());
		}
	}

	@Override
	public void faildWriteRowsLogger(HashMap<ByteString, ByteString> pairs) {
		for(Entry<ByteString, ByteString> obj:pairs.entrySet()){
			loggerFail.info(obj.getValue().toStringUtf8());
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

	@Override
	public ScannerInterface getInitScanner() {
		return new FileScanner();
	}

	@Override
	public void finishedReport(String filePath,
			int importFileLineNum,
			int totalImportCount,
			int totalEmptyCount,
			int totalSkipCount,
			int totalParseErrorCount,
			int totalBatchPutFailCount,
			int totalDuplicateCount,
			long duration,
			LinkedHashMap<String, Long> ttlSkipTypeMap){
		filesNum.incrementAndGet();
        StringBuilder result = new StringBuilder(
                "["+getClass().getSimpleName()+" summary]" +
                        ", Process ratio 100% file=" + filePath + ", " +
                        "total=" + importFileLineNum + ", " +
                        "imported=" + totalImportCount + ", " +
                        "empty=" + totalEmptyCount + ", " +
                        "skip=" + totalSkipCount + ", " +
                        "parseErr=" + totalParseErrorCount + ", " +
                        "putErr=" + totalBatchPutFailCount + ", " +
                        "duplicate=" + totalDuplicateCount + ", " +
                        "duration=" + duration / 1000 + "s, ");
        result.append("Skip type[");
        for (Map.Entry<String, Long> item : ttlSkipTypeMap.entrySet()) {
            result.append("<").append(item.getKey()).append(">").append("[").append(item.getValue()).append("]").append("]");
        }
        logger.info(result.toString());
        
	}
}