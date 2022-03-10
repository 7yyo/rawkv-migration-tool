package com.pingcap.task;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.pingcap.controller.FileScanner;
import com.pingcap.controller.ScannerInterface;
import com.pingcap.enums.Model;
import com.pingcap.pojo.LineDataText;
import com.pingcap.rawkv.LimitSpeedkv;
import com.pingcap.util.PropertiesUtil;
import io.prometheus.client.Histogram;

public class Import implements TaskInterface {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger loggerFail = LoggerFactory.getLogger(Model.BP_FAIL_LOG);
    private static final Logger auditLog = LoggerFactory.getLogger(Model.AUDIT_LOG);
    private Map<String, String> properties = null;
    //private String pid = JavaUtil.getPid();
    private Histogram DURATION = Histogram.build().name("duration").help("Everything duration").labelNames("type").register();
    
    private long ttl = 604800;

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

        PropertiesUtil.checkConfig(properties, Model.IMPORT_FILE_PATH);
        PropertiesUtil.checkConfig(properties, Model.TTL_SKIP_TYPE);
        PropertiesUtil.checkConfig(properties, Model.TTL_PUT_TYPE);
        PropertiesUtil.checkConfig(properties, Model.SCENES);
        PropertiesUtil.checkConfig(properties, Model.MODE);       
        PropertiesUtil.checkConfig(properties, Model.UPDATE_TIME);
        
        PropertiesUtil.checkNaturalNumber( properties, Model.TTL, false);
        ttl = Integer.parseInt(properties.get(Model.TTL));
        if(Model.JSON_FORMAT.equals(properties.get(Model.MODE))){
        	if(Model.INDEX_TYPE.equals(properties.get(Model.SCENES))){
        		logger.error("Configuration json format not support indexType of scense");
        		System.exit(0); 
        	}
        }
        else{
        	PropertiesUtil.checkConfig(properties, Model.APP_ID);
            PropertiesUtil.checkConfig(properties, Model.ENV_ID);
            if(Model.TEMP_INDEX_INFO.equals(properties.get(Model.SCENES))&& !Model.ROWB64_FORMAT.equals(properties.get(Model.MODE))){
                logger.error("Configuration csv format not support tempIndexInfo of scense");
                System.exit(0);  
            }
        }
	}

	@Override
	public int executeTikv(Map<String, Object> propParameters, RawKVClient rawKvClient, AtomicInteger totalParseErrorCount, LinkedHashMap<ByteString, LineDataText> pairs,
			LinkedHashMap<ByteString, LineDataText> pairs_jmp, boolean hasTtl,String filePath,int dataSize) {
        List<Kvrpcpb.KvPair> kvHaveList = null;
        int ret = dataSize;
        if (Model.ON.equals(properties.get(Model.CHECK_EXISTS_KEY))) {
                // For batch get to check exists kv
                List<ByteString> kvList = new ArrayList<>(pairs.keySet());
                Histogram.Timer batchGetTimer = REQUEST_LATENCY.labels("batch get").startTimer();
                try {
                    // Batch get from raw kv.
                	if(0<kvList.size()){
                		//approximate value
                		ret += (kvList.get(0).size()*kvList.size());
                	}
                    kvHaveList = LimitSpeedkv.batchGet(rawKvClient,kvList,0==ret?1:ret);
                } catch (Exception e) {
                	TaskInterface.BATCH_PUT_FAIL_COUNTER.labels("batch put fail").inc();
                    throw e;
                }
                finally{
                	kvList.clear();
                	kvList = null;
                	batchGetTimer.observeDuration();
                }
                ByteString  curKey;
                for (Kvrpcpb.KvPair kv : kvHaveList) {
                	curKey = kv.getKey();
                	pairs_jmp.put(curKey,pairs.get(curKey));
                	auditLog.info("Skip exists key={}, file={}, almost line={}", curKey.toStringUtf8(), filePath, pairs.get(curKey).getLineData());
                	pairs.remove(curKey);
                }
        }
        Histogram.Timer batchPutTimer = REQUEST_LATENCY.labels("batch put").startTimer();
    	if(hasTtl) {
    		LimitSpeedkv.batchPut(rawKvClient,pairs,ttl,dataSize);	
    	}
    	else {
    		LimitSpeedkv.batchPut(rawKvClient,pairs,dataSize);
    	}
    	batchPutTimer.observeDuration();
		return ret;
	}

	@Override
	public Histogram getHistogram() {
		return DURATION;
	}

	@Override
	public void succeedWriteRowsLogger(String filePath, LinkedHashMap<ByteString, LineDataText> pairs) {
		for(Entry<ByteString, LineDataText> obj:pairs.entrySet()){
			logger.debug("File={}, key={}, value={}", filePath, obj.getKey().toStringUtf8(), obj.getValue().getValue().toStringUtf8());
		}
	}

	@Override
	public void faildWriteRowsLogger(LinkedHashMap<ByteString, LineDataText> pairs_lines) {
		for(Entry<ByteString, LineDataText> obj:pairs_lines.entrySet()){
			loggerFail.info(obj.getValue().getLineData());
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
			LinkedHashMap<String, Long> ttlSkipTypeMap,
			Map<String, Object> propParameters){
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

	@Override
	public void installPrivateParamters(Map<String, Object> propParameters) {
		// TODO Auto-generated method stub
	}
}
