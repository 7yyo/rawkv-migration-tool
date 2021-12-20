package com.pingcap.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.pingcap.enums.Model;
import com.pingcap.util.JavaUtil;
import com.pingcap.util.PropertiesUtil;

import io.prometheus.client.Histogram;

public class ImportIndexType implements TaskInterface {
    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private Map<String, String> properties = null;
    private String pid = JavaUtil.getPid();
    private Histogram IMPORT_INDEXTYPE_DURATION = Histogram.build().name("import_indextype_duration+"+pid).help("indexType duration").labelNames("type").register();
    
    private long ttl = 0;
    
	public ImportIndexType() {
		// TODO Auto-generated constructor stub
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
	public Logger getLoggerAudit() {
		return logger;
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
        PropertiesUtil.checkNaturalNumber( properties, Model.TTL, false);
    	// check importer.in.rollback value must be greater than 0 or null
        PropertiesUtil.checkNaturalNumber( properties, Model.ROLLBACK, true);
	}

	@Override
	public HashMap<ByteString, ByteString> executeTikv(RawKVClient rawKvClient, HashMap<ByteString, ByteString> pairs,
			HashMap<ByteString, String> pairs_lines, boolean hasTtl,String filePath) {
        List<Kvrpcpb.KvPair> kvHaveList = null;
        //It jump check exists key when use rollback
        if (Model.ON.equals(properties.get(Model.CHECK_EXISTS_KEY)) && StringUtils.isBlank(properties.get(Model.ROLLBACK))) {
            // Only json file skip exists key.
            String importMode = properties.get(Model.MODE);
            if (Model.JSON_FORMAT.equals(importMode)) {

                // For batch get to check exists kv
                ////List<ByteString> kvList = new ArrayList<>(pairs.keySet());
                Histogram.Timer batchGetTimer = TaskInterface.REQUEST_LATENCY.labels("batch get").startTimer();
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
                	logger.info("Skip exists key={}, file={}, almost line={}", kv.getKey().toStringUtf8(), filePath, pairs_lines.get(kv.getKey()));
                	pairs.remove(kv.getKey());
                }
            }
        }
        Histogram.Timer batchPutTimer = REQUEST_LATENCY.labels("batch put").startTimer();
        try {
	        if(hasTtl) {
	        	rawKvClient.batchPut(pairs);
	        }
	        else {
	        	rawKvClient.batchPut(pairs,ttl);
	        }
        }
        catch (Exception e) {
        	TaskInterface.BATCH_PUT_FAIL_COUNTER.labels("batch put fail").inc();
            throw e;
        }
        finally{
        	batchPutTimer.observeDuration();
        }
    	return pairs;
	}

	@Override
	public Histogram getHistogram() {
		return IMPORT_INDEXTYPE_DURATION;
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
			logger.info(obj.getValue().toStringUtf8());
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
