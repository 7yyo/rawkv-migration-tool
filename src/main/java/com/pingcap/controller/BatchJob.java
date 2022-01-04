package com.pingcap.controller;

import com.pingcap.dataformat.DataFactory;
import com.pingcap.dataformat.DataFormatInterface;
import com.pingcap.enums.Model;
import com.pingcap.task.*;
import io.prometheus.client.Histogram;
import org.apache.commons.lang.StringUtils;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchJob implements Runnable {
	public static final AtomicInteger totalUsedCount = new AtomicInteger(0);
	private static int BUFFER_SIZE = 4096;
    private final String absolutePath;
    private final TiSession tiSession;
    private final List<String> ttlSkipTypeList;
    private final HashMap<String, Long> ttlSkipTypeMap;
    private final List<String> ttlPutList;
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private final Map<String, String> lineBlock = new HashMap(BUFFER_SIZE);
    private final AtomicInteger totalImportCount;
    private final AtomicInteger totalEmptyCount;
    private final AtomicInteger totalSkipCount;
    private final AtomicInteger totalParseErrorCount;
    private final AtomicInteger totalBatchPutFailCount;
    private final AtomicInteger totalDuplicateCount;
    private final CountDownLatch countDownLatch;
    private TaskInterface cmdInterFace = null;
    int dataSize = 0;
    int dataTtlSize = 0;
    
    // Batch get kv list.
    private HashMap<ByteString, ByteString> kvPairs = new HashMap<>(BUFFER_SIZE);
    private HashMap<ByteString, String> kvPairs_lines = new HashMap<>(BUFFER_SIZE);
    private HashMap<ByteString, ByteString> kvPairsTtl = new HashMap<>(BUFFER_SIZE);
    private HashMap<ByteString, String> kvPairsTtl_lines = new HashMap<>(BUFFER_SIZE);
	
    public BatchJob(
            TiSession tiSession,
            AtomicInteger totalImportCount,
            AtomicInteger totalEmptyCount,
            AtomicInteger totalSkipCount,
            AtomicInteger totalParseErrorCount,
            AtomicInteger totalBatchPutFailCount,
            String absolutePath,
            List<String> ttlSkipTypeList,
            HashMap<String, Long> ttlSkipTypeMap,
            Map<String, String> lineBlock,
            CountDownLatch countDownLatch,
            AtomicInteger totalDuplicateCount,
            List<String> ttlPutList,
            TaskInterface cmdInterFace) {
    	totalUsedCount.incrementAndGet();
        this.totalImportCount = totalImportCount;
        this.tiSession = tiSession;
        this.totalEmptyCount = totalEmptyCount;
        this.totalSkipCount = totalSkipCount;
        this.totalParseErrorCount = totalParseErrorCount;
        this.totalBatchPutFailCount = totalBatchPutFailCount;
        this.absolutePath = absolutePath;
        this.ttlPutList = ttlPutList;
        this.ttlSkipTypeList = ttlSkipTypeList;
        this.ttlSkipTypeMap = ttlSkipTypeMap;
        this.lineBlock.putAll(lineBlock);
        this.countDownLatch = countDownLatch;
        this.totalDuplicateCount = totalDuplicateCount;
        this.cmdInterFace = cmdInterFace;
    }

    @Override
    public void run() {
    	Map<String, String> properties = cmdInterFace.getProperties();
        String scenes = properties.get(Model.SCENES);
        String importMode = properties.get(Model.MODE);
        final int batchSize = Integer.parseInt(properties.get(Model.BATCH_SIZE));

        RawKVClient rawKvClient = tiSession.createRawClient();

        // The kv pair to be inserted into the raw kv.
        String line; // Import file line. Import line col: id, type.
        boolean notData;
        DataFactory dataFactory = DataFactory.getInstance(importMode,properties);
        for(Entry<String, String> pos:lineBlock.entrySet()) {
            String lineNo= pos.getKey();
            line = pos.getValue();

            // If import file has blank line, continue, recode skip + 1.
            if (StringUtils.isBlank(line)) {
            	cmdInterFace.getLogger().warn("There is blank lines in the file={}, line={}", absolutePath, lineNo);
            	totalEmptyCount.incrementAndGet();
                continue;
            }

            Histogram.Timer parseJsonTimer = cmdInterFace.getHistogram().labels("parse json").startTimer();
            //20220104 delete Histogram.Timer toObjTimer = cmdInterFace.getHistogram().labels("to obj").startTimer();

            try {
            	notData = dataFactory.formatToKeyValue( scenes, line,new DataFormatInterface.DataFormatCallBack() {

					@Override
					public boolean putDataCallBack( String ttlType, ByteString key, ByteString value) {
						// If importer.ttl.put.type exists, put with ttl, then continue.
						ByteString du = null;
						if(null != ttlType) {
			                if (ttlPutList.contains(ttlType)) {
			                	du = kvPairsTtl.put(key, value);
				                if (du != null) {
				                    totalDuplicateCount.incrementAndGet();
				                    cmdInterFace.getLoggerAudit().debug("File={}, key={}, value={}", absolutePath, key.toStringUtf8(), value.toStringUtf8());
				                    return false;
				                }
				                else {
				                	kvPairsTtl_lines.put(key, lineNo);
				                	dataTtlSize += (key.size()+value.size());
				                	return true;
				                }
			                }
			                else {
			                    // If it exists in the ttl type map, skip.
			                    if (ttlSkipTypeList.contains(ttlType)) {
			                        ttlSkipTypeMap.put(ttlType, ttlSkipTypeMap.get(ttlType) + 1);
			                        cmdInterFace.getLoggerAudit().info("Skip key={}, file={}, line={}", key.toStringUtf8(), absolutePath, lineNo);
			                        totalSkipCount.incrementAndGet();
			                        return false;
			                    }
			                }							
						}
						
						du = kvPairs.put(key, value);
			            if (du != null) {
			                totalDuplicateCount.incrementAndGet();
			                cmdInterFace.getLoggerAudit().debug("File={}, key={}, value={}", absolutePath, key.toStringUtf8(), value.toStringUtf8());
			            }
			            else {
			            	dataSize += (key.size()+value.size());
			                kvPairs_lines.put(key, lineNo);
			            }
						du = null;
		                return true;
					}
				});
            } catch (Exception e) {
            	totalParseErrorCount.getAndIncrement();
            	cmdInterFace.getLoggerAudit().error("Parse failed, file={}, data={}, line={}", absolutePath, line, lineNo);
            	continue;
            }
            finally{
            	parseJsonTimer.observeDuration();
            }
        	if(!notData)
        		continue;
        	
        	if(batchSize <= kvPairs.size()+kvPairsTtl.size()){
	            if(0 < kvPairs.size()){
	        		if(doWriteTikv( rawKvClient, kvPairs, kvPairs_lines, lineBlock, false)){
	            		TaskInterface.totalDataBytes.getAndAdd(dataSize);
	            		dataSize = 0;
	        		}
	            }
	
	            if(0 < kvPairsTtl.size()){
	        		if(doWriteTikv( rawKvClient, kvPairsTtl, kvPairsTtl_lines, lineBlock, true)){
	            		TaskInterface.totalDataBytes.getAndAdd(dataTtlSize);
	            		dataTtlSize = 0;
	        		}
	        	}
        	}
        }
        if(0 < kvPairs.size()+kvPairsTtl.size()){
	    	if(0 < kvPairs.size()){
	    		if(doWriteTikv( rawKvClient, kvPairs, kvPairs_lines, lineBlock, false)){
	        		TaskInterface.totalDataBytes.getAndAdd(dataSize);
	    		}
	    	}
	    	if(0 < kvPairsTtl.size()){
	    		if(doWriteTikv( rawKvClient, kvPairsTtl, kvPairsTtl_lines, lineBlock, true)){
	        		TaskInterface.totalDataBytes.getAndAdd(dataTtlSize);
	    		}
	    	}
        }
    	properties = null;
    	lineBlock.clear();
        kvPairs = null;
        kvPairs_lines = null;
        kvPairsTtl = null;
        kvPairsTtl_lines = null;    	
        countDownLatch.countDown();
        try {
        	rawKvClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }  
        finally{
        	rawKvClient = null;
        }
        totalUsedCount.decrementAndGet();
    }
 
    private boolean doWriteTikv(RawKVClient rawKvClient,HashMap<ByteString, ByteString> pairs, HashMap<ByteString, String> pairs_lines,Map<String,String> lineBlock, boolean hasTtl){
    	boolean ret = true;
		try {
			cmdInterFace.executeTikv(rawKvClient, pairs, pairs_lines, hasTtl, absolutePath, lineBlock);
			totalImportCount.addAndGet(pairs.size());
			cmdInterFace.succeedWriteRowsLogger(absolutePath, pairs);
		}
		catch(Exception e){
			ret = false;
			totalBatchPutFailCount.addAndGet(pairs.size());
			cmdInterFace.faildWriteRowsLogger(pairs);
			cmdInterFace.getLoggerFail().error("Failed to batch put{}, file={}, size={}, line={}", hasTtl?"TTL":"", absolutePath, pairs_lines.size(), pairs_lines.values().toString());
		}
		finally{
    		int jumpExistCount = pairs_lines.size()-pairs.size();
    		if(0 < jumpExistCount){
    			totalSkipCount.addAndGet(jumpExistCount);
    		}
/*    		for(Entry<ByteString, String> pos:pairs_lines.entrySet()){
    			lineBlock.remove(pos.getValue());
    		}*/
    		pairs.clear();
    		pairs_lines.clear();
		}
		return ret;
    }
}
