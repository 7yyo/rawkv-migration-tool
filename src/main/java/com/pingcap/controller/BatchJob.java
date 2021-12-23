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
    private final String absolutePath;
    private final TiSession tiSession;
    private final List<String> ttlSkipTypeList;
    private final HashMap<String, Long> ttlSkipTypeMap;
    private final List<String> ttlPutList;
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private final Map<String, String> lineBlock = new HashMap(4096);
    private final AtomicInteger totalImportCount;
    private final AtomicInteger totalEmptyCount;
    private final AtomicInteger totalSkipCount;
    private final AtomicInteger totalParseErrorCount;
    private final AtomicInteger totalBatchPutFailCount;
    private final AtomicInteger totalDuplicateCount;
    private final CountDownLatch countDownLatch;
    private TaskInterface cmdInterFace = null;
	
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
    	final Map<String, String> properties = cmdInterFace.getProperties();
        String scenes = properties.get(Model.SCENES);
        String importMode = properties.get(Model.MODE);
        int batchSize = Integer.parseInt(properties.get(Model.BATCH_SIZE));

        // Batch get kv list.
        final HashMap<ByteString, ByteString> kvPairs = new HashMap<>(batchSize+1);
        final HashMap<ByteString, String> kvPairs_lines = new HashMap<>(batchSize+1);
        final HashMap<ByteString, ByteString> kvPairsTtl = new HashMap<>(batchSize+1);
        final HashMap<ByteString, String> kvPairsTtl_lines = new HashMap<>(batchSize+1);
        RawKVClient rawKvClient = tiSession.createRawClient();

        // The kv pair to be inserted into the raw kv.
        String line; // Import file line. Import line col: id, type.
        boolean notData;
        DataFactory dataFactory = DataFactory.getInstance(importMode,properties);

        for(Entry<String, String> pos:lineBlock.entrySet()) {
            final String lineNo= pos.getKey();
            line = pos.getValue();

            // If import file has blank line, continue, recode skip + 1.
            if (StringUtils.isBlank(line)) {
            	cmdInterFace.getLogger().warn("There is blank lines in the file={}, line={}", absolutePath, lineNo);
            	totalEmptyCount.incrementAndGet();
                continue;
            }

            Histogram.Timer parseJsonTimer = cmdInterFace.getHistogram().labels("parse json").startTimer();
            Histogram.Timer toObjTimer = cmdInterFace.getHistogram().labels("to obj").startTimer();
            try {
            	notData = dataFactory.formatToKeyValue( parseJsonTimer, totalParseErrorCount, scenes, line,new DataFormatInterface.DataFormatCallBack() {
					
					@Override
					public boolean putDataCallBack( String ttlType, ByteString key, ByteString value) {
						// If importer.ttl.put.type exists, put with ttl, then continue.
						toObjTimer.observeDuration();
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
			                kvPairs_lines.put(key, lineNo);
			            }
						
		                return true;
					}
				});
            } catch (Exception e) {
            	totalParseErrorCount.getAndIncrement();
            	cmdInterFace.getLoggerAudit().error("Parse failed, file={}, data={}, line={}", absolutePath, line, lineNo);
            	continue;
            }
        	if(!notData)
        		continue;
        	
            if(batchSize <= kvPairs.size()){	            	
            	try{
            		cmdInterFace.executeTikv(rawKvClient, kvPairs, kvPairs_lines, false, absolutePath);
	                totalImportCount.addAndGet(kvPairs.size());
	                cmdInterFace.succeedWriteRowsLogger(absolutePath, kvPairs);
                }
                catch(Exception e){
                	totalBatchPutFailCount.addAndGet(kvPairs.size());
                	cmdInterFace.getLoggerFail().error("Failed to batch put, file={}, size={}, line={}", absolutePath, kvPairs_lines.size(), kvPairs_lines.values().toString());
                }
            	finally{
            		int jumpExistCount = kvPairs_lines.size()-kvPairs.size();
            		if(0 < jumpExistCount){
            			totalSkipCount.addAndGet(jumpExistCount);
            		}
            		kvPairs.clear();
            		kvPairs_lines.clear();
            	}
            }

            if(batchSize <= kvPairsTtl.size()){
            	try{
            		cmdInterFace.executeTikv(rawKvClient, kvPairsTtl, kvPairsTtl_lines, true, absolutePath);
            		totalImportCount.addAndGet(kvPairsTtl.size());
            		cmdInterFace.succeedWriteRowsLogger(absolutePath, kvPairsTtl);
            	}
            	catch(Exception e){
            		totalBatchPutFailCount.addAndGet(kvPairsTtl.size());
            		cmdInterFace.faildWriteRowsLogger(kvPairsTtl);
            		cmdInterFace.getLoggerFail().error("Failed to batch put TTL, file={}, size={}, line={}", absolutePath, kvPairsTtl_lines.size(), kvPairsTtl_lines.values().toString());
            	}
            	finally{
            		int jumpExistCount = kvPairsTtl_lines.size()-kvPairsTtl.size();
            		if(0 < jumpExistCount){
            			totalSkipCount.addAndGet(jumpExistCount);
            		}	            		
            		kvPairsTtl.clear();
            		kvPairsTtl_lines.clear();
            	}
        	}
        }
    	if(0 < kvPairs.size()){
    		try {
    			cmdInterFace.executeTikv(rawKvClient, kvPairs, kvPairs_lines, false, absolutePath);
    			totalImportCount.addAndGet(kvPairs.size());
    			cmdInterFace.succeedWriteRowsLogger(absolutePath, kvPairs);
    		}
    		catch(Exception e){
    			totalBatchPutFailCount.addAndGet(kvPairs.size());
    			cmdInterFace.faildWriteRowsLogger(kvPairs);
    			cmdInterFace.getLoggerFail().error("Failed to batch put, file={}, size={}, line={}", absolutePath, kvPairs_lines.size(), kvPairs_lines.values().toString());
    		}
    		finally{
        		int jumpExistCount = kvPairs_lines.size()-kvPairs.size();
        		if(0 < jumpExistCount){
        			totalSkipCount.addAndGet(jumpExistCount);
        		}
        		kvPairs.clear();
        		kvPairs_lines.clear();
    		}
    	}
    	if(0 < kvPairsTtl.size()){
    		try {
    			cmdInterFace.executeTikv(rawKvClient, kvPairsTtl, kvPairsTtl_lines, true, absolutePath);
    			totalImportCount.addAndGet(kvPairsTtl.size());
    			cmdInterFace.succeedWriteRowsLogger(absolutePath, kvPairsTtl);
    		}
    		catch(Exception e){
    			totalBatchPutFailCount.addAndGet(kvPairsTtl.size());
    			cmdInterFace.faildWriteRowsLogger(kvPairsTtl);
    			cmdInterFace.getLoggerFail().error("Failed to batch put TTL, file={}, size={}, line={}", absolutePath, kvPairsTtl_lines.size(), kvPairsTtl_lines.values().toString());
    		}
    		finally{
        		int jumpExistCount = kvPairsTtl_lines.size()-kvPairsTtl.size();
        		if(0 < jumpExistCount){
        			totalSkipCount.addAndGet(jumpExistCount);
        		}	     
        		kvPairsTtl.clear();
        		kvPairsTtl_lines.clear();
    		}
    	}

        countDownLatch.countDown();
        try {
        	rawKvClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }  
    }
 
}
