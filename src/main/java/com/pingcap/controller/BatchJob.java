package com.pingcap.controller;

import com.pingcap.dataformat.DataFactory;
import com.pingcap.dataformat.DataFormatInterface;
import com.pingcap.enums.Model;
import com.pingcap.pojo.BatchDataSource;
import com.pingcap.pojo.LineDataText;
import com.pingcap.task.*;
import io.prometheus.client.Histogram;
import org.apache.commons.lang.StringUtils;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchJob implements Runnable {
	public static final AtomicInteger totalUsedCount = new AtomicInteger(0);
	private final Map<String, Object> propParameters;
    private final String absolutePath;
    private final TiSession tiSession;
    private final List<String> ttlSkipTypeList;
    private final HashMap<String, Long> ttlSkipTypeMap;
    private final List<String> ttlPutList;
    private BatchDataSource lineBlock;
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
    private LinkedHashMap<ByteString, LineDataText> kvPairs = null;
    private LinkedHashMap<ByteString, LineDataText> kvPairsTtl = null;
	
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
            List<LineDataText> lineBlock,
            CountDownLatch countDownLatch,
            AtomicInteger totalDuplicateCount,
            List<String> ttlPutList,
            TaskInterface cmdInterFace,
            Map<String, Object> propParameters) {
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
 
        this.countDownLatch = countDownLatch;
        this.totalDuplicateCount = totalDuplicateCount;
        this.cmdInterFace = cmdInterFace;
        this.propParameters = propParameters;
        
        this.lineBlock = new BatchDataSource(lineBlock);
    }

    @Override
    public void run() {
    	Map<String, String> properties = cmdInterFace.getProperties();
        String scenes = properties.get(Model.SCENES);
        String importMode = properties.get(Model.MODE);

        final int batchSize = Integer.parseInt(properties.get(Model.BATCH_SIZE));
        kvPairs = new LinkedHashMap<>(batchSize);
        kvPairsTtl = new LinkedHashMap<>(batchSize);
        int processCounts;

        RawKVClient rawKvClient = tiSession.createRawClient();

        // The kv pair to be inserted into the raw kv.

        boolean notData;
        DataFactory dataFactory = null;
		try {
			dataFactory = DataFactory.getInstance(importMode,properties);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
        LineDataText dataSource;
        List<LineDataText> lineDataSourceList = lineBlock.getLineDataSourceList();
        for(int row=0;row<lineDataSourceList.size();row++) {
        	dataSource = lineDataSourceList.get(row);
        	final String line = dataSource.getLineData();
        	final int lineNo = dataSource.getLineNo();
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
						LineDataText du = null;
						if(null != ttlType) {
			                if (ttlPutList.contains(ttlType)) {
			                	du = kvPairsTtl.put(key, new LineDataText(lineNo,line,value));
				                if (du != null) {
				                    totalDuplicateCount.incrementAndGet();
				                    cmdInterFace.getLoggerAudit().debug("File={}, key={}, value={}", absolutePath, key.toStringUtf8(), value.toStringUtf8());
				                    return false;
				                }
				                else {
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

						du = kvPairs.put(key, new LineDataText(lineNo,line,value));
			            if (du != null) {
			                totalDuplicateCount.incrementAndGet();
			                cmdInterFace.getLoggerAudit().debug("File={}, key={}, value={}", absolutePath, key.toStringUtf8(), value.toStringUtf8());
			            }
			            else {
			            	dataSize += (key.size()+value.size());
			            }
						du = null;
		                return true;
					}
				});
            } catch (Exception e) {
            	cmdInterFace.getLoggerAudit().error("Parse failed, file={}, data={}, line={}", absolutePath, line, lineNo);
            	int err = totalParseErrorCount.getAndIncrement();
            	//If there is no success and 10 consecutive errors, exit the program
            	if(0 == totalImportCount.get()){
	            	if(err >= 10 && 0 == (kvPairs.size()+kvPairsTtl.size())){
	            		cmdInterFace.getLogger().error("If there are many parsing errors in succession, exit the program.");
	            		System.exit(0);
	            	}
            	}
            	continue;
            }
            finally{
            	parseJsonTimer.observeDuration();
            }
        	if(!notData){
        		continue;
        	}
        	processCounts = kvPairs.size()+kvPairsTtl.size();
        	if(batchSize <= processCounts){
	            if(0 < kvPairs.size()){
	            	TaskInterface.totalDataBytes.addAndGet(doWriteTikv( rawKvClient, kvPairs, lineBlock, false,dataSize));
	            	dataSize = 0;
	            }
	
	            if(0 < kvPairsTtl.size()){
	        		TaskInterface.totalDataBytes.addAndGet(doWriteTikv( rawKvClient, kvPairsTtl, lineBlock, true,dataTtlSize));
	            	dataTtlSize = 0;
	        	}
	            lineBlock.reduceLineDataSourceList(row);
	            row=0;
        	}
        }
        processCounts = kvPairs.size()+kvPairsTtl.size();
        if(0 < processCounts){
	    	if(0 < kvPairs.size()){
	    		TaskInterface.totalDataBytes.addAndGet(doWriteTikv( rawKvClient, kvPairs, lineBlock, false,dataSize));
	    	}
	    	if(0 < kvPairsTtl.size()){
	    		TaskInterface.totalDataBytes.addAndGet(doWriteTikv( rawKvClient, kvPairsTtl, lineBlock, true,dataTtlSize));
	    	}
        }
    	properties = null;
    	lineBlock.destory();
    	lineBlock = null;
        kvPairs = null;
        kvPairsTtl = null;
        
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
 
    private int doWriteTikv(RawKVClient rawKvClient,LinkedHashMap<ByteString, LineDataText> pairs, BatchDataSource dataSource, boolean hasTtl,int dataSize){
    	int ret = 0;
    	LinkedHashMap<ByteString, LineDataText> kvPairs_Jump = new LinkedHashMap<>(1024);
		try {
			ret = cmdInterFace.executeTikv(propParameters, rawKvClient, totalParseErrorCount,pairs, kvPairs_Jump, hasTtl, absolutePath, dataSize);
		 	totalImportCount.addAndGet(pairs.size());
			cmdInterFace.succeedWriteRowsLogger(absolutePath, pairs);
		}
		catch(Exception e){
			totalBatchPutFailCount.addAndGet(pairs.size());
			cmdInterFace.faildWriteRowsLogger(pairs);
			cmdInterFace.getLogger().error("Failed to batch put{}, file={}, size={}", hasTtl?" TTL":"", absolutePath, pairs.size());
		}
		finally{
    		if(0 < kvPairs_Jump.size()){
    			totalSkipCount.addAndGet(kvPairs_Jump.size());
    		}
    		pairs.clear();
    		kvPairs_Jump.clear();
		}
		kvPairs_Jump = null;
		return ret;
    }
}
