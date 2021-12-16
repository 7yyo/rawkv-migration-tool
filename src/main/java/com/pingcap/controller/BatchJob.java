package com.pingcap.controller;

import com.pingcap.cmd.*;
import com.pingcap.dataformat.DataFactory;
import com.pingcap.dataformat.DataFormatInterface;
import com.pingcap.enums.Model;
import com.pingcap.util.PropertiesUtil;
import io.prometheus.client.Histogram;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchJob extends Thread {

    private Logger logger = null;
    private Logger auditLog = null;
    private Logger bpFailLog = null;

    static final Histogram DURATION = Histogram.build().name("duration").help("Everything duration").labelNames("type").register();

    private final String task;
    private final String absolutePath;
    private final TiSession tiSession;
    private final List<String> ttlSkipTypeList;
    private final HashMap<String, Long> ttlSkipTypeMap;
    private final List<String> ttlPutList;
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private final Map<String, String> lineBlock = new HashMap(4096);
    ////private final String fileBlock;
    private final AtomicInteger totalImportCount;
    private final AtomicInteger totalSkipCount;
    private final AtomicInteger totalParseErrorCount;
    private final AtomicInteger totalBatchPutFailCount;
    private final AtomicInteger totalDuplicateCount;
    private final Map<String, String> properties;
    private final CountDownLatch countDownLatch;
    private CmdInterface cmdInterFace = null;
	
    public BatchJob(
    		String task,
            TiSession tiSession,
            AtomicInteger totalImportCount,
            AtomicInteger totalSkipCount,
            AtomicInteger totalParseErrorCount,
            AtomicInteger totalBatchPutFailCount,
            String absolutePath,
            List<String> ttlSkipTypeList,
            HashMap<String, Long> ttlSkipTypeMap,
            ////String fileBlock,
            Map<String, String> lineBlock,
            Map<String, String> properties,
            CountDownLatch countDownLatch,
            AtomicInteger totalDuplicateCount,
            List<String> ttlPutList,
            CmdInterface cmdInterFace) {
    	this.task = task;
        this.totalImportCount = totalImportCount;
        this.tiSession = tiSession;
        this.totalSkipCount = totalSkipCount;
        this.totalParseErrorCount = totalParseErrorCount;
        this.totalBatchPutFailCount = totalBatchPutFailCount;
        this.absolutePath = absolutePath;
        this.ttlPutList = ttlPutList;
        this.ttlSkipTypeList = ttlSkipTypeList;
        this.ttlSkipTypeMap = ttlSkipTypeMap;
        this.lineBlock.putAll(lineBlock);
        ////this.fileBlock = fileBlock;
        this.properties = properties;
        this.countDownLatch = countDownLatch;
        this.totalDuplicateCount = totalDuplicateCount;
        this.cmdInterFace = cmdInterFace;
        logger = cmdInterFace.getLogger();
        auditLog = cmdInterFace.getLoggerProcess();
        bpFailLog = cmdInterFace.getLoggerProcess(); 
    }

    @Override
    public void run() {
    	cmdInterFace.setTtl(Long.parseLong(properties.get(Model.TTL)));
        String scenes = properties.get(Model.SCENES);
        String importMode = properties.get(Model.MODE);      
        int batchSize = Integer.parseInt(properties.get(Model.BATCH_SIZE));

        // Batch get kv list.
        final HashMap<ByteString, ByteString> kvPairs = new HashMap<>(batchSize+1);
        final HashMap<ByteString, String> kvPairs_lines = new HashMap<>(batchSize+1);
        final HashMap<ByteString, ByteString> kvPairsTtl = new HashMap<>(batchSize+1);
        final HashMap<ByteString, String> kvPairsTtl_lines = new HashMap<>(batchSize+1);
        RawKVClient rawKvClient = tiSession.createRawClient();
        ////try {
            // The kv pair to be inserted into the raw kv.
            String line; // Import file line. Import line col: id, type.
            boolean notData;
            DataFactory dataFactory = DataFactory.getInstance(importMode,properties);

            for(Entry<String, String> pos:lineBlock.entrySet()) {
                final String lineNo= pos.getKey();
                line = pos.getValue();

                // If import file has blank line, continue, recode skip + 1.
                if (StringUtils.isBlank(line)) {
                    logger.warn("There is blank lines in the file={}, line={}", absolutePath, lineNo);
                    totalSkipCount.addAndGet(1);
                    continue;
                }

                Histogram.Timer parseJsonTimer = DURATION.labels("parse json").startTimer();
                Histogram.Timer toObjTimer = DURATION.labels("to obj").startTimer();
                try {
                	notData = dataFactory.formatToKeyValue( parseJsonTimer, totalParseErrorCount, scenes, lineNo,new DataFormatInterface.DataFormatCallBack() {
						
						@Override
						public boolean putDataCallBack( String ttlType, ByteString key, ByteString value) {
							// If importer.ttl.put.type exists, put with ttl, then continue.
							toObjTimer.observeDuration();
							//logger.debug("File={}, key={}, value={}", absolutePath, key.toStringUtf8(), value.toStringUtf8());
							ByteString du = null;
							if(null != ttlType) {
				                if (ttlPutList.contains(ttlType)) {
				                	du = kvPairsTtl.put(key, value);
					                if (du != null) {
					                    totalDuplicateCount.addAndGet(1);
					                }
					                else {
					                	kvPairsTtl_lines.put(key, lineNo);
					                }
				                }
				                else {
				                    // If it exists in the ttl type map, skip.
				                    if (ttlSkipTypeList.contains(ttlType)) {
				                        ttlSkipTypeMap.put(ttlType, ttlSkipTypeMap.get(ttlType) + 1);
				                        auditLog.info("Skip key={}, file={}, line={}", key.toStringUtf8(), absolutePath, lineNo);
				                        totalSkipCount.addAndGet(1);
				                        return false;
				                    }
				                    ////logger.debug("File={}, key={}, value={}", absolutePath, key.toStringUtf8(), value.toStringUtf8());
				                }							
							}
							else {
								du = kvPairs.put(key, value);
				                if (du != null) {
				                    totalDuplicateCount.addAndGet(1);
				                }
				                else {
				                	kvPairs_lines.put(key, lineNo);
				                }
							}
			                return true;
						}
					});
                	if(!notData)
                		continue;
                	if(batchSize <= kvPairs.size())
                		cmdInterFace.executeTikv(rawKvClient, kvPairs, kvPairs_lines, false);
                	if(batchSize <= kvPairsTtl.size())
                		cmdInterFace.executeTikv(rawKvClient, kvPairs, kvPairs_lines, true);
                } catch (Exception e) {
                	auditLog.error("Parse failed, file={}, data={}, line={}", absolutePath, line, lineNo);
                	continue;
                }
            }
        	if(0 < kvPairs.size())
        		cmdInterFace.executeTikv(rawKvClient, kvPairs, kvPairs_lines, false);
        	if(0 < kvPairsTtl.size())
        		cmdInterFace.executeTikv(rawKvClient, kvPairs, kvPairs_lines, true);

            countDownLatch.countDown();
            try {
            	rawKvClient.close();
            } catch (Exception e) {
                e.printStackTrace();
            }  
    }
 
}
