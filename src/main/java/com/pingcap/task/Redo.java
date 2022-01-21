package com.pingcap.task;

import static com.pingcap.enums.Model.MODE;
import static com.pingcap.enums.Model.SCENES;
import static com.pingcap.enums.Model.TTL_SKIP_TYPE;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.alibaba.fastjson.JSONObject;
import com.pingcap.controller.FileScanner;
import com.pingcap.controller.ScannerInterface;
import com.pingcap.enums.Model;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.InfoInterface;
import com.pingcap.pojo.TempIndexInfo;
import com.pingcap.rawkv.LimitSpeedkv;
import com.pingcap.util.CountUtil;
import com.pingcap.util.FileUtil;
import com.pingcap.util.PropertiesUtil;

import io.prometheus.client.Histogram;

public class Redo implements TaskInterface {
    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger redoLog = LoggerFactory.getLogger(Model.REDO_LOG);
    private static final Logger redoFailLog = LoggerFactory.getLogger(Model.REDO_FAIL_LOG);
    private static final String AtomicInteger_checkSumFail = "_checkSumFail";
    private static final String AtomicInteger_notInsert = "_notInsert";
    
    private Histogram DURATION = Histogram.build().name("redo duration").help("redo duration").labelNames("type").register();
    private Map<String, String> properties = null;
    
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
	public Map<String, String> getProperties() {
		return properties;
	}

	@Override
	public void setProperties(Map<String, String> properties) {
		checkAllParameters(properties);
		this.properties = properties;
	}

	@Override
	public void installPrivateParamters(Map<String, Object> propParameters) {
    	propParameters.put(AtomicInteger_notInsert, new AtomicInteger(0));
    	propParameters.put(AtomicInteger_checkSumFail, new AtomicInteger(0));
	}

	@Override
	public HashMap<ByteString, ByteString> executeTikv(Map<String, Object> propParameters, RawKVClient rawKvClient,
			HashMap<ByteString, ByteString> pairs, HashMap<ByteString, String> pairs_lines, boolean hasTtl,
			String filePath, Map<String, String> lineBlock, int dataSize) {
		Histogram.Timer batchGetTimer = DURATION.labels("batch_get").startTimer();
		List<Kvrpcpb.KvPair> kvList = null;

		List<ByteString> keyList = new ArrayList<>(pairs.keySet());
		InfoInterface infoRawKV,tmpRawKV;
		Object clazz = new TempIndexInfo();
		if(Model.INDEX_INFO.equals(properties.get(SCENES)))
			clazz = new IndexInfo();
		try {
			// Batch get keys which to check sum
			kvList = LimitSpeedkv.batchGet(rawKvClient,keyList,dataSize);
		} catch (Exception e) {
            for (Entry<ByteString, ByteString> originalKv : pairs.entrySet()) {
            	logger.error("Batch get failed.Key={}, file={}, almost line={}", originalKv.getKey().toStringUtf8(), filePath, lineBlock.get(pairs_lines.get(originalKv.getKey())));
            }
            throw e;
        }
		finally{
			batchGetTimer.observeDuration();
		}

		Map<ByteString, InfoInterface> rawKvResultMap = new HashMap<>(kvList.size());
        for (Kvrpcpb.KvPair kvPair : kvList) {
        	infoRawKV = (InfoInterface)JSONObject.parseObject(kvPair.getValue().toStringUtf8(), clazz.getClass());
        	rawKvResultMap.put(kvPair.getKey(), infoRawKV);
        }
        kvList.clear();
        kvList = null;
        int iCheckSumFail=0,iNotInsert=0;
        Histogram.Timer csTimer = DURATION.labels("redo_sum").startTimer();
        ByteString curKey;
        for (int i=0;i<keyList.size();i++) {
        	curKey = keyList.get(i);
        	tmpRawKV = (InfoInterface)JSONObject.parseObject(pairs.get(curKey).toStringUtf8(), clazz.getClass());
        	if (tmpRawKV.getOpType() == null) {
                redoLog.error("Redo data opType must not be null. File={}, data={}, line={}", filePath, lineBlock.get(pairs_lines.get(curKey)), pairs_lines.get(curKey));
                System.exit(0);
            }
        	infoRawKV = rawKvResultMap.get(curKey);
            switch (tmpRawKV.getOpType()) {
	            case com.pingcap.enums.Model.ADD:
	            case com.pingcap.enums.Model.UPDATE:
	                if (null != infoRawKV) {
	                    if (tmpRawKV.getUpdateTime() != null && infoRawKV.getUpdateTime() != null) {
	                        // Redo > RawKV
	                        if (CountUtil.compareTime( tmpRawKV.getUpdateTime(), infoRawKV.getUpdateTime()) >= 0) {
	                            // if duration == 0, D
	                            if(!put( rawKvClient, curKey, pairs.get(curKey), Long.parseLong(tmpRawKV.getDuration()), lineBlock.get(pairs_lines.get(curKey)), pairs_lines.get(curKey))){
		                        	++iCheckSumFail;
		                            pairs.remove(curKey);
		                            pairs_lines.remove(curKey);
	                            }
	                        } else {
	                        	redoLog.error("times waitting. Key={}, redoTso={} < rawkvTso={}, line={}, so skip. OpType={}", curKey.toStringUtf8(),tmpRawKV.getUpdateTime(), infoRawKV.getUpdateTime(), pairs_lines.get(curKey),tmpRawKV.getOpType());
	                            redoFailLog.info(lineBlock.get(pairs_lines.get(curKey)));
	                            pairs.remove(curKey);
	                            pairs_lines.remove(curKey);
	                            ++iCheckSumFail;
	                        }
	                    } else {
	                        // If timeStamp is null, put
                            if(!put( rawKvClient, curKey, pairs.get(curKey), Long.parseLong(tmpRawKV.getDuration()), lineBlock.get(pairs_lines.get(curKey)), pairs_lines.get(curKey))){
	                        	++iCheckSumFail;
	                            pairs.remove(curKey);
	                            pairs_lines.remove(curKey);
                            }
	                    }
	                } else {
	                    // If raw kv is not exists, put.
                        if(!put( rawKvClient, curKey, pairs.get(curKey), Long.parseLong(tmpRawKV.getDuration()), lineBlock.get(pairs_lines.get(curKey)), pairs_lines.get(curKey))){
                        	++iCheckSumFail;
                            pairs.remove(curKey);
                            pairs_lines.remove(curKey);
                        }
	                }
	                break;
	            case com.pingcap.enums.Model.DELETE:
	                if (null != infoRawKV) {
	                    if (tmpRawKV.getUpdateTime() != null && infoRawKV.getUpdateTime() != null) {
	                        if (CountUtil.compareTime(tmpRawKV.getUpdateTime(), infoRawKV.getUpdateTime()) >= 0) {
		                        if(!delete( rawKvClient, curKey, lineBlock.get(pairs_lines.get(curKey)), pairs_lines.get(curKey))){
		                        	++iCheckSumFail;
		                            pairs.remove(curKey);
		                            pairs_lines.remove(curKey);
		                        }
	                        } else {         
	                        	redoLog.error("times waitting. Key={}, redoTso={} < rawkvTso={}, line={}, so skip. OpType=delete", curKey.toStringUtf8(),tmpRawKV.getUpdateTime(), infoRawKV.getUpdateTime(), pairs_lines.get(curKey));
	                            redoFailLog.info(lineBlock.get(pairs_lines.get(curKey)));
	                            pairs.remove(curKey);
	                            pairs_lines.remove(curKey);
	                            ++iCheckSumFail;
	                        }
	                    } else {
	                        if(!delete( rawKvClient, curKey, lineBlock.get(pairs_lines.get(curKey)), pairs_lines.get(curKey))){
	                        	++iCheckSumFail;
	                            pairs.remove(curKey);
	                            pairs_lines.remove(curKey);
	                        }
	                    }
	                } else {    
	                    redoLog.error("raw KV not exists. Key={}, file={}, line={}, so skip. OpType=delete", curKey.toStringUtf8(),filePath, pairs_lines.get(curKey));
	                    infoRawKV = (InfoInterface)JSONObject.parseObject(pairs.get(curKey).toStringUtf8(), clazz.getClass());
	                    redoFailLog.info(lineBlock.get(pairs_lines.get(curKey)));
	                    pairs.remove(curKey);
	                    pairs_lines.remove(curKey);
	                    ++iNotInsert;
	                }
	                break;
	            default:
	            	redoLog.error("Redo data opType occurred IllegalStateException. File={}, data={}, line={}", filePath, lineBlock.get(pairs_lines.get(curKey)), pairs_lines.get(curKey));
	                throw new IllegalStateException(infoRawKV.getOpType());
            }
        }
        keyList.clear();
        keyList = null;
        rawKvResultMap.clear();
        rawKvResultMap = null;
        if(0<iCheckSumFail){
        	((AtomicInteger)propParameters.get(AtomicInteger_checkSumFail)).addAndGet(iCheckSumFail);
        }
        if(0<iNotInsert){
        	((AtomicInteger)propParameters.get(AtomicInteger_notInsert)).addAndGet(iNotInsert);
        }
        csTimer.observeDuration();
		return pairs;
	}
	
    public static boolean put(RawKVClient rawKVClient, ByteString key, ByteString value, long ttl, String lineData, String lineNo) {
        try {
            rawKVClient.put(key, value, ttl);
        } catch (Exception e) {
            redoLog.error("Redo put failed. key={}, line={}, lineNum={}", key.toStringUtf8(), lineData, lineNo, e);
            redoFailLog.info(lineData);
            return false;
        }
        return true;
    }
    
    public static boolean delete(RawKVClient rawKVClient, ByteString key, String lineData, String lineNo) {
        try {
            rawKVClient.delete(key);
        } catch (Exception e) {
            redoLog.error("Redo delete failed. key={}, line={}, lineNum={}", key.toStringUtf8(), lineData, lineNo, e);
            redoFailLog.info(lineData);
            return false;
        }
        return true;
    }
    
	@Override
	public void succeedWriteRowsLogger(String filePath, HashMap<ByteString, ByteString> pairs) {

	}

	@Override
	public void faildWriteRowsLogger(HashMap<ByteString, ByteString> pairs) {
		for(Entry<ByteString, ByteString> obj:pairs.entrySet()){
			redoFailLog.info(obj.getValue().toStringUtf8());
		}
	}

	@Override
	public ScannerInterface getInitScanner() {
		return new FileScanner();
	}

	@Override
	public void finishedReport(String filePath, int importFileLineNum, int totalImportCount, int totalEmptyCount,
			int totalSkipCount, int totalParseErrorCount, int totalBatchPutFailCount, int totalDuplicateCount,
			long duration, LinkedHashMap<String, Long> ttlSkipTypeMap, Map<String, Object> propParameters) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        String now = simpleDateFormat.format(new Date());
        String moveFilePath = properties.get(Model.REDO_MOVE_PATH);

        int iCheckSumFail = ((AtomicInteger)propParameters.get(AtomicInteger_checkSumFail)).get();
        int iNotInsert = ((AtomicInteger)propParameters.get(AtomicInteger_notInsert)).get();

        File checkSumFile = new File(filePath);
        File moveFile = new File(moveFilePath);
        try {
        	int total = filesNum.incrementAndGet();
        	if(!checkSumFile.getParentFile().getAbsolutePath().equals(moveFile.getAbsolutePath())){
        		moveFile = new File(moveFilePath + "/" + now + "/" + checkSumFile.getName() + "." + total);
        		FileUtils.moveFile(checkSumFile, moveFile);
        	}
        } catch (IOException e) {
            e.printStackTrace();
        }
        StringBuilder result = new StringBuilder(
                "["+getClass().getSimpleName()+" summary]" +
                        ", Process ratio 100% file=" + filePath + ", " +
                        "total=" + importFileLineNum + ", " +
                        "totalCheck=" + totalImportCount + ", " +
                        "empty=" + totalEmptyCount + ", " +
                        "skip=" + totalSkipCount + ", " +
                        "parseErr=" + totalParseErrorCount + ", " +
                        "notExits=" + iNotInsert + ", " +
                        "redoSumFail=" + iCheckSumFail + ", " +
                        "duration=" + duration / 1000 + "s, ");
        result.append("Skip type[");
        for (Map.Entry<String, Long> item : ttlSkipTypeMap.entrySet()) {
            result.append("<").append(item.getKey()).append(">").append("[").append(item.getValue()).append("]").append("]");
        }
        logger.info(result.toString());
	}

	@Override
	public void checkAllParameters(Map<String, String> properties) {
		TaskInterface.checkShareParameters(properties);
		
        PropertiesUtil.checkConfig(properties, Model.IMPORT_FILE_PATH);
        PropertiesUtil.checkConfig(properties, Model.CHECK_SUM_MOVE_PATH);
        PropertiesUtil.checkConfig(properties, MODE);
        PropertiesUtil.checkConfig(properties, SCENES);
        PropertiesUtil.checkNaturalNumber( properties, Model.BATCHS_PACKAGE_SIZE, false);

        // Skip ttl type when check sum.
        PropertiesUtil.checkConfig(properties, TTL_SKIP_TYPE);
        // Skip ttl put when check sum.
        PropertiesUtil.checkConfig(properties, Model.TTL_PUT_TYPE);
        PropertiesUtil.checkNumberFromTo( properties, Model.TASKSPEEDLIMIT, false,20,1000);
        
        PropertiesUtil.checkConfig(properties, Model.REDO_MOVE_PATH);
        PropertiesUtil.checkConfig(properties, Model.REDO_FILE_ORDER);
        String moveFilePath = properties.get(Model.REDO_MOVE_PATH);
        // MoveFilePath
        FileUtil.createFolder(moveFilePath);
	}

	@Override
	public Histogram getHistogram() {
		return DURATION;
	}

}
