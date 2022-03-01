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

import com.pingcap.controller.FileScanner;
import com.pingcap.controller.ScannerInterface;
import com.pingcap.dataformat.DataFactory;
import com.pingcap.enums.Model;
import com.pingcap.pojo.InfoInterface;
import com.pingcap.pojo.LineDataText;
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
    
    private Histogram DURATION = Histogram.build().name("redoduration").help("redo duration").labelNames("type").register();
    private Map<String, String> properties = null;
    private DataFactory dataFactory;
    
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
	public int executeTikv(Map<String, Object> propParameters, RawKVClient rawKvClient, AtomicInteger totalParseErrorCount,
			LinkedHashMap<ByteString, LineDataText> pairs, LinkedHashMap<ByteString, LineDataText> pairs_jmp, boolean hasTtl,
			String filePath, int dataSize) {
		Histogram.Timer batchGetTimer = DURATION.labels("batch_get").startTimer();
		List<Kvrpcpb.KvPair> kvList = null;
		int ret = dataSize;
		int startLineNo = 0;
		List<ByteString> keyList = new ArrayList<>(pairs.keySet());
		InfoInterface infoRawKV,tmpRawKV = null;
		LineDataText lineData;
		final String scenes = properties.get(SCENES);
		try {
			// Batch get keys which to check sum
        	if(0<keyList.size()){
        		//approximate value
        		ret = (keyList.get(0).size()*keyList.size());
        		startLineNo = pairs.get(keyList.get(0)).getLineNo();
        	}
			kvList = LimitSpeedkv.batchGet(rawKvClient,keyList,ret);
		} catch (Exception e) {
			logger.error("Batch get failed. file={}, almost line={},{}", filePath, startLineNo, pairs.size());
            throw e;
        }
		finally{
			batchGetTimer.observeDuration();
		}

		Map<ByteString, InfoInterface> rawKvResultMap = new HashMap<>(kvList.size());
        for (Kvrpcpb.KvPair kvPair : kvList) {
        	try {
				infoRawKV = dataFactory.packageToObject(scenes, kvPair.getKey(), kvPair.getValue(), null);
	        	rawKvResultMap.put(kvPair.getKey(), infoRawKV);
			} catch (Exception e) {
				e.printStackTrace();
			}
        }
        kvList.clear();
        kvList = null;
        int iCheckSumFail=0,iNotInsert=0;
        Histogram.Timer csTimer = DURATION.labels("redo_sum").startTimer();
        ByteString curKey;
        for (int i=0;i<keyList.size();i++) {
        	curKey = keyList.get(i);
        	try {
				tmpRawKV = dataFactory.packageToObject(scenes, curKey, pairs.get(curKey).getValue(), null);
			} catch (Exception e) {
				e.printStackTrace();
				lineData = pairs.get(curKey);
                redoLog.error("Redo packageToObject exception. File={}, data={}, line={}", filePath, lineData.getLineData(), lineData.getLineNo());
                System.exit(0);
			}
        	if (tmpRawKV.getOpType() == null) {
        		lineData = pairs.get(curKey);
                redoLog.error("Redo data opType must not be null. File={}, data={}, line={}", filePath, lineData.getLineData(), lineData.getLineNo());
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
	                            if(!put( rawKvClient, curKey, pairs, Long.parseLong(tmpRawKV.getDuration()))){
		                        	++iCheckSumFail;
		                            pairs.remove(curKey);
		                            //pairs_lines.remove(curKey);
	                            }
	                        } else {
	                        	lineData = pairs.get(curKey);
	                        	redoLog.error("times waitting. Key={}, redoTso={} < rawkvTso={}, line={}, so skip. OpType={}", curKey.toStringUtf8(),tmpRawKV.getUpdateTime(), infoRawKV.getUpdateTime(), lineData.getLineNo(),tmpRawKV.getOpType());
	                            redoFailLog.info(lineData.getLineData());
	                            pairs.remove(curKey);
	                            //pairs_lines.remove(curKey);
	                            ++iCheckSumFail;
	                        }
	                    } else {
	                        // If timeStamp is null, put
                            if(!put( rawKvClient, curKey, pairs, Long.parseLong(tmpRawKV.getDuration()))){
	                        	++iCheckSumFail;
	                            pairs.remove(curKey);
	                            //pairs_lines.remove(curKey);
                            }
	                    }
	                } else {
	                    // If raw kv is not exists, put.
                        if(!put( rawKvClient, curKey, pairs, Long.parseLong(tmpRawKV.getDuration()))){
                        	++iCheckSumFail;
                            pairs.remove(curKey);
                            //pairs_lines.remove(curKey);
                        }
	                }
	                break;
	            case com.pingcap.enums.Model.DELETE:
	                if (null != infoRawKV) {
	                    if (tmpRawKV.getUpdateTime() != null && infoRawKV.getUpdateTime() != null) {
	                        if (CountUtil.compareTime(tmpRawKV.getUpdateTime(), infoRawKV.getUpdateTime()) >= 0) {
		                        if(!delete( rawKvClient, curKey, pairs)){
		                        	++iCheckSumFail;
		                            pairs.remove(curKey);
		                            //pairs_lines.remove(curKey);
		                        }
	                        } else {
	                        	lineData = pairs.get(curKey);
	                        	redoLog.error("times waitting. Key={}, redoTso={} < rawkvTso={}, line={}, so skip. OpType=delete", curKey.toStringUtf8(),tmpRawKV.getUpdateTime(), infoRawKV.getUpdateTime(), lineData.getLineNo());
	                            redoFailLog.info(lineData.getLineData());
	                            pairs.remove(curKey);
	                            //pairs_lines.remove(curKey);
	                            ++iCheckSumFail;
	                        }
	                    } else {
	                        if(!delete( rawKvClient, curKey, pairs)){
	                        	++iCheckSumFail;
	                            pairs.remove(curKey);
	                            //pairs_lines.remove(curKey);
	                        }
	                    }
	                } else {
	                	lineData = pairs.get(curKey);
	                    redoLog.error("raw KV not exists. Key={}, file={}, line={}, so skip. OpType=delete", curKey.toStringUtf8(),filePath, lineData.getLineData());
	                    ////infoRawKV = (InfoInterface)JSONObject.parseObject(lineData.getLineData(), clazz.getClass());
	                    redoFailLog.info(lineData.getLineData());
	                    pairs.remove(curKey);
	                    //pairs_lines.remove(curKey);
	                    ++iNotInsert;
	                }
	                break;
	            default:
	            	redoLog.error("Redo data opType occurred IllegalStateException. File={}, data={}, line={}", filePath, pairs.get(curKey).getLineData(), pairs.get(curKey).getLineNo());
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
		return ret;
	}
	
    public static boolean put(RawKVClient rawKVClient, ByteString key, LinkedHashMap<ByteString, LineDataText> pairs, long ttl) {
    	LineDataText lineData = pairs.get(key);
        try {
            rawKVClient.put(key, lineData.getValue(), ttl);
        } catch (Exception e) {
            redoLog.error("Redo put failed. key={}, line={}, lineNum={}", key.toStringUtf8(), lineData.getLineData(), lineData.getLineNo(), e);
            redoFailLog.info(lineData.getLineData());
            return false;
        }
        return true;
    }
    
    public static boolean delete(RawKVClient rawKVClient, ByteString key, LinkedHashMap<ByteString, LineDataText> lineDatas) {
        try {
            rawKVClient.delete(key);
        } catch (Exception e) {
        	LineDataText data = lineDatas.get(key);
            redoLog.error("Redo delete failed. key={}, line={}, lineNum={}", key.toStringUtf8(), data.getLineData(), data.getLineNo(), e);
            redoFailLog.info(data.getLineData());
            return false;
        }
        return true;
    }
    
	@Override
	public void succeedWriteRowsLogger(String filePath, LinkedHashMap<ByteString, LineDataText> pairs) {

	}

	@Override
	public void faildWriteRowsLogger(LinkedHashMap<ByteString, LineDataText> pairs_lines) {
		for(Entry<ByteString, LineDataText> obj:pairs_lines.entrySet()){
			redoFailLog.info(obj.getValue().getLineData());
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

        File redoFile = new File(filePath);
        File moveFile = new File(moveFilePath);
        try {
        	int total = filesNum.incrementAndGet();
        	if(!redoFile.getParentFile().getAbsolutePath().equals(moveFile.getAbsolutePath())){
        		moveFile = new File(moveFilePath + "/" + now + "/" + redoFile.getName() + "." + total);
        		FileUtils.moveFile(redoFile, moveFile);
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

        // Skip ttl type when check sum.
        PropertiesUtil.checkConfig(properties, TTL_SKIP_TYPE);
        // Skip ttl put when check sum.
        PropertiesUtil.checkConfig(properties, Model.TTL_PUT_TYPE);
        
        PropertiesUtil.checkConfig(properties, Model.REDO_MOVE_PATH);
        String moveFilePath = properties.get(Model.REDO_MOVE_PATH);
        // MoveFilePath
        FileUtil.createFolder(moveFilePath);
        if(!Model.JSON_FORMAT.equals(properties.get(Model.MODE))){
            logger.error("The redo function does not support non JSON format data");
            System.exit(0);  
        }
        else{
	        if(Model.INDEX_TYPE.equals(properties.get(Model.SCENES))){
	            logger.error("Configuration json format not support indexType of scense");
	            System.exit(0);  
	        }
        }
        try {
			dataFactory = DataFactory.getInstance(properties.get(Model.MODE),properties);
		} catch (Exception e) {
            logger.error("illegal file format");
            System.exit(0);
		}
	}

	@Override
	public Histogram getHistogram() {
		return DURATION;
	}

}
