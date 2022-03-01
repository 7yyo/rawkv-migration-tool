package com.pingcap.task;

import static com.pingcap.enums.Model.CHECK_SUM_LOG;
import static com.pingcap.enums.Model.CS_FAIL_LOG;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.FileUtils;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.pingcap.controller.FileScanner;
import com.pingcap.controller.ScannerInterface;
import com.pingcap.dataformat.DataFactory;
import com.pingcap.enums.Model;
import com.pingcap.util.FileUtil;
import com.pingcap.util.PropertiesUtil;
import com.pingcap.pojo.InfoInterface;
import com.pingcap.pojo.LineDataText;
import com.pingcap.rawkv.LimitSpeedkv;

import io.prometheus.client.Histogram;

public class CheckSum implements TaskInterface {
    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger checkSumLog = LoggerFactory.getLogger(CHECK_SUM_LOG);
    private static final Logger csFailLog = LoggerFactory.getLogger(CS_FAIL_LOG);
    private static final String AtomicInteger_checkSumFail = "_checkSumFail";
    private static final String AtomicInteger_notInsert = "_notInsert";
    private Map<String, String> properties = null;

    //private String pid = JavaUtil.getPid();
    private Histogram CHECK_SUM_DURATION = Histogram.build().name("checksum_duration").help("Check sum duration").labelNames("type").register();
    private DataFactory dataFactory;
	public CheckSum() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Logger getLogger() {
		return logger;
	}

	@Override
	public Logger getLoggerAudit() {
		return checkSumLog;
	}

	@Override
	public Logger getLoggerFail() {
		return csFailLog;
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
        String moveFilePath = properties.get(Model.CHECK_SUM_MOVE_PATH);
        // MoveFilePath
        FileUtil.createFolder(moveFilePath);
        if(Model.JSON_FORMAT.equals(properties.get(Model.MODE))){
        	if(Model.INDEX_TYPE.equals(properties.get(Model.SCENES))){
        		logger.error("Configuration json format not support indexType of scense");
        		System.exit(0); 
        	}
        }
        else{
            if(Model.TEMP_INDEX_INFO.equals(properties.get(Model.SCENES))&& !Model.ROWB64_FORMAT.equals(properties.get(Model.MODE))){
                logger.error("Configuration csv format not support tempIndexInfo of scense");
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
	public int executeTikv(Map<String, Object> propParameters,RawKVClient rawKvClient, AtomicInteger totalParseErrorCount, LinkedHashMap<ByteString, LineDataText> pairs,
			LinkedHashMap<ByteString, LineDataText> pairs_jmp, boolean hasTtl,String filePath,int dataSize) {
		Histogram.Timer batchGetTimer = CHECK_SUM_DURATION.labels("batch_get").startTimer();
		List<Kvrpcpb.KvPair> kvList = null;
		int ret = 0,ret_rx = 0;
		List<ByteString> keyList = new ArrayList<>(pairs.keySet());
		InfoInterface infoRawKV,tmpRawKV;
		final String scenes = properties.get(SCENES);
		try {
			// Batch get keys which to check sum
        	if(0<keyList.size()){
        		//approximate value
        		ret = (keyList.get(0).size()*keyList.size());
        	}
			kvList = LimitSpeedkv.batchGet(rawKvClient,keyList,ret);
		} catch (Exception e) {
            for (Entry<ByteString, LineDataText> originalKv : pairs.entrySet()) {
            	logger.error("Batch get failed.Key={}, file={}, almost line={}", originalKv.getKey().toStringUtf8(), filePath, originalKv.getValue().getLineData());
            }
            throw e;
        }
		finally{
			batchGetTimer.observeDuration();
		}

		Map<ByteString, InfoInterface> rawKvResultMap = new HashMap<>(kvList.size());
        for (Kvrpcpb.KvPair kvPair : kvList) {
        	try {
        		ret_rx += (kvPair.getKey().size() + kvPair.getValue().size());
				infoRawKV = dataFactory.packageToObject(scenes, kvPair.getKey(), kvPair.getValue(), null);
	        	rawKvResultMap.put(kvPair.getKey(), infoRawKV);
			} catch (Exception e) {
				e.printStackTrace();
			}
        }
        if(0 < ret_rx){
	        LimitSpeedkv.testTraffic(ret_rx);
	        ret += ret_rx;
        }
        kvList.clear();
        kvList = null;
        int iCheckSumFail=0,iNotInsert=0;
        Histogram.Timer csTimer = CHECK_SUM_DURATION.labels("check_sum").startTimer();
        ByteString curKey;
        for (int i=0;i<keyList.size();i++) {
        	curKey = keyList.get(i);
        	infoRawKV = rawKvResultMap.get(curKey);
            if (null != infoRawKV) {
            	try {
					tmpRawKV = dataFactory.packageToObject(scenes, curKey,pairs.get(curKey).getValue(),null);
	                if (!infoRawKV.equalsValue(tmpRawKV)) {
	                    checkSumLog.error("Check sum failed. Key={}", curKey.toStringUtf8());
	                    csFailLog.info(pairs.get(curKey).getLineData());
	                    pairs.remove(curKey);
	                    //pairs_lines.remove(curKey);
	                    ++iCheckSumFail;
	                }
				} catch (Exception e) {
					e.printStackTrace();
					totalParseErrorCount.incrementAndGet();
				}
            } else {
                checkSumLog.error("Key={} is not exists.", curKey.toStringUtf8());
                try {
					////infoRawKV = dataFactory.packageToObject(scenes,curKey,pairs.get(curKey).getValue(),null);
	                csFailLog.info(pairs.get(curKey).getLineData());
	                pairs.remove(curKey);
	                //pairs_lines.remove(curKey);
	                ++iNotInsert;
				} catch (Exception e) {
					e.printStackTrace();
					totalParseErrorCount.incrementAndGet();
				}
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

	@Override
	public Histogram getHistogram() {
		return CHECK_SUM_DURATION;
	}

	@Override
	public void succeedWriteRowsLogger(String filePath, LinkedHashMap<ByteString, LineDataText> pairs) {
	/*	//CheckSum not record sucess log
	for(Entry<ByteString, ByteString> obj:pairs.entrySet()){
			getLogger().debug("File={}, key={}, value={}", filePath, obj.getKey().toStringUtf8(), obj.getValue().toStringUtf8());
		}
	*/
	}

	@Override
	public void faildWriteRowsLogger(LinkedHashMap<ByteString, LineDataText> pairs_lines) {
		for(Entry<ByteString, LineDataText> obj:pairs_lines.entrySet()){
			csFailLog.info(obj.getValue().getLineData());
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
	public void finishedReport(String filePath, int importFileLineNum, int totalImportCount, int totalEmptyCount,
			int totalSkipCount, int totalParseErrorCount, int totalBatchPutFailCount, int totalDuplicateCount,
			long duration, LinkedHashMap<String, Long> ttlSkipTypeMap,Map<String, Object> propParameters) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        String now = simpleDateFormat.format(new Date());
        String moveFilePath = properties.get(Model.CHECK_SUM_MOVE_PATH);

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
                        "duplicate=" + totalDuplicateCount + ", " +
                        "checkSumFail=" + iCheckSumFail + ", " +
                        "duration=" + duration / 1000 + "s, ");
        result.append("Skip type[");
        for (Map.Entry<String, Long> item : ttlSkipTypeMap.entrySet()) {
            result.append("<").append(item.getKey()).append(">").append("[").append(item.getValue()).append("]").append("]");
        }
        logger.info(result.toString());
	}

	@Override
	public void installPrivateParamters(Map<String, Object> propParameters) {
    	propParameters.put(AtomicInteger_notInsert, new AtomicInteger(0));
    	propParameters.put(AtomicInteger_checkSumFail, new AtomicInteger(0));
	}
	
}
