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

import com.alibaba.fastjson.JSONObject;
import com.pingcap.controller.FileScanner;
import com.pingcap.controller.ScannerInterface;
import com.pingcap.dataformat.DataFactory;
import com.pingcap.enums.Model;
import com.pingcap.util.PropertiesUtil;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.InfoInterface;
import com.pingcap.pojo.TempIndexInfo;
import io.prometheus.client.Histogram;

public class CheckSum implements TaskInterface {
    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger checkSumLog = LoggerFactory.getLogger(CHECK_SUM_LOG);
    private static final Logger csFailLog = LoggerFactory.getLogger(CS_FAIL_LOG);
    private Map<String, String> properties = null;
    
    //private String pid = JavaUtil.getPid();
    private Histogram CHECK_SUM_DURATION = Histogram.build().name("checksum_duration").help("Check sum duration").labelNames("type").register();
    
    @SuppressWarnings("unused")
	private long ttl = 0;
    // Total not in rawKv num
    AtomicInteger notInsert = new AtomicInteger(0);
    // Total check sum fail num
    AtomicInteger checkSumFail = new AtomicInteger(0);
    DataFactory dataFactory;
    
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
        PropertiesUtil.checkNaturalNumber( properties, Model.BATCHS_PACKAGE_SIZE, false);

        // Skip ttl type when check sum.
        PropertiesUtil.checkConfig(properties, TTL_SKIP_TYPE);
        // Skip ttl put when check sum.
        PropertiesUtil.checkConfig(properties, Model.TTL_PUT_TYPE);
        dataFactory = DataFactory.getInstance(properties.get(Model.MODE),properties);
	}

	@Override
	public HashMap<ByteString, ByteString> executeTikv(RawKVClient rawKvClient, HashMap<ByteString, ByteString> pairs,
			HashMap<ByteString, String> pairs_lines, boolean hasTtl,String filePath,final Map<String, String> lineBlock) {
		Histogram.Timer batchGetTimer = CHECK_SUM_DURATION.labels("batch_get").startTimer();
		List<Kvrpcpb.KvPair> kvList = null;

		List<ByteString> keyList = new ArrayList<>(pairs.keySet());
		InfoInterface infoRawKV,tmpRawKV;
		Object clazz = new TempIndexInfo();
		if(Model.INDEX_INFO.equals(properties.get(SCENES)))
			clazz = new IndexInfo();
		try {
			// Batch get keys which to check sum
			kvList = rawKvClient.batchGet(keyList);
		} catch (Exception e) {
            for (Entry<ByteString, ByteString> originalKv : pairs.entrySet()) {
            	logger.error("Batch get failed.Key={}, file={}, almost line={}", originalKv.getKey().toStringUtf8(), filePath, lineBlock.get(pairs_lines.get(originalKv.getKey())));
            }
            throw e;
        }
		finally{
			keyList.clear();
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
        Histogram.Timer csTimer = CHECK_SUM_DURATION.labels("check_sum").startTimer();
        for (Entry<ByteString, ByteString> originalKv : pairs.entrySet()) {
        	infoRawKV = rawKvResultMap.get(originalKv.getKey());
            if (null != infoRawKV) {
            	tmpRawKV = (InfoInterface)JSONObject.parseObject(originalKv.getValue().toStringUtf8(), clazz.getClass());
                if (!infoRawKV.equalsValue(tmpRawKV)) {
                    checkSumLog.error("Check sum failed. Key={}", originalKv.getKey().toStringUtf8());
                    csFailLog.info(lineBlock.get(pairs_lines.get(originalKv.getKey())));
                    pairs.remove(originalKv.getKey());
                    pairs_lines.remove(originalKv.getKey());
                    ++iCheckSumFail;
                }
            } else {
                checkSumLog.error("Key={} is not exists.", originalKv.getKey().toStringUtf8());
                infoRawKV = (InfoInterface)JSONObject.parseObject(originalKv.getValue().toStringUtf8(), clazz.getClass());
                csFailLog.info(lineBlock.get(pairs_lines.get(originalKv.getKey())));
                pairs.remove(originalKv.getKey());
                pairs_lines.remove(originalKv.getKey());
                ++iNotInsert;
            }
        }
        rawKvResultMap.clear();
        rawKvResultMap = null;
        if(0<iCheckSumFail)
        	checkSumFail.addAndGet(iCheckSumFail);
        if(0<iNotInsert)
        	notInsert.addAndGet(iNotInsert);
        csTimer.observeDuration();
		return pairs;
	}

	@Override
	public Histogram getHistogram() {
		return CHECK_SUM_DURATION;
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
			csFailLog.info(obj.getValue().toStringUtf8());
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
			long duration, LinkedHashMap<String, Long> ttlSkipTypeMap) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        String now = simpleDateFormat.format(new Date());
        String moveFilePath = properties.get(Model.CHECK_SUM_MOVE_PATH);
        File checkSumFile = new File(filePath);
        File moveFile = new File(moveFilePath + "/" + now + "/" + checkSumFile.getName() + "." + filesNum.incrementAndGet());
        try {
            FileUtils.moveFile(checkSumFile, moveFile);
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
                        "notExits=" + notInsert.get() + ", " +
                        "checkSumFail=" + checkSumFail.get() + ", " +
                        "duration=" + duration / 1000 + "s, ");
        result.append("Skip type[");
        for (Map.Entry<String, Long> item : ttlSkipTypeMap.entrySet()) {
            result.append("<").append(item.getKey()).append(">").append("[").append(item.getValue()).append("]").append("]");
        }
        logger.info(result.toString());
	}
	
}
