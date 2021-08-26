package com.pingcap.checksum;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import static com.pingcap.enums.Model.*;

import com.pingcap.enums.Model;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.TempIndexInfo;
import com.pingcap.timer.CheckSumTimer;
import com.pingcap.util.CountUtil;
import com.pingcap.util.FileUtil;
import io.prometheus.client.Histogram;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CheckSumJsonJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger checkSumLog = LoggerFactory.getLogger(CHECK_SUM_LOG);
    private static final Logger csFailLog = LoggerFactory.getLogger(CS_FAIL_LOG);

    static final Histogram CHECK_SUM_DURATION = Histogram.build().name("check_sum_duration").help("Check sum duration").labelNames("type").register();

    private final String checkSumFilePath;
    private final TiSession tiSession;
    private final Map<String, String> properties;
    private final AtomicInteger fileNum;
    private final String now;

    public CheckSumJsonJob(String checkSumFilePath, TiSession tiSession, Map<String, String> properties, AtomicInteger fileNum, String now) {
        this.checkSumFilePath = checkSumFilePath;
        this.tiSession = tiSession;
        this.properties = properties;
        this.fileNum = fileNum;
        this.now = now;
    }

    @Override
    public void run() {

//        String simpleCheckSum = properties.get(SIMPLE_CHECK_SUM);
//        if (!ON.equals(simpleCheckSum)) {
//            CheckSum.checkSum(checkSumFilePath, tiSession, properties);
//        } else {
//            CheckSum.doRedo(checkSumFilePath, tiSession, properties);
//        }

//        String simpleCheckSum = properties.get(SIMPLE_CHECK_SUM);
//
//        if (ON.equals(simpleCheckSum)) {
//            CheckSum.doRedo(checkSumFilePath, tiSession, properties);
//        } else {

        logger.info("Start check sum file={}", checkSumFilePath);

        List<String> ttlSkipTypeList = new ArrayList<>(Arrays.asList(properties.get(TTL_SKIP_TYPE).split(",")));

        int parseErr = 0;
        int notInsert = 0;
        int checkSumFail = 0;
        AtomicInteger totalLine = new AtomicInteger(0);

        RawKVClient rawKvClient = tiSession.createRawClient();

        File checkSumFile = new File(checkSumFilePath);

        Timer timer = new Timer();
        CheckSumTimer checkSumTimer = new CheckSumTimer(checkSumFilePath, totalLine, FileUtil.getFileLines(checkSumFile));
        timer.schedule(checkSumTimer, 5000, Long.parseLong(properties.get(TIMER_INTERVAL)));

        IndexInfo indexInfoRawKv, indexInfoOriginal, rawKvIndexInfoValue;
        TempIndexInfo tempIndexInfoRawKv, tempIndexInfoOriginal, rawKvTempIndexInfoValue;
        Map<String, IndexInfo> originalIndexInfoMap = new HashMap<>(), rawKvIndexInfoMap = new HashMap<>();
        Map<String, TempIndexInfo> originalTempIndexInfoMap = new HashMap<>(), rawKvTempIndexInfoMap = new HashMap<>();
        List<ByteString> keyList = new ArrayList<>();
        List<Kvrpcpb.KvPair> kvList = new ArrayList<>();
        JSONObject jsonObject;
        String key, value, envId, checkSumFileLine;
        int limit = 0, totalCount = 0;
        int limitSize = Integer.parseInt(properties.get(CHECK_SUM_LIMIT));
        String delimiter1 = properties.get(DELIMITER_1);
        String delimiter2 = properties.get(DELIMITER_2);
        int lineCount = FileUtil.getFileLines(checkSumFile);

        LineIterator originalLineIterator = FileUtil.createLineIterator(checkSumFile);
        if (originalLineIterator != null) {
            while (originalLineIterator.hasNext()) {
                Histogram.Timer parseTimer = CHECK_SUM_DURATION.labels("parse").startTimer();
                limit++;
                totalCount++;
                totalLine.addAndGet(1);
                checkSumFileLine = originalLineIterator.nextLine();
                switch (properties.get(MODE)) {
                    case JSON_FORMAT:
                        try {
                            jsonObject = JSONObject.parseObject(checkSumFileLine);
                        } catch (Exception e) {
                            parseErr++;
                            continue;
                        }
                        switch (properties.get(SCENES)) {
                            case INDEX_INFO:

                                indexInfoOriginal = JSON.toJavaObject(jsonObject, IndexInfo.class);
                                if (ttlSkipTypeList.contains(indexInfoOriginal.getType())) {
                                    continue;
                                }

                                if (properties.get(ENV_ID) != null) {
                                    envId = properties.get(ENV_ID);
                                } else {
                                    envId = indexInfoOriginal.getEnvId();
                                }

                                key = String.format(IndexInfo.KET_FORMAT, envId, indexInfoOriginal.getType(), indexInfoOriginal.getId());

                                // TODO
                                if (DELETE.equals(indexInfoOriginal.getOpType())) {
                                    value = rawKvClient.get(ByteString.copyFromUtf8(key)).toStringUtf8();
                                    if (!value.isEmpty()) {
                                        indexInfoRawKv = JSONObject.parseObject(value, IndexInfo.class);
                                        if (CountUtil.compareTime(indexInfoRawKv.getUpdateTime(), indexInfoOriginal.getUpdateTime()) <= 0) {
                                            logger.error("Check sum key={} delete fail, rawkv tso={} > delete tso={}", key, indexInfoRawKv.getUpdateTime(), indexInfoOriginal.getUpdateTime());
                                            checkSumFail++;
                                        }
                                    }
                                    continue;
                                }
                                originalIndexInfoMap.put(key, indexInfoOriginal);
                                keyList.add(ByteString.copyFromUtf8(key));
                                break;
                            case TEMP_INDEX_INFO:
                                tempIndexInfoOriginal = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                                if (properties.get(ENV_ID) != null) {
                                    envId = properties.get(ENV_ID);
                                } else {
                                    envId = tempIndexInfoOriginal.getEnvId();
                                }
                                key = String.format(TempIndexInfo.KEY_FORMAT, envId, tempIndexInfoOriginal.getId());
                                if (DELETE.equals(tempIndexInfoOriginal.getOpType())) {
                                    value = rawKvClient.get(ByteString.copyFromUtf8(key)).toStringUtf8();
                                    if (!value.isEmpty()) {
                                        logger.error("Check sum key={} delete fail.", key);
                                        checkSumFail++;
                                    }
                                    continue;
                                }
                                originalTempIndexInfoMap.put(key, tempIndexInfoOriginal);
                                keyList.add(ByteString.copyFromUtf8(key));
                                break;
                            default:
                        }
                        break;
                    case CSV_FORMAT:
                        indexInfoOriginal = new IndexInfo();
                        IndexInfo.csv2IndexInfo(indexInfoOriginal, checkSumFileLine, delimiter1, delimiter2);
                        key = String.format(IndexInfo.KET_FORMAT, indexInfoOriginal.getEnvId(), indexInfoOriginal.getType(), indexInfoOriginal.getId());
                        originalIndexInfoMap.put(key, indexInfoOriginal);
                        break;
                    default:
                }
                parseTimer.observeDuration();

                if (limit == limitSize || totalCount == lineCount) {
                    try {
                        Histogram.Timer batchGetTimer = CHECK_SUM_DURATION.labels("batch_get").startTimer();
                        kvList = rawKvClient.batchGet(keyList);
                        batchGetTimer.observeDuration();
                    } catch (Exception e) {
                        logger.error("Batch get failed. file={}, line={}", checkSumFile.getAbsolutePath(), totalCount);
                    }
                    Histogram.Timer csTimer = CHECK_SUM_DURATION.labels("check_sum").startTimer();
                    switch (properties.get(SCENES)) {
                        case INDEX_INFO:
                            for (Kvrpcpb.KvPair kvPair : kvList) {
                                indexInfoRawKv = JSONObject.parseObject(kvPair.getValue().toStringUtf8(), IndexInfo.class);
                                rawKvIndexInfoMap.put(kvPair.getKey().toStringUtf8(), indexInfoRawKv);
                            }
                            for (Map.Entry<String, IndexInfo> originalIndexInfoKv : originalIndexInfoMap.entrySet()) {
                                rawKvIndexInfoValue = rawKvIndexInfoMap.get(originalIndexInfoKv.getKey());
                                if (rawKvIndexInfoValue != null) {
                                    if (!rawKvIndexInfoValue.equals(originalIndexInfoKv.getValue())) {
                                        checkSumLog.error("Check sum failed! Key={}", originalIndexInfoKv.getKey());
                                        csFailLog.info(JSON.toJSONString(originalIndexInfoKv.getValue()));
                                        checkSumFail++;
                                    }
                                } else {
                                    checkSumLog.error("Key={} is not exists.", originalIndexInfoKv.getKey());
                                    csFailLog.info(JSON.toJSONString(originalIndexInfoKv.getValue()));
                                    notInsert++;
                                }
                            }
                            break;
                        case TEMP_INDEX_INFO:
                            for (Kvrpcpb.KvPair kvPair : kvList) {
                                tempIndexInfoRawKv = JSONObject.parseObject(kvPair.getValue().toStringUtf8(), TempIndexInfo.class);
                                rawKvTempIndexInfoMap.put(kvPair.getKey().toStringUtf8(), tempIndexInfoRawKv);
                            }
                            for (Map.Entry<String, TempIndexInfo> originalKv : originalTempIndexInfoMap.entrySet()) {
                                rawKvTempIndexInfoValue = rawKvTempIndexInfoMap.get(originalKv.getKey());
                                if (rawKvTempIndexInfoValue != null) {
                                    if (!rawKvTempIndexInfoValue.equals(originalKv.getValue())) {
                                        checkSumLog.error("Check sum failed. Key={}", originalKv.getKey());
                                        csFailLog.info(JSON.toJSONString(originalKv.getValue()));
                                        checkSumFail++;
                                    }
                                } else {
                                    checkSumLog.error("Key={} is not exists.", originalKv.getKey());
                                    csFailLog.info(JSON.toJSONString(originalKv.getValue()));
                                    notInsert++;
                                }
                            }
                            break;
                        default:
                    }
                    limit = 0;
                    kvList.clear();
                    keyList.clear();
                    originalIndexInfoMap.clear();
                    rawKvIndexInfoMap.clear();
                    originalTempIndexInfoMap.clear();
                    rawKvTempIndexInfoMap.clear();
                    csTimer.observeDuration();
                }
            }
        }
        try {
            originalLineIterator.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        timer.cancel();
        logger.info("Check sum file={} complete. Total={}, notExists={}, skipParseErr={}, checkSumFail={}", checkSumFile.getAbsolutePath(), totalLine, notInsert, parseErr, checkSumFail);

        String moveFilePath = properties.get(CHECK_SUM_MOVE_PATH);
        File moveFile = new File(moveFilePath + "/" + now + "/" + checkSumFile.getName() + "." + fileNum.addAndGet(1));
        try {
            FileUtils.moveFile(checkSumFile, moveFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

//}
