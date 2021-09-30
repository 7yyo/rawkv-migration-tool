package com.pingcap.checksum;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import static com.pingcap.enums.Model.*;

import com.pingcap.enums.Model;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.TempIndexInfo;
import com.pingcap.timer.CheckSumTimer;
import com.pingcap.util.FileUtil;
import com.pingcap.util.PropertiesUtil;
import io.prometheus.client.Histogram;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
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

        logger.info("Start check sum file={}", checkSumFilePath);

        PropertiesUtil.checkConfig(properties, MODE);
        PropertiesUtil.checkConfig(properties, SCENES);

        // Skip ttl type when check sum.
        PropertiesUtil.checkConfig(properties, TTL_SKIP_TYPE);
        List<String> ttlSkipTypeList = new ArrayList<>(Arrays.asList(properties.get(TTL_SKIP_TYPE).split(",")));

        // Skip ttl put when check sum.
        PropertiesUtil.checkConfig(properties, Model.TTL_PUT_TYPE);
        List<String> ttlPutList = new ArrayList<>(Arrays.asList(properties.get(Model.TTL_PUT_TYPE).split(",")));

        // Total skip num
        AtomicInteger skip = new AtomicInteger(0);
        // Total parse err num
        AtomicInteger parseErr = new AtomicInteger(0);
        // Total not in rawKv num
        AtomicInteger notInsert = new AtomicInteger(0);
        // Total check sum fail num
        AtomicInteger checkSumFail = new AtomicInteger(0);
        // Total check sum num = Total - total skip
        AtomicInteger totalCheck = new AtomicInteger(0);

        RawKVClient rawKvClient = tiSession.createRawClient();

        File checkSumFile = new File(checkSumFilePath);

        Timer timer = new Timer();
        CheckSumTimer checkSumTimer = new CheckSumTimer(checkSumFilePath, totalCheck, FileUtil.getFileLines(checkSumFile));
        PropertiesUtil.checkConfig(properties, TIMER_INTERVAL);
        timer.schedule(checkSumTimer, 5000, Long.parseLong(properties.get(TIMER_INTERVAL)));

        IndexInfo indexInfoOriginal;
        TempIndexInfo tempIndexInfoOriginal;
        Map<String, IndexInfo> originalIndexInfoMap = new HashMap<>(), rawKvIndexInfoMap = new HashMap<>();
        Map<String, TempIndexInfo> originalTempIndexInfoMap = new HashMap<>(), rawKvTempIndexInfoMap = new HashMap<>();
        List<ByteString> keyList = new ArrayList<>();
        JSONObject jsonObject;
        String key, value, envId, checkSumFileLine;
        int limit = 0, totalCount = 0;

        PropertiesUtil.checkConfig(properties, CHECK_SUM_LIMIT);
        int limitSize = Integer.parseInt(properties.get(CHECK_SUM_LIMIT));

        PropertiesUtil.checkConfig(properties, KEY_DELIMITER);
        String keyDelimiter = properties.get(KEY_DELIMITER);

        PropertiesUtil.checkConfig(properties, DELIMITER_1);
        PropertiesUtil.checkConfig(properties, DELIMITER_2);
        String delimiter1 = properties.get(DELIMITER_1);
        String delimiter2 = properties.get(DELIMITER_2);

        // Check sum file line num
        int lineCount = FileUtil.getFileLines(checkSumFile);

        LineIterator originalLineIterator = FileUtil.createLineIterator(checkSumFile);
        if (originalLineIterator != null) {
//            while (originalLineIterator.hasNext()) {
            for (int n = 0; n < lineCount; n++) {
                Histogram.Timer parseTimer = CHECK_SUM_DURATION.labels("parse").startTimer();
                limit++;
                // How many lines we check sum.
                totalCount++;
                try {
                    checkSumFileLine = originalLineIterator.nextLine();
                } catch (Exception e) {
//                    logger.error("LineIterator error, file = {}", checkSumFile.getAbsolutePath(), e);
//                    skip.addAndGet(1);
                    limit = checkSum(limit, limitSize, totalCount, lineCount, rawKvClient, keyList, properties, checkSumFile, originalIndexInfoMap, rawKvIndexInfoMap, checkSumFail, notInsert, rawKvTempIndexInfoMap, originalTempIndexInfoMap, totalCheck);
                    continue;
                }

                if (StringUtils.isBlank(checkSumFileLine) || "".equals(checkSumFileLine.trim())) {
                    logger.warn("There is blank lines in the file={}, line={}", checkSumFile.getAbsolutePath(), totalCount);
                    skip.addAndGet(1);
                    limit = checkSum(limit, limitSize, totalCount, lineCount, rawKvClient, keyList, properties, checkSumFile, originalIndexInfoMap, rawKvIndexInfoMap, checkSumFail, notInsert, rawKvTempIndexInfoMap, originalTempIndexInfoMap, totalCheck);
                    continue;
                }

                switch (properties.get(MODE)) {
                    case JSON_FORMAT:
                        try {
                            jsonObject = JSONObject.parseObject(checkSumFileLine);
                        } catch (Exception e) {
                            parseErr.addAndGet(1);
                            logger.error("Failed to parse file={}, data={}, line={}", checkSumFile, checkSumFileLine, totalCount);
                            limit = checkSum(limit, limitSize, totalCount, lineCount, rawKvClient, keyList, properties, checkSumFile, originalIndexInfoMap, rawKvIndexInfoMap, checkSumFail, notInsert, rawKvTempIndexInfoMap, originalTempIndexInfoMap, totalCheck);
                            continue;
                        }
                        switch (properties.get(SCENES)) {
                            case INDEX_INFO:

                                indexInfoOriginal = JSON.toJavaObject(jsonObject, IndexInfo.class);

                                if (!StringUtils.isEmpty(properties.get(ENV_ID))) {
                                    envId = properties.get(ENV_ID);
                                } else {
                                    envId = indexInfoOriginal.getEnvId();
                                }

                                key = String.format(IndexInfo.KET_FORMAT, keyDelimiter, envId, keyDelimiter, indexInfoOriginal.getType(), keyDelimiter, indexInfoOriginal.getId());

                                if (DELETE.equals(indexInfoOriginal.getOpType())) {
                                    value = rawKvClient.get(ByteString.copyFromUtf8(key)).toStringUtf8();
                                    if (!value.isEmpty()) {
//                                        indexInfoRawKv = JSONObject.parseObject(value, IndexInfo.class);
//                                        if (indexInfoRawKv.getUpdateTime() != null && indexInfoOriginal.getUpdateTime() != null) {
//                                            if (CountUtil.compareTime(indexInfoRawKv.getUpdateTime().replaceAll("T", " ").replaceAll("Z", ""), indexInfoOriginal.getUpdateTime().replaceAll("T", " ").replaceAll("Z", "")) <= 0) {
//                                                logger.error("Check sum key={} delete fail, rawkv tso={} <= delete tso={}", key, indexInfoRawKv.getUpdateTime(), indexInfoOriginal.getUpdateTime());
//                                                checkSumFail.addAndGet(1);
//                                            }
//                                        }
                                        checkSumLog.error("Check sum delete fail, it still exists, key={}, file={}, line={}", key, checkSumFile.getAbsolutePath(), totalCount);
                                        csFailLog.info(JSON.toJSONString(indexInfoOriginal));
                                        checkSumFail.addAndGet(1);
                                    }
                                    totalCheck.addAndGet(1);
                                    limit = checkSum(limit, limitSize, totalCount, lineCount, rawKvClient, keyList, properties, checkSumFile, originalIndexInfoMap, rawKvIndexInfoMap, checkSumFail, notInsert, rawKvTempIndexInfoMap, originalTempIndexInfoMap, totalCheck);
                                    continue;
                                }

                                // TTL first
                                if (ttlPutList.contains(indexInfoOriginal.getType())) {
                                    skip.addAndGet(1);
                                    logger.error("Skip ttl put key={}, file={}, line={}", key, checkSumFile.getAbsolutePath(), totalCount);
                                    limit = checkSum(limit, limitSize, totalCount, lineCount, rawKvClient, keyList, properties, checkSumFile, originalIndexInfoMap, rawKvIndexInfoMap, checkSumFail, notInsert, rawKvTempIndexInfoMap, originalTempIndexInfoMap, totalCheck);
                                    continue;
                                }

                                // Skip second
                                if (ttlSkipTypeList.contains(indexInfoOriginal.getType())) {
                                    skip.addAndGet(1);
                                    logger.error("Skip ttl type key={}, file={}, line={}", key, checkSumFile.getAbsolutePath(), totalCount);
                                    limit = checkSum(limit, limitSize, totalCount, lineCount, rawKvClient, keyList, properties, checkSumFile, originalIndexInfoMap, rawKvIndexInfoMap, checkSumFail, notInsert, rawKvTempIndexInfoMap, originalTempIndexInfoMap, totalCheck);
                                    continue;
                                }

                                originalIndexInfoMap.put(key, indexInfoOriginal);
                                keyList.add(ByteString.copyFromUtf8(key));
                                break;
                            case TEMP_INDEX_INFO:
                                tempIndexInfoOriginal = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                                if (!StringUtils.isEmpty(properties.get(ENV_ID))) {
                                    envId = properties.get(ENV_ID);
                                } else {
                                    envId = tempIndexInfoOriginal.getEnvId();
                                }
                                key = String.format(TempIndexInfo.KEY_FORMAT, keyDelimiter, envId, keyDelimiter, tempIndexInfoOriginal.getId());
                                if (DELETE.equals(tempIndexInfoOriginal.getOpType())) {
                                    value = rawKvClient.get(ByteString.copyFromUtf8(key)).toStringUtf8();
                                    if (!value.isEmpty()) {
                                        checkSumLog.error("Check sum delete fail, it still exists, key={}, file={}, line={}", key, checkSumFile.getAbsolutePath(), totalCount);
                                        csFailLog.info(JSON.toJSONString(tempIndexInfoOriginal));
                                        checkSumFail.addAndGet(1);
                                    }
                                    totalCheck.addAndGet(1);
                                    limit = checkSum(limit, limitSize, totalCount, lineCount, rawKvClient, keyList, properties, checkSumFile, originalIndexInfoMap, rawKvIndexInfoMap, checkSumFail, notInsert, rawKvTempIndexInfoMap, originalTempIndexInfoMap, totalCheck);
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
                        if (StringUtils.isEmpty(properties.get(ENV_ID))) {
                            logger.error("Must be set [importer.out.envId] for csv check sum.");
                            System.exit(0);
                        }
                        key = String.format(IndexInfo.KET_FORMAT, keyDelimiter, properties.get(ENV_ID), keyDelimiter, indexInfoOriginal.getType(), keyDelimiter, indexInfoOriginal.getId());
                        originalIndexInfoMap.put(key, indexInfoOriginal);
                        keyList.add(ByteString.copyFromUtf8(key));
                        break;
                    default:
                }
                parseTimer.observeDuration();

                limit = checkSum(limit, limitSize, totalCount, lineCount, rawKvClient, keyList, properties, checkSumFile, originalIndexInfoMap, rawKvIndexInfoMap, checkSumFail, notInsert, rawKvTempIndexInfoMap, originalTempIndexInfoMap, totalCheck);

            }
            try {
                originalLineIterator.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        timer.cancel();
        logger.info("Check sum file={} complete. Total={}, totalCheck={}, notExists={}, skip={}, parseErr={}, checkSumFail={}", checkSumFile.getAbsolutePath(), totalCount, totalCheck, notInsert, skip, parseErr, checkSumFail);

        String moveFilePath = properties.get(CHECK_SUM_MOVE_PATH);
        File moveFile = new File(moveFilePath + "/" + now + "/" + checkSumFile.getName() + "." + fileNum.addAndGet(1));
        try {
            FileUtils.moveFile(checkSumFile, moveFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static int checkSum(
            Integer limit,
            Integer limitSize,
            Integer totalCount,
            Integer lineCount,
            RawKVClient rawKvClient,
            List<ByteString> keyList,
            Map<String, String> properties,
            File checkSumFile,
            Map<String, IndexInfo> originalIndexInfoMap,
            Map<String, IndexInfo> rawKvIndexInfoMap,
            AtomicInteger checkSumFail,
            AtomicInteger notInsert,
            Map<String, TempIndexInfo> rawKvTempIndexInfoMap,
            Map<String, TempIndexInfo> originalTempIndexInfoMap,
            AtomicInteger totalCheck

    ) {
        if (Objects.equals(limit, limitSize) || Objects.equals(totalCount, lineCount)) {

            List<Kvrpcpb.KvPair> kvList;
            try {
                Histogram.Timer batchGetTimer = CHECK_SUM_DURATION.labels("batch_get").startTimer();
                // Batch get keys which to check sum
                kvList = rawKvClient.batchGet(keyList);
                batchGetTimer.observeDuration();
            } catch (Exception e) {
                for (ByteString k : keyList) {
                    if (properties.get(SCENES).equals(INDEX_INFO)) {
                        logger.error("Batch get failed.Key={}, file={}, almost line={}", JSON.toJSONString(originalIndexInfoMap.get(k.toStringUtf8())), checkSumFile.getAbsolutePath(), totalCount);
                        csFailLog.info(JSON.toJSONString(originalIndexInfoMap.get(k.toStringUtf8())));
                    } else if (properties.get(SCENES).equals(TEMP_INDEX_INFO)) {
                        logger.error("Batch get failed.Key={}, file={}, almost line={}", JSON.toJSONString(originalTempIndexInfoMap.get(k.toStringUtf8())), checkSumFile.getAbsolutePath(), totalCount);
                        csFailLog.info(JSON.toJSONString(originalTempIndexInfoMap.get(k.toStringUtf8())));
                    } else {
                        logger.error("Scenes error:" + properties.get(SCENES));
                        System.exit(0);
                    }
                }
                checkSumFail.addAndGet(keyList.size());
                keyList.clear();
                originalIndexInfoMap.clear();
                rawKvIndexInfoMap.clear();
                originalTempIndexInfoMap.clear();
                rawKvTempIndexInfoMap.clear();
                limit = 0;
                return limit;
            }
            Histogram.Timer csTimer = CHECK_SUM_DURATION.labels("check_sum").startTimer();
            switch (properties.get(SCENES)) {
                case INDEX_INFO:
                    // Put all will check sum key to map.
                    IndexInfo indexInfoRawKv;
                    for (Kvrpcpb.KvPair kvPair : kvList) {
                        indexInfoRawKv = JSONObject.parseObject(kvPair.getValue().toStringUtf8(), IndexInfo.class);
                        rawKvIndexInfoMap.put(kvPair.getKey().toStringUtf8(), indexInfoRawKv);
                    }
                    // Check Sum
                    IndexInfo rawKvIndexInfoValue;
                    for (Map.Entry<String, IndexInfo> originalIndexInfoKv : originalIndexInfoMap.entrySet()) {
                        rawKvIndexInfoValue = rawKvIndexInfoMap.get(originalIndexInfoKv.getKey());
                        if (rawKvIndexInfoValue != null) {
                            if (!rawKvIndexInfoValue.equals(originalIndexInfoKv.getValue())) {
                                checkSumLog.error("Check sum failed! Key={}", JSON.toJSONString(originalIndexInfoKv.getKey()));
                                csFailLog.info(JSON.toJSONString(originalIndexInfoKv.getValue()));
                                checkSumFail.addAndGet(1);
                            }
                        } else {
                            checkSumLog.error("Key={} is not exists.", JSON.toJSONString(originalIndexInfoKv.getKey()));
                            csFailLog.info(JSON.toJSONString(originalIndexInfoKv.getValue()));
                            notInsert.addAndGet(1);
                        }
                    }
                    totalCheck.addAndGet(keyList.size());
                    break;
                case TEMP_INDEX_INFO:
                    TempIndexInfo tempIndexInfoRawKv;
                    for (Kvrpcpb.KvPair kvPair : kvList) {
                        tempIndexInfoRawKv = JSONObject.parseObject(kvPair.getValue().toStringUtf8(), TempIndexInfo.class);
                        rawKvTempIndexInfoMap.put(kvPair.getKey().toStringUtf8(), tempIndexInfoRawKv);
                    }
                    TempIndexInfo rawKvTempIndexInfoValue;
                    for (Map.Entry<String, TempIndexInfo> originalKv : originalTempIndexInfoMap.entrySet()) {
                        rawKvTempIndexInfoValue = rawKvTempIndexInfoMap.get(originalKv.getKey());
                        if (rawKvTempIndexInfoValue != null) {
                            if (!rawKvTempIndexInfoValue.equals(originalKv.getValue())) {
                                checkSumLog.error("Check sum failed. Key={}", JSON.toJSONString(originalKv.getValue()));
                                csFailLog.info(JSON.toJSONString(originalKv.getValue()));
                                checkSumFail.addAndGet(1);
                            }
                        } else {
                            checkSumLog.error("Key={} is not exists.", JSON.toJSONString(originalKv.getValue()));
                            csFailLog.info(JSON.toJSONString(originalKv.getValue()));
                            notInsert.addAndGet(1);
                        }
                    }
                    totalCheck.addAndGet(keyList.size());
                    break;
                default:
            }
            kvList.clear();
            keyList.clear();
            originalIndexInfoMap.clear();
            rawKvIndexInfoMap.clear();
            originalTempIndexInfoMap.clear();
            rawKvTempIndexInfoMap.clear();
            csTimer.observeDuration();
            limit = 0;
            return limit;
        }
        return limit;
    }


}
