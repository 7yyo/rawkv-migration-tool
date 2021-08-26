package com.pingcap.importer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.enums.Model;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.ServiceTag;
import com.pingcap.pojo.TempIndexInfo;
import com.pingcap.rawkv.RawKv;
import io.prometheus.client.Histogram;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchPutJob extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger auditLog = LoggerFactory.getLogger(Model.AUDIT_LOG);

    private static final long TTL_TIME = 259200000;

    static final Histogram DURATION = Histogram.build().name("duration").help("Everything duration").labelNames("type").register();

    private final String filePath;
    private final TiSession tiSession;
    private final List<String> ttlSkipTypeList;
    private final HashMap<String, Long> ttlSkipTypeMap;
    private final List<String> ttlPutList;
    private final String fileBlock;
    private final AtomicInteger totalImportCount;
    private final AtomicInteger totalSkipCount;
    private final AtomicInteger totalParseErrorCount;
    private final AtomicInteger totalBatchPutFailCount;
    private final AtomicInteger totalDuplicateCount;
    private final Map<String, String> properties;
    private final CountDownLatch countDownLatch;
//    private final int checkSumFilePathNum;

    public BatchPutJob(
            TiSession tiSession,
            AtomicInteger totalImportCount,
            AtomicInteger totalSkipCount,
            AtomicInteger totalParseErrorCount,
            AtomicInteger totalBatchPutFailCount,
            String filePath,
            List<String> ttlSkipTypeList,
            HashMap<String, Long> ttlSkipTypeMap,
            String fileBlock,
            Map<String, String> properties,
            CountDownLatch countDownLatch,
            AtomicInteger totalDuplicateCount,
            List<String> ttlPutList
//            , int checkSumFilePathNum
    ) {

        this.totalImportCount = totalImportCount;
        this.tiSession = tiSession;
        this.totalSkipCount = totalSkipCount;
        this.totalParseErrorCount = totalParseErrorCount;
        this.totalBatchPutFailCount = totalBatchPutFailCount;
        this.filePath = filePath;
        this.ttlPutList = ttlPutList;
        this.ttlSkipTypeList = ttlSkipTypeList;
        this.ttlSkipTypeMap = ttlSkipTypeMap;
        this.fileBlock = fileBlock;
        this.properties = properties;
        this.countDownLatch = countDownLatch;
        this.totalDuplicateCount = totalDuplicateCount;
//        this.checkSumFilePathNum = checkSumFilePathNum;

    }

    @Override
    public void run() {

        String scenes = properties.get(Model.SCENES);
        String importMode = properties.get(Model.MODE);

        String envId = properties.get(Model.ENV_ID);
        String appId = properties.get(Model.APP_ID);

        int batchSize = Integer.parseInt(properties.get(Model.BATCH_SIZE));
//        int checkSumPercentage = Integer.parseInt(properties.get(Model.CHECK_SUM_PERCENTAGE));

        File file = new File(filePath);

        // start = Where does the child thread start processing
        int start = Integer.parseInt(fileBlock.split(",")[0]);
        int todo = Integer.parseInt(fileBlock.split(",")[1]);// todoImportFileLine  = How many rows the child thread has to process

        // If turn off simpleCheckSum & enable check sum, write check sum log.
//        String simpleCheckSum = properties.get(Model.SIMPLE_CHECK_SUM);
//        String enableCheckSum = properties.get(Model.ENABLE_CHECK_SUM);
//        boolean writeCheckSumFile = Model.ON.equals(enableCheckSum) && !Model.ON.equals(simpleCheckSum);
//
//        FileChannel checkSumFileChannel = null;
//        if (writeCheckSumFile) {
//            checkSumFileChannel = CheckSum.initCheckSumLog(properties, file, checkSumFilePathNum);
//        }

        // Raw kv client.
        RawKVClient rawKvClient = tiSession.createRawClient();
        // Batch put kv map.
        HashMap<ByteString, ByteString> kvPairs = new HashMap<>(16);
        // Batch get kv list.
        List<String> kvList = new ArrayList<>();

        try {

            LineIterator lineIterator = FileUtils.lineIterator(file, "UTF-8");
            // If the data file has a large number of rows, the block time may be slightly longer
            Histogram.Timer fileBlockTimer = DURATION.labels("split file").startTimer();
            for (int m = 0; m < start; m++) {
                lineIterator.nextLine();
            }
            fileBlockTimer.observeDuration();

            // Cassandra is from original data file.
            // TiKV is will be put raw kv.
            IndexInfo indexInfoCassandra;
            IndexInfo indexInfoTiKV = new IndexInfo();

            TempIndexInfo tempIndexInfoCassandra;
            TempIndexInfo tempIndexInfoTiKV = new TempIndexInfo();

            // Exists is already in raw kv.
//            IndexInfo existsIndexInfo;
//            TempIndexInfo existsTempIndexInfo;

            // For CSV format, There may be have two delimiter, if CSV has only one delimiter, delimiter2 is invalid.
            String delimiter1 = properties.get(Model.DELIMITER_1);
            String delimiter2 = properties.get(Model.DELIMITER_2);
            // Check sum delimiter, default @#@#@ ======== key@#@#@fileLine.
//            String checkSumDelimiter = properties.get(Model.CHECK_SUM_DELIMITER);
            // For TempIndexInfo, CSV has no updateTime col, so we should add this col, this format is yyyy-MM-dd hh:mm:ss
//            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            // For check sum, judge whether to write the row of data to check sum by the percentage of random number.
//            Random random = new Random();

            // The string type of the key.
            String k;
            // The kv pair to be inserted into the raw kv.
            ByteString key = ByteString.EMPTY, value = ByteString.EMPTY;
//                    , existsValue;
            String line, id, type; // Import file line. Import line col: id, type.
            JSONObject jsonObject; // For import file format is json.

            int cycleCount = 0, totalCount = 0;

            for (int n = 0; n < todo; n++) {

                cycleCount++;
                totalCount++;
//                logger.info(cycleCount + "===" + totalCount);

                try {
                    line = lineIterator.nextLine();
                } catch (NoSuchElementException e) {
                    continue;
                }

                // If import file has blank line, continue.
                if (StringUtils.isBlank(line)) {
                    logger.warn("There is blank lines in the file={}, line={}", file.getAbsolutePath(), start + totalCount);
                    continue;
                }

                Histogram.Timer parseJsonTimer = DURATION.labels("parse json").startTimer();
                Histogram.Timer toObjTimer = DURATION.labels("to obj").startTimer();

                // Json or CSV format
                switch (importMode) {

                    case Model.JSON_FORMAT:
                        try {
                            // Regardless of tempIndexInfo or indexInfo, the conversion of JSONObject is the same logic.
                            jsonObject = JSONObject.parseObject(line);
                            parseJsonTimer.observeDuration();
                        } catch (Exception e) {
                            auditLog.error("Parse failed, file={}, json={}, line={}", file, line, start + totalCount);
                            totalParseErrorCount.addAndGet(1);
                            // If parse failed, we need to judge whether we need to insert in batches at this time
                            cycleCount = RawKv.batchPut(totalCount, todo, cycleCount, batchSize, rawKvClient, kvPairs, kvList, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);
                            continue;
                        }

                        // IndexInfo or TempIndexInfo
                        switch (scenes) {

                            case Model.INDEX_INFO:

                                // Cassandra IndexInfo
                                indexInfoCassandra = JSON.toJavaObject(jsonObject, IndexInfo.class);
                                if (indexInfoCassandra.getUpdateTime() != null) {
                                    indexInfoCassandra.setUpdateTime(indexInfoCassandra.getUpdateTime().replaceAll("T", " ").replaceAll("Z", ""));
                                } else {
                                    indexInfoCassandra.setUpdateTime(null);
                                }
                                toObjTimer.observeDuration();

                                // If the configuration file is configured with envId, we need to overwrite the corresponding value in the json file
                                if (envId != null) {
                                    k = String.format(IndexInfo.KET_FORMAT, envId, indexInfoCassandra.getType(), indexInfoCassandra.getId());
                                } else {
                                    k = String.format(IndexInfo.KET_FORMAT, indexInfoCassandra.getEnvId(), indexInfoCassandra.getType(), indexInfoCassandra.getId());
                                }

                                // TiKV indexInfo
                                IndexInfo.initValueIndexInfoTiKV(indexInfoTiKV, indexInfoCassandra);

                                key = ByteString.copyFromUtf8(k);
                                value = ByteString.copyFromUtf8(JSONObject.toJSONString(indexInfoTiKV));

                                // If importer.ttl.put.type exists, put with ttl, then continue.
                                if (ttlPutList.contains(indexInfoCassandra.getType())) {
                                    rawKvClient.put(key, value, TTL_TIME);
                                    continue;
                                }

                                // If it exists in the ttl type map, skip.
                                if (ttlSkipTypeList.contains(indexInfoCassandra.getType())) {
                                    ttlSkipTypeMap.put(indexInfoCassandra.getType(), ttlSkipTypeMap.get(indexInfoCassandra.getType()) + 1);
                                    auditLog.warn("Skip key={}, file={}, line={}", k, file.getAbsolutePath(), start + totalCount);
                                    totalSkipCount.addAndGet(1);
                                    cycleCount = RawKv.batchPut(totalCount, todo, cycleCount, batchSize, rawKvClient, kvPairs, kvList, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);
                                    continue;
                                }

                                logger.debug("File={}, key={}, value={}", file.getAbsolutePath(), key.toStringUtf8(), JSONObject.toJSONString(indexInfoTiKV));

                                break;

                            case Model.TEMP_INDEX_INFO:

                                tempIndexInfoCassandra = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                                // TempIndexInfo has no timestamp, so we should add current timestamp for data.
//                                tempIndexInfoCassandra.setUpdateTime(simpleDateFormat.format(new Date()));
                                toObjTimer.observeDuration();

                                if (envId != null) {
                                    k = String.format(TempIndexInfo.KEY_FORMAT, envId, tempIndexInfoCassandra.getId());
                                } else {
                                    k = String.format(TempIndexInfo.KEY_FORMAT, tempIndexInfoCassandra.getEnvId(), tempIndexInfoCassandra.getId());
                                }

                                // Compare this kv tso with kvPairs
//                                existsValue = kvPairs.get(ByteString.copyFromUtf8(k));
//                                if (existsValue != null && !existsValue.isEmpty()) {
//                                    JSONObject existsJSONObject = JSONObject.parseObject(existsValue.toStringUtf8());
//                                    existsTempIndexInfo = JSON.toJavaObject(existsJSONObject, TempIndexInfo.class);
//                                    // exist time > update time
//                                    if (CountUtil.compareTime(existsTempIndexInfo.getUpdateTime(), tempIndexInfoCassandra.getUpdateTime()) < 0) {
//                                        auditLog.warn("Skip key[{}] in kvPairs, kvPairs tso[{}], curr tso[{}], file[{}], line[{}]", k, existsTempIndexInfo.getUpdateTime(), tempIndexInfoCassandra.getUpdateTime(), file.getAbsolutePath(), start + totalCount);
//                                        totalSkipCount.addAndGet(1);
//                                        cycleCount = RawKv.batchPut(totalCount, todo, cycleCount, batchSize, rawKvClient, kvPairs, kvList, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);
//                                        continue;
//                                    } else if ((CountUtil.compareTime(existsTempIndexInfo.getUpdateTime(), tempIndexInfoCassandra.getUpdateTime()) > 0)) {
//                                        totalSkipCount.addAndGet(1);
//                                        auditLog.warn("Cover key[{}] in kvPairs, kvPairs tso[{}], curr tso[{}], file[{}], line[{}]", k, existsTempIndexInfo.getUpdateTime(), tempIndexInfoCassandra.getUpdateTime(), file.getAbsolutePath(), start + totalCount);
//                                    }
//                                }

                                // TiKV tempIndexInfo
                                TempIndexInfo.initValueTempIndexInfo(tempIndexInfoTiKV, tempIndexInfoCassandra);

                                key = ByteString.copyFromUtf8(k);
                                value = ByteString.copyFromUtf8(JSONObject.toJSONString(tempIndexInfoTiKV));

                                logger.debug("File={}, key={}, value={}", file.getAbsolutePath(), k, JSONObject.toJSONString(tempIndexInfoTiKV));

                                break;

                            default:
                                logger.error("Illegal format={}", Model.MODE);

                        }
                        break;

                    case Model.CSV_FORMAT:

                        try {

                            id = line.split(delimiter1)[0];
                            type = line.split(delimiter1)[1];
                            indexInfoCassandra = new IndexInfo();

                            if (envId != null) {
                                k = String.format(IndexInfo.KET_FORMAT, envId, type, id);
                            } else {
                                k = String.format(IndexInfo.KET_FORMAT, indexInfoCassandra.getEnvId(), type, id);
                            }

                            // CSV has no timestamp, so don't consider.
                            String targetId = line.split(delimiter1)[2].split(delimiter2)[0];
                            indexInfoTiKV.setTargetId(targetId);
                            indexInfoTiKV.setAppId(appId);
                            String v = line.split(delimiter1)[2];
                            ServiceTag serviceTag = new ServiceTag();
                            serviceTag.setBLKMDL_ID(v.split(delimiter2)[0]);
                            serviceTag.setPD_SALE_FTA_CD(v.split(delimiter2)[1]);
                            serviceTag.setACCT_DTL_TYPE(v.split(delimiter2)[2]);
                            serviceTag.setCORPPRVT_FLAG(v.split(delimiter2)[3]);
                            serviceTag.setCMTRST_CST_ACCNO(v.split(delimiter2)[4]);
                            serviceTag.setAR_ID(v.split(delimiter2)[5]);
                            serviceTag.setQCRCRD_IND(v.split(delimiter2)[6]);
                            indexInfoTiKV.setServiceTag(JSON.toJSONString(serviceTag));

                            key = ByteString.copyFromUtf8(k);
                            value = ByteString.copyFromUtf8(JSONObject.toJSONString(indexInfoTiKV));

                            if (ttlPutList.contains(indexInfoCassandra.getType())) {
                                rawKvClient.put(key, value, TTL_TIME);
                                continue;
                            }

                            // Skip the type that exists in the tty type map.
                            if (ttlSkipTypeList.contains(type)) {
                                ttlSkipTypeMap.put(type, ttlSkipTypeMap.get(type) + 1);
                                auditLog.warn("[Skip key={} in file={} ,line={}", k, file.getAbsolutePath(), start + totalCount);
                                totalSkipCount.addAndGet(1);
                                cycleCount = RawKv.batchPut(totalCount, todo, cycleCount, batchSize, rawKvClient, kvPairs, kvList, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);
                                continue;
                            }

                        } catch (Exception e) {
                            logger.error("Failed to parse json, file[{}], json[{}], line[{}]", file, line, start + totalCount);
                            totalParseErrorCount.addAndGet(1);
                            // if _todo_ == totalCount in json failed, batch put.
                            cycleCount = RawKv.batchPut(totalCount, todo, cycleCount, batchSize, rawKvClient, kvPairs, kvList, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);
                            continue;
                        }

                        break;

                    default:
                        logger.error("Illegal format={}", importMode);
                        return;
                }

                // Sampling data is written into the check sum file
//                if (writeCheckSumFile) {
//                    int ratio = random.nextInt(100 / checkSumPercentage) + 1;
//                    if (ratio == 1) {
//                        Histogram.Timer writeCheckSumTimer = DURATION.labels("check sum").startTimer();
//                        if (checkSumFileChannel != null)
//                            checkSumFileChannel.write(StandardCharsets.UTF_8.encode(k + checkSumDelimiter + (start + totalCount) + "\n"));
//                        writeCheckSumTimer.observeDuration();
//                    }
//                }

                ByteString du = kvPairs.put(key, value);
                if (du != null) {
                    totalDuplicateCount.addAndGet(1);
                }
                kvList.add(line);
                cycleCount = RawKv.batchPut(totalCount, todo, cycleCount, batchSize, rawKvClient, kvPairs, kvList, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);

            }

            try {
//                if (writeCheckSumFile) {
//                    if (checkSumFileChannel != null) checkSumFileChannel.close();
//                }
                lineIterator.close();
                rawKvClient.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            countDownLatch.countDown();


        } catch (IOException e) {
            e.printStackTrace();
        }


    }


}
