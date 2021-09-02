package com.pingcap.importer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.enums.Model;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.ServiceTag;
import com.pingcap.pojo.TempIndexInfo;
import com.pingcap.rawkv.RawKv;
import com.pingcap.util.PropertiesUtil;
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

    private static final long TTL_TIME = 604800000;

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
            List<String> ttlPutList) {
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

    }

    @Override
    public void run() {

        PropertiesUtil.checkConfig(properties, Model.SCENES);
        String scenes = properties.get(Model.SCENES);
        PropertiesUtil.checkConfig(properties, Model.MODE);
        String importMode = properties.get(Model.MODE);

        PropertiesUtil.checkConfig(properties, Model.ENV_ID);
        String envId = properties.get(Model.ENV_ID);
        PropertiesUtil.checkConfig(properties, Model.APP_ID);
        String appId = properties.get(Model.APP_ID);

        PropertiesUtil.checkConfig(properties, Model.BATCH_SIZE);
        int batchSize = Integer.parseInt(properties.get(Model.BATCH_SIZE));

        File file = new File(filePath);

        // start_ = Where does the child thread start processing
        int start = Integer.parseInt(fileBlock.split(",")[0]);
        // todo_ ImportFileLine  = How many rows the child thread has to process
        int todo = Integer.parseInt(fileBlock.split(",")[1]);


        RawKVClient rawKvClient = tiSession.createRawClient();

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

            // Cassandra is from original data file, TiKV is will be put raw kv.
            IndexInfo indexInfoCassandra;
            IndexInfo indexInfoTiKV = new IndexInfo();

            TempIndexInfo tempIndexInfoCassandra;
            TempIndexInfo tempIndexInfoTiKV = new TempIndexInfo();

            // For CSV format, There may be have two delimiter, if CSV has only one delimiter, delimiter2 is invalid.
            String delimiter1 = properties.get(Model.DELIMITER_1);
            String delimiter2 = properties.get(Model.DELIMITER_2);
            // The string type of the key.
            String k;
            // The kv pair to be inserted into the raw kv.
            ByteString key = ByteString.EMPTY, value = ByteString.EMPTY;
            String line, id, type; // Import file line. Import line col: id, type.
            JSONObject jsonObject; // For import file format is json.

            int cycleCount = 0, totalCount = 0;

            for (int n = 0; n < todo; n++) {

                cycleCount++;
                totalCount++;

                try {
                    line = lineIterator.nextLine();
                } catch (NoSuchElementException e) {
                    logger.error("LineIterator error, file = {}", file.getAbsolutePath(), e);
                    totalParseErrorCount.addAndGet(1);
                    break;
                }

                // If import file has blank line, continue, recode skip + 1.
                if (StringUtils.isBlank(line) || "".equals(line.trim())) {
                    logger.warn("There is blank lines in the file={}, line={}", file.getAbsolutePath(), start + totalCount);
                    totalSkipCount.addAndGet(1);
                    continue;
                }

                Histogram.Timer parseJsonTimer = DURATION.labels("parse json").startTimer();
                Histogram.Timer toObjTimer = DURATION.labels("to obj").startTimer();

                // Json or CSV format
                switch (importMode) {

                    case Model.JSON_FORMAT:
                        try {
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
                                    totalImportCount.addAndGet(1);
                                    cycleCount = RawKv.batchPut(totalCount, todo, cycleCount, batchSize, rawKvClient, kvPairs, kvList, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);
                                    continue;
                                }

                                // If it exists in the ttl type map, skip.
                                if (ttlSkipTypeList.contains(indexInfoCassandra.getType())) {
                                    ttlSkipTypeMap.put(indexInfoCassandra.getType(), ttlSkipTypeMap.get(indexInfoCassandra.getType()) + 1);
                                    auditLog.info("Skip key={}, file={}, line={}", k, file.getAbsolutePath(), start + totalCount);
                                    totalSkipCount.addAndGet(1);
                                    cycleCount = RawKv.batchPut(totalCount, todo, cycleCount, batchSize, rawKvClient, kvPairs, kvList, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);
                                    continue;
                                }

                                logger.debug("File={}, key={}, value={}", file.getAbsolutePath(), key.toStringUtf8(), JSONObject.toJSONString(indexInfoTiKV));

                                break;

                            case Model.TEMP_INDEX_INFO:

                                tempIndexInfoCassandra = JSON.toJavaObject(jsonObject, TempIndexInfo.class);

                                if (envId != null) {
                                    k = String.format(TempIndexInfo.KEY_FORMAT, envId, tempIndexInfoCassandra.getId());
                                } else {
                                    k = String.format(TempIndexInfo.KEY_FORMAT, tempIndexInfoCassandra.getEnvId(), tempIndexInfoCassandra.getId());
                                }

                                // TiKV tempIndexInfo
                                TempIndexInfo.initValueTempIndexInfo(tempIndexInfoTiKV, tempIndexInfoCassandra);

                                key = ByteString.copyFromUtf8(k);
                                value = ByteString.copyFromUtf8(JSONObject.toJSONString(tempIndexInfoTiKV));

                                logger.debug("File={}, key={}, value={}", file.getAbsolutePath(), k, JSONObject.toJSONString(tempIndexInfoTiKV));

                                break;

                            default:
                                logger.error("Illegal format={}", Model.MODE);
                                System.exit(0);

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
                            logger.error("Failed to parse csv, file[{}], json[{}], line[{}]", file, line, start + totalCount);
                            totalParseErrorCount.addAndGet(1);
                            // if _todo_ == totalCount in json failed, batch put.
                            cycleCount = RawKv.batchPut(totalCount, todo, cycleCount, batchSize, rawKvClient, kvPairs, kvList, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);
                            continue;
                        }

                        break;

                    default:
                        logger.error("Illegal format={}", importMode);
                        System.exit(0);
                }

                ByteString du = kvPairs.put(key, value);
                if (du != null) {
                    totalDuplicateCount.addAndGet(1);
                }
                kvList.add(line);
                cycleCount = RawKv.batchPut(totalCount, todo, cycleCount, batchSize, rawKvClient, kvPairs, kvList, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);

            }

            try {
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
