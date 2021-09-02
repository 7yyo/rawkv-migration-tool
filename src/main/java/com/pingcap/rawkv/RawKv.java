package com.pingcap.rawkv;

import com.pingcap.enums.Model;
import com.pingcap.util.PropertiesUtil;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RawKv {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger auditLog = LoggerFactory.getLogger(Model.AUDIT_LOG);
    private static final Logger bpFailLog = LoggerFactory.getLogger(Model.BP_FAIL_LOG);

    static final Counter BATCH_PUT_FAIL_COUNTER = Counter.build().name("batch_put_fail_counter").help("Batch put fail counter.").labelNames("batch_put_fail").register();
    static final Histogram REQUEST_LATENCY = Histogram.build().name("requests_latency_seconds").help("Request latency in seconds.").labelNames("request_latency").register();


    public static int batchPut(int totalCount, int todo, int count, int batchSize, RawKVClient rawKvClient, HashMap<ByteString, ByteString> kvPairs, List<String> fileLineList, File file, AtomicInteger totalLineCount, AtomicInteger totalSkipCount, AtomicInteger totalBatchPutFailCount, int totalLine, Map<String, String> properties) {
        if (totalCount == todo || count == batchSize) {

            if (!kvPairs.isEmpty()) {

                PropertiesUtil.checkConfig(properties, Model.CHECK_EXISTS_KEY);
                if (Model.ON.equals(properties.get(Model.CHECK_EXISTS_KEY))) {
                    // Only json file skip exists key.
                    String importMode = properties.get(Model.MODE);
                    if (Model.JSON_FORMAT.equals(importMode)) {

                        // For batch get to check exists kv
                        List<ByteString> kvList = new ArrayList<>(kvPairs.keySet());

                        Histogram.Timer batchGetTimer = REQUEST_LATENCY.labels("batch get").startTimer();
                        List<Kvrpcpb.KvPair> kvHaveList;
                        try {
                            // Batch get from raw kv.
                            kvHaveList = rawKvClient.batchGet(kvList);
                        } catch (Exception e) {
                            BATCH_PUT_FAIL_COUNTER.labels("batch put fail").inc();
                            for (String s : fileLineList) {
                                bpFailLog.info(s);
                            }
                            // If batch put fails, record the failed batch put data under this path
//                        FileChannel batchPutErrFileChannel;
//                        RawKv.initBatchPutErrLog(properties, file, fileLineList);
                            totalBatchPutFailCount.addAndGet(kvPairs.size());
                            logger.error("Failed to get put, file={}", file.getAbsolutePath(), e);
                            count = 0;
                            kvPairs.clear();
                            fileLineList.clear();
                            return count;
                        }

                        batchGetTimer.observeDuration();
                        for (Kvrpcpb.KvPair kv : kvHaveList) {
                            kvPairs.remove(kv.getKey());
                            auditLog.info("Skip exists key={}, file={}, almost line={}", kv.getKey().toStringUtf8(), file.getAbsolutePath(), totalLine);
                        }
                        totalSkipCount.addAndGet(kvHaveList.size());

                    }
                }

                PropertiesUtil.checkConfig(properties, Model.TTL);
                try {
                    Histogram.Timer batchPutTimer = REQUEST_LATENCY.labels("batch put").startTimer();
                    if (Model.INDEX_INFO.equals(properties.get(Model.SCENES))) {
                        rawKvClient.batchPut(kvPairs);
                    } else if (Model.TEMP_INDEX_INFO.equals(properties.get(Model.SCENES))) {
                        rawKvClient.batchPut(kvPairs, Long.parseLong(properties.get(Model.TTL)));
                    }
                    batchPutTimer.observeDuration();
                    totalLineCount.addAndGet(kvPairs.size());
                } catch (Exception e) {
                    BATCH_PUT_FAIL_COUNTER.labels("batch put fail").inc();
                    // If batch put fails, record the failed batch put data under this path
                    for (String s : fileLineList) {
                        bpFailLog.info(s);
                    }
                    totalBatchPutFailCount.addAndGet(kvPairs.size());
                    logger.error("Failed to batch put, file={}", file.getAbsolutePath(), e);
//                    FileChannel batchPutErrFileChannel;
//                    RawKv.initBatchPutErrLog(properties, file, fileLineList);
//                    totalBatchPutFailCount.addAndGet(kvPairs.size());
//                    logger.error("Failed to batch put, file={}", file.getAbsolutePath(), e);
                } finally {
                    kvPairs.clear();
                    fileLineList.clear();
                    count = 0;
                }
            }
        }
        return count;
    }

//    /**
//     * Initialize batch put err log. When batch put fails, all failed kv pairs will be recorded
//     *
//     * @param originalFile: The original file name should be included in the log name
//     */
//    public static void initBatchPutErrLog(Map<String, String> properties, File originalFile, List<String> fileLineList) {
//
//        FileChannel redoFileChannel;
//
//        // If first batch put fail, create redo folder.
//        String redoFolderPath = properties.get(Model.BATCH_PUT_ERR_FILE_PATH);
//        FileUtil.createFolder(redoFolderPath);
//
//        // Redo inside folder
//        String redoFolderInPath = redoFolderPath.replaceAll("\"", "") + "/" + originalFile.getName().replaceAll("\\.", "");
//        FileUtil.createFolder(redoFolderInPath);
//
//        String redoFileName = redoFolderPath.replaceAll("\"", "") + "/" + originalFile.getName().replaceAll("\\.", "") + "/" + Thread.currentThread().getId() + ".txt";
//        File redoFile = FileUtil.createFile(redoFileName);
//
//        FileOutputStream fileOutputStream;
//        try {
//            fileOutputStream = new FileOutputStream(redoFile, true);
//            redoFileChannel = fileOutputStream.getChannel();
//            for (String kv : fileLineList) {
//                try {
//                    // When batch put fails, record the kv pairs that failed batch put
//                    redoFileChannel.write(StandardCharsets.UTF_8.encode(kv + "\n"));
//                } catch (IOException ioException) {
//                    ioException.printStackTrace();
//                }
//            }
//            redoFileChannel.close();
//            fileOutputStream.close();
//        } catch (IOException fileNotFoundException) {
//            fileNotFoundException.printStackTrace();
//        }
//
//
//    }

    public static void get(TiSession tiSession, String key) {
        if (StringUtils.isEmpty(key)) {
            logger.warn("The key cannot be null.");
        } else {
            RawKVClient rawKvClient = tiSession.createRawClient();
            String value = rawKvClient.get(ByteString.copyFromUtf8(key)).toStringUtf8();
            logger.info("Key={}, Value={}", key, value);
            rawKvClient.close();
        }
    }

    public static void truncateRawKv(TiSession tiSession) {
        logger.info("Start to truncate raw kv...");
        RawKVClient rawKvClient = tiSession.createRawClient();
        long startTime = System.currentTimeMillis();
        rawKvClient.deleteRange(ByteString.EMPTY, ByteString.EMPTY);
        rawKvClient.close();
        logger.info("Truncate complete. Duration={}s", (System.currentTimeMillis() - startTime) / 1000);
    }

    public static void deleteByKey(TiSession tiSession, String key) {
        if (StringUtils.isEmpty(key)) {
            logger.warn("The key cannot by null.");
        } else {
            try (RawKVClient rawKvClient = tiSession.createRawClient()) {
                rawKvClient.delete(ByteString.copyFromUtf8(key));
                logger.info("Delete key={} success.", key);
            } catch (Exception e) {
                logger.error("Delete key={} fail.", key);
            }
        }
    }

    public static void deleteByPrefix(TiSession tiSession, String key) {
        if (StringUtils.isEmpty(key)) {
            logger.warn("The key prefix cannot by null.");
        } else {
            try (RawKVClient rawKvClient = tiSession.createRawClient()) {
                rawKvClient.deletePrefix(ByteString.copyFromUtf8(key));
                logger.info("Delete key by prefix={} success.", key);
            } catch (Exception e) {
                logger.error("Delete key by prefix={} fail.", key);
            }
        }
    }

}
