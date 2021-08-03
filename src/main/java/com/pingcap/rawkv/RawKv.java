package com.pingcap.rawkv;

import com.pingcap.enums.Model;
import com.pingcap.util.FileUtil;
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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RawKv {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger auditLog = LoggerFactory.getLogger(Model.AUDIT_LOG);

    static final Counter REQUEST_COUNTER = Counter.build().name("request_counter").help("Request counter.").labelNames("request_counter").register();
    static final Counter BATCH_PUT_FAIL_COUNTER = Counter.build().name("batch_put_fail_counter").help("Batch put fail counter.").labelNames("batch_put_fail").register();
    static final Histogram REQUEST_LATENCY = Histogram.build().name("requests_latency_seconds").help("Request latency in seconds.").labelNames("request_latency").register();


    public static int batchPut(int totalCount, int todo, int count, int batchSize, RawKVClient rawKvClient, HashMap<ByteString, ByteString> kvPairs, List<String> fileLineList, File file, AtomicInteger totalLineCount, AtomicInteger totalSkipCount, AtomicInteger totalBatchPutFailCount, int totalLine, Map<String, String> properties) {
        if (totalCount == todo || count == batchSize) {

            if (!kvPairs.isEmpty()) {

                // Only json file skip exists key.
                String importMode = properties.get(Model.MODE);
                if (Model.JSON_FORMAT.equals(importMode)) {

                    // For batch get to check exists kv
                    List<ByteString> kvList = new ArrayList<>(kvPairs.keySet());
                    // Just for test
                    if (Model.ON.equals(properties.get(Model.DELETE_FOR_TEST))) {
                        REQUEST_COUNTER.labels("batch delete").inc();
                        Histogram.Timer batchDeleteTimer = REQUEST_LATENCY.labels("batch delete").startTimer();
                        rawKvClient.batchDelete(kvList);
                        batchDeleteTimer.observeDuration();
                    }

                    // Batch get from raw kv.
                    Histogram.Timer batchGetTimer = REQUEST_LATENCY.labels("batch get").startTimer();
                    List<Kvrpcpb.KvPair> kvHaveList = rawKvClient.batchGet(kvList);
                    batchGetTimer.observeDuration();

                    for (Kvrpcpb.KvPair kv : kvHaveList) {
                        kvPairs.remove(kv.getKey());
                        auditLog.warn("Skip exists key[{}], file[{}], almost line[{}]", kv.getKey().toStringUtf8(), file.getAbsolutePath(), totalLine);
                    }
                    totalSkipCount.addAndGet(kvHaveList.size());
                }

                long ttl = Long.parseLong(properties.get(Model.TTL_DAY));
                try {
                    Histogram.Timer batchPutTimer = REQUEST_LATENCY.labels("batch put").startTimer();
                    if (Model.INDEX_INFO.equals(properties.get(Model.SCENES))) {
                        rawKvClient.batchPut(kvPairs);
                        REQUEST_COUNTER.labels("batch put").inc();
                    } else if (Model.TEMP_INDEX_INFO.equals(properties.get(Model.SCENES))) {
                        rawKvClient.batchPut(kvPairs, ttl);
                        REQUEST_COUNTER.labels("batch put").inc();
                    }
                    batchPutTimer.observeDuration();
                    totalLineCount.addAndGet(kvPairs.size());
                } catch (Exception e) {
                    BATCH_PUT_FAIL_COUNTER.labels("batch put fail").inc();
                    // If batch put fails, record the failed batch put data under this path
                    FileChannel batchPutErrFileChannel = RawKv.initBatchPutErrLog(properties, file);
                    for (String kv : fileLineList) {
                        try {
                            // When batch put fails, record the kv pairs that failed batch put
                            batchPutErrFileChannel.write(StandardCharsets.UTF_8.encode(kv + "\n"));
                        } catch (IOException ioException) {
                            ioException.printStackTrace();
                        }
                    }
                    totalBatchPutFailCount.addAndGet(kvPairs.size());
                    logger.error("Failed to batch put, file={}", file.getAbsolutePath(), e);
                } finally {
                    kvPairs.clear();
                    fileLineList.clear();
                    count = 0;
                }
            }
        }
        return count;
    }

    /**
     * Initialize batch put err log. When batch put fails, all failed kv pairs will be recorded
     *
     * @param originalFile: The original file name should be included in the log name
     */
    public static FileChannel initBatchPutErrLog(Map<String, String> properties, File originalFile) {

        // If first batch put fail, create redo folder.
        String redoFolderPath = properties.get(Model.BATCH_PUT_ERR_FILE_PATH);
        FileUtil.createFolder(redoFolderPath);

        // Redo inside folder
        String redoFolderInPath = redoFolderPath.replaceAll("\"", "") + "/" + originalFile.getName().replaceAll("\\.", "");
        FileUtil.createFolder(redoFolderInPath);

        String redoFileName = redoFolderPath.replaceAll("\"", "") + "/" + originalFile.getName().replaceAll("\\.", "") + "/" + Thread.currentThread().getId() + ".txt";
        File redoFile = FileUtil.createFile(redoFileName);

        FileChannel redoFileChannel = null;
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(redoFile);
            redoFileChannel = fileOutputStream.getChannel();
        } catch (FileNotFoundException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        }
        return redoFileChannel;
    }

    /**
     * Get the corresponding value by key
     *
     * @param key: The key to be queried
     */
    public static void get(TiSession tiSession, String key) {
        if (StringUtils.isEmpty(key)) {
            logger.warn("The key cannot be empty.");
        } else {
            RawKVClient rawKvClient = tiSession.createRawClient();
            String value = rawKvClient.get(ByteString.copyFromUtf8(key)).toStringUtf8();
            logger.info("Key={}, Value={}", key, value);
            rawKvClient.close();
        }
    }

    /**
     * Clear all data in raw KV
     */
    public static void truncateRawKv(TiSession tiSession) {
        logger.info("Start to truncate raw kv...");
        RawKVClient rawKvClient = tiSession.createRawClient();
        long startTime = System.currentTimeMillis();
        rawKvClient.deleteRange(ByteString.EMPTY, ByteString.EMPTY);
        rawKvClient.close();
        logger.info("Truncate complete. Duration={}s", (System.currentTimeMillis() - startTime) / 1000);
    }

}
