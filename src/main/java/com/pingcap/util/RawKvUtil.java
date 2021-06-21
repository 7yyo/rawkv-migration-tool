package com.pingcap.util;

import com.pingcap.enums.Model;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.common.key.Key;
import org.tikv.common.region.TiRegion;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author yuyang
 */
public class RawKvUtil {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger auditLog = LoggerFactory.getLogger(Model.AUDIT_LOG);

    static final Counter REQUEST_COUNTER = Counter.build().name("request_counter").help("Request counter.").labelNames("request_counter").register();
    static final Counter BATCH_PUT_FAIL_COUNTER = Counter.build().name("batch_put_fail_counter").help("Batch put fail counter.").labelNames("batch_put_fail").register();
    static final Histogram REQUEST_LATENCY = Histogram.build().name("requests_latency_seconds").help("Request latency in seconds.").labelNames("request_latency").register();

    public static int batchPut(int totalCount, int todo, int count, int batchSize, RawKVClient rawKvClient, HashMap<ByteString, ByteString> kvPairs, List<String> kvList, File file, AtomicInteger totalLineCount, AtomicInteger totalSkipCount, AtomicInteger totalBatchPutFailCount, int totalLine, Properties properties, FileChannel fileChannel) {

        // batch put
        if (totalCount == todo || count == batchSize) {

            String importMode = properties.getProperty(Model.MODE);
            long ttl = Long.parseLong(properties.getProperty(Model.TTL_DAY));

            // Only json file skip exists key.
            if (Model.JSON_FORMAT.equals(importMode)) {

                if (Model.ON.equals(properties.getProperty(Model.CHECK_EXISTS_KEY))) {
                    List<ByteString> list = new ArrayList<>(kvPairs.keySet());
                    if (Model.ON.equals(properties.getProperty(Model.DELETE_FOR_TEST))) {
                        REQUEST_COUNTER.labels("batch delete").inc();
                        Histogram.Timer batchDeleteTimer = REQUEST_LATENCY.labels("batch delete").startTimer();
                        rawKvClient.batchDelete(list);
                        batchDeleteTimer.observeDuration();
                    }
                    Histogram.Timer batchGetTimer = REQUEST_LATENCY.labels("batch get").startTimer();
                    List<Kvrpcpb.KvPair> haveList = rawKvClient.batchGet(list);
                    batchGetTimer.observeDuration();
                    REQUEST_COUNTER.labels("batch get").inc();
                    for (Kvrpcpb.KvPair kv : haveList) {
                        Histogram.Timer deleteTimer = REQUEST_LATENCY.labels("delete").startTimer();
                        kvPairs.remove(kv.getKey());
                        deleteTimer.observeDuration();
                        auditLog.warn(String.format("Skip key - exists: [ %s ], file is [ %s ], almost line= %s", kv.getKey().toStringUtf8(), file.getAbsolutePath(), totalLine));
                    }
                    totalSkipCount.addAndGet(haveList.size());
                }

            }

            if (!kvPairs.isEmpty()) {
                try {
                    Histogram.Timer batchPutTimer = REQUEST_LATENCY.labels("batch put").startTimer();
                    if (Model.INDEX_INFO.equals(properties.getProperty(Model.SCENES))) {
                        rawKvClient.batchPut(kvPairs);
                        REQUEST_COUNTER.labels("batch put").inc();
                    } else if (Model.TEMP_INDEX_INFO.equals(properties.getProperty(Model.SCENES))) {
                        rawKvClient.batchPut(kvPairs, ttl);
                        REQUEST_COUNTER.labels("batch put").inc();
                    }
                    batchPutTimer.observeDuration();
                    totalLineCount.addAndGet(kvPairs.size());
                } catch (Exception e) {
                    BATCH_PUT_FAIL_COUNTER.labels("batch put fail").inc();
                    for (String kv : kvList) {
                        try {
                            fileChannel.write(StandardCharsets.UTF_8.encode(kv + "\n"));
                        } catch (IOException ioException) {
                            ioException.printStackTrace();
                        }
                    }
                    totalBatchPutFailCount.addAndGet(kvPairs.size());
                    logger.error(String.format("Failed to batch put in file[%s]", file.getAbsolutePath()), e);
                } finally {
                    kvPairs.clear();
                    kvList.clear();
                    count = 0;
                }
            }
        }
        return count;
    }

    public static String get(RawKVClient rawKVClient, String key) {
        ByteString value = rawKVClient.get(ByteString.copyFromUtf8(key));
        return value.toStringUtf8();
    }

    public static void batchGetCheck(String filePath, TiSession tiSession, Properties properties) {
        RawKVClient rawKVClient = tiSession.createRawClient();
        File file = new File(filePath);
        List<ByteString> keyList = new ArrayList<>();
        LineIterator lineIterator = null;
        try {
            lineIterator = FileUtils.lineIterator(file, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        lineIterator.nextLine();
        logger.info(String.format("Start checking file [%s]", filePath));
        while (lineIterator.hasNext()) {
            String line = lineIterator.nextLine().split(properties.getProperty("importer.checkSum.checkSumDelimiter"))[0];
            ByteString key = ByteString.copyFromUtf8(line);
            keyList.add(key);
        }
        List<Kvrpcpb.KvPair> kvPairs = rawKVClient.batchGet(keyList);
        String newFileName = file.getAbsolutePath() + "_check_sum.txt";
        File newFile = new File(newFileName);
        try {
            newFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info(String.format("The data content in tikv has been queried according to the path file, and it is being written to the same level directory of [%s].", filePath));
        for (Kvrpcpb.KvPair kv : kvPairs) {
            String newLine = String.format("key=%s, value=%s\n", kv.getKey().toStringUtf8(), kv.getValue().toStringUtf8());
            try {
                FileUtils.write(newFile, newLine, true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Create a log of batch put failed data records.
     */
    public static FileChannel initBatchPutErrLog(Properties properties, FileChannel fileChannel, File originalFile) {

        String batchPutErrFilePath = properties.getProperty(Model.BATCH_PUT_ERR_FILE_PATH);
        String batchPutErrFileName = batchPutErrFilePath.replaceAll("\"", "") + "/" + originalFile.getName().replaceAll("\\.", "") + "/" + Thread.currentThread().getId() + ".txt";

        File batchPutErrFile = new File(batchPutErrFileName);
        try {
            batchPutErrFile.getParentFile().mkdirs();
            batchPutErrFile.createNewFile();
            FileOutputStream fileOutputStream = new FileOutputStream(new File(batchPutErrFileName));
            fileChannel = fileOutputStream.getChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return fileChannel;
    }

    /**
     * Get all regions in the current raw kv.
     */
    public static List<TiRegion> getTiRegionList(TiSession tiSession) {
        List<TiRegion> regionList = new ArrayList<>();
        ByteString key = ByteString.EMPTY;
        boolean isStart = true;
        TiRegion tiRegion;
        while (isStart || !key.isEmpty()) {
            isStart = false;
            tiRegion = tiSession.getRegionManager().getRegionByKey(key);
            regionList.add(tiRegion);
            key = Key.toRawKey(tiRegion.getEndKey()).toByteString();
        }
        return regionList;
    }

}
