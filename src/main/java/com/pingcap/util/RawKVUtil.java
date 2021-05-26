package com.pingcap.util;

import com.pingcap.enums.Model;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


public class RawKVUtil {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger auditLog = LoggerFactory.getLogger(Model.AUDIT_LOG);

    public static int batchPut(int totalCount, int todo, int count, int batchSize, RawKVClient rawKVClient, ConcurrentHashMap<ByteString, ByteString> kvPairs, File file, AtomicInteger totalLineCount, AtomicInteger totalSkipCount, AtomicInteger totalBatchPutFailCount, int totalLine, Properties properties) {

        if (totalCount == todo || count == batchSize) {

            String importMode = properties.getProperty(Model.MODE);
            long ttl = Long.parseLong(properties.getProperty(Model.TTL_DAY));

            if (Model.JSON_FORMAT.equals(importMode)) { // Only json file skip exists key.

                if (Model.ON.equals(properties.getProperty(Model.CHECK_EXISTS_KEY))) {
                    List<ByteString> list = new ArrayList<>();
                    for (Map.Entry<ByteString, ByteString> item : kvPairs.entrySet()) {
                        list.add(item.getKey());
                    }
                    if (Model.ON.equals(properties.getProperty(Model.DELETE_FOR_TEST))) {
                        rawKVClient.batchDelete(list);
                    }
                    List<Kvrpcpb.KvPair> haveList = rawKVClient.batchGet(list);
                    for (Kvrpcpb.KvPair kv : haveList) {
                        kvPairs.remove(kv.getKey());
                        auditLog.warn(String.format("Skip key - exists: [ %s ], file is [ %s ], almost line= %s", kv.getKey().toStringUtf8(), file.getAbsolutePath(), totalLine));
                        totalSkipCount.addAndGet(haveList.size());
                    }
                }

            }

            if (!kvPairs.isEmpty()) {
                try {
                    if (Model.JSON_FORMAT.equals(importMode)) {
                        rawKVClient.batchPut(kvPairs);
                    } else if (Model.CSV_FORMAT.equals(importMode)) {
                        rawKVClient.batchPut(kvPairs, ttl);
                    }
                    totalLineCount.addAndGet(kvPairs.size());
                } catch (Exception e) {
                    logger.error(String.format("Batch put TiKV failed, file=[%s]", file.getAbsolutePath()));
                    totalBatchPutFailCount.addAndGet(kvPairs.size());
                }
            }
            kvPairs.clear();
            count = 0;
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

}
