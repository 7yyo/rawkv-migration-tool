package com.pingcap.util;

import com.pingcap.enums.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.File;
import java.util.*;
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

                if (Model.ON.equals(properties.getProperty(Model.SKIP_EXISTS_KEY))) {
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

}
