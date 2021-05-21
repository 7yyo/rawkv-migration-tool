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

public class PutUtil {

    private static final Logger logger = LoggerFactory.getLogger("logBackLog");
    private static final Logger auditLog = LoggerFactory.getLogger("auditLog");

    public static int batchPut(int totalCount, int todo, int count, int batchSize, RawKVClient rawKVClient, ConcurrentHashMap<ByteString, ByteString> kvPairs, File file, AtomicInteger totalLineCount, AtomicInteger totalSkipCount, AtomicInteger totalBatchPutFailCount, int totalLine, Properties properties) {

        if (totalCount == todo || count == batchSize) {

            String mode = properties.getProperty("importer.in.mode");
            String scenes = properties.getProperty("importer.in.scenes");
            Long ttl = Long.valueOf(properties.getProperty("importer.ttl.day"));

            if (Model.JSON_FORMAT.equals(mode)) { // Only json file skip exists key.

                // Just for test
                List<ByteString> list = new ArrayList<>();
                for (Map.Entry<ByteString, ByteString> item : kvPairs.entrySet()) {
                    list.add(item.getKey());
                }
                rawKVClient.batchDelete(list);
                // Just for test

                List<Kvrpcpb.KvPair> haveList = rawKVClient.batchGet(list);
                for (Kvrpcpb.KvPair kv : haveList) {
                    kvPairs.remove(kv.getKey());
                    auditLog.warn(String.format("Skip key - exists: [ %s ], file is [ %s ], almost line= %s", kv.getKey().toStringUtf8(), file.getAbsolutePath(), totalLine));
                    totalSkipCount.addAndGet(haveList.size());
                }
            }

            if (!kvPairs.isEmpty()) {
                try {
                    if (Model.INDEX_INFO.equals(scenes)) {
                        rawKVClient.batchPut(kvPairs);
                    } else if (Model.TEMP_INDEX_INFO.equals(scenes)) {
                        rawKVClient.batchPut(kvPairs, ttl);
                    }
                    totalLineCount.addAndGet(kvPairs.size());
                } catch (Exception e) {
                    logger.error(String.format("Batch put TiKV failed, file=[%s]", file.getAbsolutePath()), e);
                    totalBatchPutFailCount.addAndGet(kvPairs.size());
                }
            }
            kvPairs.clear();
            count = 0;
        }
        return count;
    }

}
