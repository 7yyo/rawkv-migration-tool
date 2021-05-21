package com.pingcap.util;

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

    public static int batchPut(int totalCount, int todo, int count, int batchSize, RawKVClient rawKVClient, ConcurrentHashMap<ByteString, ByteString> kvPairs, File file, AtomicInteger totalLineCount, AtomicInteger totalSkipCount, AtomicInteger totalBatchPutFailCount, int totalLine, String mode) {

        if (totalCount == todo || count == batchSize) {
            // TODO
            List<Kvrpcpb.KvPair> haveList = null;
            if ("json".equals(mode)) { // Only json file skip exists key.
                List<ByteString> list = new ArrayList<>();
                for (Map.Entry<ByteString, ByteString> item : kvPairs.entrySet()) {
                    list.add(item.getKey());
                }
                rawKVClient.batchDelete(list);

                haveList = rawKVClient.batchGet(list);
                for (Kvrpcpb.KvPair kv : haveList) {
                    kvPairs.remove(kv.getKey());
                    auditLog.warn(String.format("Skip key - exists: [ %s ], file is [ %s ], almost line= %s", kv.getKey().toStringUtf8(), file.getAbsolutePath(), totalLine));
                    totalSkipCount.addAndGet(haveList.size());
                }
            }

            if (!kvPairs.isEmpty()) {
                try {
                    rawKVClient.batchPut(kvPairs);
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
