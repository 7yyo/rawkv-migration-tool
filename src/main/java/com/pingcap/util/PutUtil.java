package com.pingcap.util;

import org.apache.log4j.Logger;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PutUtil {

    private static final Logger logger = Logger.getLogger(PutUtil.class);

    public static int batchPut(int totalCount, int todo, int count, int batchSize, RawKVClient rawKVClient, ConcurrentHashMap<ByteString, ByteString> kvPairs, File file, AtomicInteger totalLineCount, AtomicInteger totalSkipCount, AtomicInteger totalBatchPutFailCount, int totalLine) {

        if (totalCount == todo || count == batchSize) {
            try {
                // TODO
                List<ByteString> list = new ArrayList<>();
                for (Map.Entry<ByteString, ByteString> item : kvPairs.entrySet()) {
                    list.add(item.getKey());
                }
                rawKVClient.batchDelete(list);

                List<Kvrpcpb.KvPair> haveList = rawKVClient.batchGet(list);
                for (Kvrpcpb.KvPair kv : haveList) {
                    kvPairs.remove(kv.getKey());
                    logger.warn(String.format("Skip key - exists: [ %s ], file is [ %s ], almost line= %s", kv.getKey().toStringUtf8(), file.getAbsolutePath(), totalLine));
                }

                totalSkipCount.addAndGet(haveList.size());

                if (!kvPairs.isEmpty()) {
                    rawKVClient.batchPut(kvPairs);
                    totalLineCount.addAndGet(kvPairs.size());
                }
                kvPairs.clear();
                count = 0;
            } catch (Exception e) {
                logger.error(String.format("Batch put TiKV failed, file=[%s]", file.getAbsolutePath()), e);
                totalLineCount.addAndGet(-(kvPairs.size()));
                totalBatchPutFailCount.addAndGet(kvPairs.size());
                logger.info(kvPairs.size());
            }
        }
        return count;
    }

}
