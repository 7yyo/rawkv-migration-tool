package com.pingcap.util;

import org.apache.log4j.Logger;
import org.tikv.raw.RawKVClient;
import shade.com.google.protobuf.ByteString;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PutUtil {

    private static final Logger logger = Logger.getLogger(PutUtil.class);

    public static void batchPut(int totalCount, int todo, int count, int batchSize, RawKVClient rawKVClient, HashMap<ByteString, ByteString> kvPairs, File file, AtomicInteger totalLineCount, AtomicInteger totalSkipCount) {
        if (totalCount == todo || count == batchSize) {
            // TODO
            for (Map.Entry<ByteString, ByteString> item : kvPairs.entrySet()) {
                rawKVClient.delete(item.getKey());
            }
            String k;
            for (Iterator<Map.Entry<ByteString, ByteString>> iterator = kvPairs.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<ByteString, ByteString> item = iterator.next();
                k = item.getKey().toStringUtf8();
                // If the key already exists, do not insert.
                if (!rawKVClient.get(item.getKey()).isEmpty()) {
                    iterator.remove();
                    totalLineCount.addAndGet(-1);
                    logger.warn(String.format("Skip key - exists: [ %s ], file is [ %s ]", k, file.getAbsolutePath()));
                    totalSkipCount.addAndGet(1);
                }
            }
            if (!kvPairs.isEmpty()) {
                try {
                    rawKVClient.batchPut(kvPairs);
                } catch (Exception e) {
                    logger.error(String.format("Batch put Tikv failed, file is [ %s ]", file.getAbsolutePath()), e);
                }
            }
            totalLineCount.addAndGet(kvPairs.size());
            kvPairs.clear();
        }
    }

}
