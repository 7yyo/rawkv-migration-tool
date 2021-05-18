package com.pingcap.util;

import org.apache.log4j.Logger;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PutUtil {

    private static final Logger logger = Logger.getLogger(PutUtil.class);

    public static int batchPut(int totalCount, int todo, int count, int batchSize, RawKVClient rawKVClient, ConcurrentHashMap<ByteString, ByteString> kvPairs, File file, AtomicInteger totalLineCount, AtomicInteger totalSkipCount, int totalLine) {
        if (totalCount == todo || count == batchSize) {


            // TODO
            List<ByteString> list = new ArrayList<>();
            for (Map.Entry<ByteString, ByteString> item : kvPairs.entrySet()) {
                list.add(item.getKey());
            }
            rawKVClient.batchDelete(list);
            // TODO


            String k;
            for (Iterator<Map.Entry<ByteString, ByteString>> iterator = kvPairs.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<ByteString, ByteString> item = iterator.next();
                k = item.getKey().toStringUtf8();
                // If the key already exists, do not insert.
                if (!rawKVClient.get(item.getKey()).isEmpty()) {
                    iterator.remove();
//                    logger.(String.format("Skip key - exists: [ %s ], file is [ %s ], line= %s", k, file.getAbsolutePath(), totalLine));
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
            count = 0;
        }
        return count;
    }

}
