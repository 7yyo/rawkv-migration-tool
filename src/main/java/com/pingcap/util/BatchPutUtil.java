package com.pingcap.util;

import org.apache.log4j.Logger;
import org.tikv.raw.RawKVClient;
import shade.com.google.protobuf.ByteString;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class BatchPutUtil {

    private static final Logger logger = Logger.getLogger(BatchPutUtil.class);

    public static void batchPut(HashMap<ByteString,ByteString> kvPairs, RawKVClient rawKVClient, File file) {
        if (!kvPairs.isEmpty()) {
            try {
                rawKVClient.batchPut(kvPairs);
            } catch (Exception e) {
                logger.error(String.format("Batch put Tikv failed, file is [ %s ]", file.getAbsolutePath()), e);
            }
        }
    }

}
