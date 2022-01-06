package com.pingcap.rawkv;

import com.pingcap.enums.Model;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

public class RawKv {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    public static void get(TiSession tiSession, String key) {
        if (StringUtils.isEmpty(key)) {
            logger.warn("The key cannot be null.");
        } else {
            RawKVClient rawKvClient = tiSession.createRawClient();
            String value = rawKvClient.get(ByteString.copyFromUtf8(key)).toStringUtf8();
            logger.info("Key={}, Value={}", key, value);
            rawKvClient.close();
        }
    }

    public static void truncateRawKv(TiSession tiSession) {
        logger.info("Start to truncate Raw KV...");
        RawKVClient rawKvClient = tiSession.createRawClient();
        long startTime = System.currentTimeMillis();
        rawKvClient.deleteRange(ByteString.EMPTY, ByteString.EMPTY);
        rawKvClient.close();
        logger.info("Truncate complete. Duration={}s", (System.currentTimeMillis() - startTime) / 1000);
    }

    public static void deleteByKey(TiSession tiSession, String key) {
        if (StringUtils.isEmpty(key)) {
            logger.warn("The key cannot by null.");
        } else {
            try (RawKVClient rawKvClient = tiSession.createRawClient()) {
                rawKvClient.delete(ByteString.copyFromUtf8(key));
                logger.info("Delete key={} success.", key);
            } catch (Exception e) {
                logger.error("Delete key={} fail.", key);
            }
        }
    }

    public static void deleteByPrefix(TiSession tiSession, String key) {
        if (StringUtils.isEmpty(key)) {
            logger.warn("The key prefix cannot by null.");
        } else {
            try (RawKVClient rawKvClient = tiSession.createRawClient()) {
                rawKvClient.deletePrefix(ByteString.copyFromUtf8(key));
                logger.info("Delete key by prefix={} success.", key);
            } catch (Exception e) {
                logger.error("Delete key by prefix={} fail.", key);
            }
        }
    }

}
