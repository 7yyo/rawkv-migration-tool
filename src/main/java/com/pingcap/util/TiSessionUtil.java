package com.pingcap.util;

import com.pingcap.enums.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

import java.util.Map;

public class TiSessionUtil {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    public static TiSession getTiSession(Map<String, String> properties) {
        String pd = properties.get(Model.PD);
        TiConfiguration conf;
        TiSession tiSession = null;
        try {
            conf = TiConfiguration.createRawDefault(pd);
            conf.setRawKVWriteTimeoutInMS(Integer.parseInt(properties.get(Model.WRITE_TIMEOUT)));
            conf.setRawKVReadTimeoutInMS(Integer.parseInt(properties.get(Model.READ_TIMEOUT)));
            conf.setRawKVBatchWriteTimeoutInMS(Integer.parseInt(properties.get(Model.BATCH_WRITE_TIMEOUT)));
            conf.setRawKVBatchReadTimeoutInMS(Integer.parseInt(properties.get(Model.BATCH_READ_TIMEOUT)));
            conf.setRawKVScanTimeoutInMS(Integer.parseInt(properties.get(Model.SCAN_TIMEOUT)));
            conf.setRawKVCleanTimeoutInMS(Integer.parseInt(properties.get(Model.CLEAN_TIMEOUT)));
            tiSession = TiSession.create(conf);
            logger.info("Create TiSession success.");
        } catch (Exception e) {
            logger.error("Create TiSession failed! Error: ", e);
            System.exit(0);
        }
        return tiSession;
    }

}
