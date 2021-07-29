package com.pingcap.util;

import com.pingcap.enums.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

import java.util.Map;
import java.util.Properties;

/**
 * @author yuyang
 */
public class TiSessionUtil {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    public static TiSession getTiSession(Map<String, String> properties) {
        String pd = properties.get(Model.PD);
        TiConfiguration conf;
        TiSession tiSession = null;
        try {
            conf = TiConfiguration.createRawDefault(pd);
            tiSession = TiSession.create(conf);
            logger.info("Create global TiSession success!");
        } catch (Exception e) {
            logger.error("Failed to create TiSession!");
            System.exit(0);
        }
        return tiSession;
    }

}
