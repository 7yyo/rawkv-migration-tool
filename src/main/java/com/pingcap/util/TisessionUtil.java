package com.pingcap.util;

import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

import java.util.Properties;

public class TiSessionUtil {

    public static TiSession getTiSession(Properties properties) {
        String pd = properties.getProperty("importer.tikv.pd");
        TiConfiguration conf = TiConfiguration.createRawDefault(pd);
        return TiSession.create(conf);
    }
}
