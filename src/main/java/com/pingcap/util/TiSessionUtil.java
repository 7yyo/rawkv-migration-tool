package com.pingcap.util;

import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

import java.util.Properties;

public class TiSessionUtil {

    private static final Properties properties = PropertiesUtil.getProperties();
    private static String pd = properties.getProperty("importer.tikv.pd");

    public static TiSession getTiSession() {
        TiConfiguration conf = TiConfiguration.createRawDefault(pd);
        return TiSession.create(conf);
    }
}
