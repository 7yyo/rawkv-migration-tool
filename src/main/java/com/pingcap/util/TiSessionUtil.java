package com.pingcap.util;

import com.pingcap.enums.Model;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

import java.util.Properties;

public class TiSessionUtil {

    public static TiSession getTiSession(Properties properties) {
        String pd = properties.getProperty(Model.PD);
        TiConfiguration conf = TiConfiguration.createRawDefault(pd);
        return TiSession.create(conf);
    }

}
