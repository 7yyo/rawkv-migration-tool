package com.pingcap;

import com.pingcap.importer.IndexInfo2T;
import com.pingcap.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.Properties;

public class Main {

    //    private static final String propertiesPath = System.getProperty("p");
    private static final String propertiesPath = "src/main/resources/importer.properties";
    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) {

        Properties properties = PropertiesUtil.getProperties(propertiesPath);
        String scenes = properties.getProperty("importer.in.scenes");
        if (StringUtils.isNotBlank(scenes)) {
            switch (scenes) {
                case "indexInfo":
                    IndexInfo2T.RunIndexInfo2T(properties);
                    break;
                case "tmpIndexInfo":

                default:
                    logger.error(String.format("Illegal scene [%s]", scenes));
            }
        } else {
            logger.error("[importer.in.scenes] must not be null!");
        }


    }

}
