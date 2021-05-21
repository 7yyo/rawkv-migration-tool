package com.pingcap;

import com.pingcap.enums.Model;
import com.pingcap.importer.IndexInfo2T;
import com.pingcap.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Main {

    //    private static final String propertiesPath = System.getProperty("p");
    private static final String propertiesPath = "src/main/resources/importer.properties";
    private static final Logger logger = LoggerFactory.getLogger("logBackLog");

    public static void main(String[] args) {

        Properties properties = PropertiesUtil.getProperties(propertiesPath);
        String mode = properties.getProperty("importer.in.mode");

        if (StringUtils.isNotBlank(mode)) {
            switch (mode) {
                case Model.JSON_FORMAT:
                    IndexInfo2T.RunIndexInfo2T(properties);
                    break;
                case "tmpIndexInfo":

                default:
                    logger.error(String.format("Illegal scene [%s]", mode));
            }
        } else {
            logger.error("[importer.in.mode] must not be null!");
        }


    }

}
