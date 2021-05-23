package com.pingcap;

import com.pingcap.enums.Model;
import com.pingcap.importer.IndexInfo2T;
import com.pingcap.importer.IndexType2T;
import com.pingcap.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Main {

    //    private static final String propertiesPath = System.getProperty("p");
    private static final String propertiesPath = "src/main/resources/importer.properties";
    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    public static void main(String[] args) {

        Properties properties = PropertiesUtil.getProperties(propertiesPath);
        String importMode = properties.getProperty(Model.MODE);
        String scenes = properties.getProperty(Model.SCENES);

        if (StringUtils.isNotBlank(importMode)) {
            if (Model.INDEX_TYPE.equals(scenes)) {
                IndexType2T.RunIndexInfo2T(properties);
                return;
            }
            switch (importMode) {
                case Model.JSON_FORMAT:
                    IndexInfo2T.RunIndexInfo2T(properties);
                    break;
                case Model.CSV_FORMAT:
                    logger.info("CSV");
                default:
                    logger.error(String.format("Illegal scene [%s]", importMode));
            }
        } else {
            logger.error(String.format("The configuration parameter [%s] must not be empty!", Model.MODE));
        }

    }

}
