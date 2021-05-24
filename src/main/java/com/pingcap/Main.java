package com.pingcap;

import com.pingcap.enums.Model;
import com.pingcap.importer.IndexInfo2T;
import com.pingcap.importer.IndexType2T;
import com.pingcap.job.checkSumJsonJob;
import com.pingcap.util.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {

//        private static final String propertiesPath = "src/main/resources/importer.properties";
    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    public static void main(String[] args) {

        String propertiesPath = System.getProperty("p");

        Properties properties = PropertiesUtil.getProperties(propertiesPath);
        String task = properties.getProperty(Model.TASK);
        String importMode = properties.getProperty(Model.MODE);
        String scenes = properties.getProperty(Model.SCENES);
        String checkSumFilePath = properties.getProperty(Model.CHECK_SUM_FILE_PATH);
        String checkSumDelimiter = properties.getProperty(Model.CHECK_SUM_DELIMITER);
        int checkSumThreadNum = Integer.parseInt(properties.getProperty(Model.CHECK_SUM_THREAD_NUM));

        switch (task) {
            case Model.IMPORT:
                if (StringUtils.isNotBlank(importMode)) {
                    if (Model.INDEX_TYPE.equals(scenes)) {
                        IndexType2T.RunIndexInfo2T(properties);
                        return;
                    }
                    IndexInfo2T.RunIndexInfo2T(properties);
                } else {
                    logger.error(String.format("The configuration parameter [%s] must not be empty!", Model.MODE));
                }
                break;
            case Model.CHECK_SUM:

                long checkStartTime = System.currentTimeMillis();

                TiSession tiSession = TiSessionUtil.getTiSession(properties);
                CheckSumUtil.checkSumIndexInfoJson(checkSumFilePath, checkSumDelimiter, tiSession, properties);

                List<File> checkSumFileList = FileUtil.showFileList(checkSumFilePath, true, properties);
                ThreadPoolExecutor checkSumThreadPoolExecutor = ThreadPoolUtil.startJob(checkSumThreadNum, checkSumThreadNum, properties);
                if (!checkSumFileList.isEmpty()) {
                    for (File checkSumFile : checkSumFileList) {
                        switch (importMode) {
                            case Model.JSON_FORMAT:
                                checkSumThreadPoolExecutor.execute(new checkSumJsonJob(checkSumFile.getAbsolutePath(), checkSumDelimiter, tiSession, properties));
                                break;
                            case Model.CSV_FORMAT:
                                logger.info("CSV");
                            default:
                                throw new IllegalStateException("Unexpected value: " + scenes);
                        }
                    }
                    checkSumThreadPoolExecutor.shutdown();
                } else {
                    logger.error(String.format("Check sum file [%s] is not exists!", checkSumFilePath));
                    return;
                }

                try {
                    checkSumThreadPoolExecutor.awaitTermination(3000, TimeUnit.SECONDS);
                    long duration = System.currentTimeMillis() - checkStartTime;
                    logger.info(String.format("All files check sum is complete! It takes [%s] seconds", (duration / 1000)));
                    System.exit(0);
                } catch (
                        InterruptedException e) {
                    e.printStackTrace();
                }

                break;
            default:
        }


    }

}
