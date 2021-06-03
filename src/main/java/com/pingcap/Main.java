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
import org.tikv.raw.RawKVClient;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    public static void main(String[] args) {

        String propertiesPath = System.getProperty("p");
        if (propertiesPath == null) {
            propertiesPath = "src/main/resources/importer.properties";
        }

        Properties properties = PropertiesUtil.getProperties(propertiesPath);

        TiSession tiSession = TiSessionUtil.getTiSession(properties);

        if (Model.GET.equals(System.getProperty("m")) && System.getProperty("k") != null) {
            RawKVClient rawKVClient = tiSession.createRawClient();
            String value = RawKVUtil.get(rawKVClient, System.getProperty("k"));
            logger.info(String.format("Result={key=%s, value=%s}", System.getProperty("k"), value));

            try {
                rawKVClient.close();
                tiSession.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return;
        }
        if (Model.CHECK.equals(System.getProperty("m")) && System.getProperty("f") != null) {
            RawKVClient rawKVClient = tiSession.createRawClient();
            RawKVUtil.batchGetCheck(System.getProperty("f"), tiSession, properties);
            try {
                rawKVClient.close();
                tiSession.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return;
        }

        String task = properties.getProperty(Model.TASK);
        String importMode = properties.getProperty(Model.MODE);
        String scenes = properties.getProperty(Model.SCENES);
        String checkSumFilePath = properties.getProperty(Model.CHECK_SUM_FILE_PATH);
        String checkSumDelimiter = properties.getProperty(Model.CHECK_SUM_DELIMITER);
        int checkSumThreadNum = Integer.parseInt(properties.getProperty(Model.CHECK_SUM_THREAD_NUM));

        if (StringUtils.isNotBlank(task)) {
            switch (task) {
                case Model.IMPORT:
                    if (StringUtils.isNotBlank(importMode)) {
                        if (Model.INDEX_TYPE.equals(scenes)) {
                            IndexType2T.runIndexType(properties, tiSession);
                            return;
                        }
                        IndexInfo2T.RunIndexInfo2T(properties, tiSession);
                    } else {
                        logger.error(String.format("The configuration parameter [%s] must not be empty!", Model.MODE));
                    }
                    break;
                case Model.CHECK_SUM:
                    long checkStartTime = System.currentTimeMillis();
                    String simpleCheckSum = properties.getProperty(Model.SIMPLE_CHECK_SUM);
                    List<File> checkSumFileList;
                    if (!Model.ON.equals(simpleCheckSum)) {
                        checkSumFileList = FileUtil.showFileList(checkSumFilePath, true, properties);
                    } else {
                        checkSumFileList = FileUtil.showFileList(properties.getProperty(Model.FILE_PATH), true, properties);
                    }

                    ThreadPoolExecutor checkSumThreadPoolExecutor = ThreadPoolUtil.startJob(checkSumThreadNum, checkSumThreadNum, properties, checkSumFilePath);
                    if (checkSumFileList != null) {
                        for (File checkSumFile : checkSumFileList) {
                            checkSumThreadPoolExecutor.execute(new checkSumJsonJob(checkSumFile.getAbsolutePath(), checkSumDelimiter, tiSession, properties));
                        }
                        checkSumThreadPoolExecutor.shutdown();
                    } else {
                        logger.error(String.format("Check sum file [%s] is not exists!", checkSumFilePath));
                        return;
                    }
                    try {
                        if (checkSumThreadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
                            long duration = System.currentTimeMillis() - checkStartTime;
                            logger.info(String.format("All files check sum is complete! It takes [%s] seconds", (duration / 1000)));
                            System.exit(0);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    break;
                default:
                    logger.error(String.format("The configuration parameter [%s] must be [import] or [checkSum]]!", Model.TASK));
            }
        }

        try {
            tiSession.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
