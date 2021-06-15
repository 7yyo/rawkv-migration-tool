package com.pingcap;

import com.pingcap.enums.Model;
import com.pingcap.export.LimitExporter;
import com.pingcap.export.RegionExporter;
import com.pingcap.importer.IndexInfo2T;
import com.pingcap.importer.IndexType2T;
import com.pingcap.job.checkSumJsonJob;
import com.pingcap.util.*;
import io.prometheus.client.Counter;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    // Total import file count
    static final Counter fileCounter = Counter.build().name("file_counter").help("File counter.").labelNames("file_counter").register();
    // Total check sum file count
    static final Counter totalCheckSumFileCounter = Counter.build().name("total_checkSum_file_counter").help("Total_checkSum_file counter.").labelNames("Total_checkSum_file_counter").register();

    public static void main(String[] args) {

        // Get properties by file path
        String propertiesPath = System.getProperty("p");
        if (propertiesPath == null) {
            propertiesPath = "src/main/resources/importer.properties";
        }
        Properties properties = PropertiesUtil.getProperties(propertiesPath);

        TiSession tiSession = TiSessionUtil.getTiSession(properties);

        try {
            new HTTPServer(7777);
        } catch (IOException e) {
            logger.error(String.format("Failed to start prometheus, port=[%s]", 7777), e);
        }

        if (System.getProperty("m") != null) {
            // Get value by key
            if (Model.GET.equals(System.getProperty("m")) && System.getProperty("k") != null) {
                String key = System.getProperty("k");
                RawKVClient rawKVClient = tiSession.createRawClient();
                String value = RawKVUtil.get(rawKVClient, key);
                logger.info(String.format("[key]%s\n[value]%s}", key, value));
                try {
                    rawKVClient.close();
                    tiSession.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.exit(0);
            }
            // Check value by key from RawKV
            if (Model.CHECK.equals(System.getProperty("m")) && System.getProperty("f") != null) {
                RawKVClient rawKVClient = tiSession.createRawClient();
                RawKVUtil.batchGetCheck(System.getProperty("f"), tiSession, properties);
                try {
                    rawKVClient.close();
                    tiSession.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.exit(0);
            }
            // Clean up all raw kv data
            if (Model.TRUNCATE.equals(System.getProperty("m"))) {
                RawKVClient rawKVClient = tiSession.createRawClient();
                logger.info("Start to clear all data in raw kv...");
                long startTime = System.currentTimeMillis();
                rawKVClient.deleteRange(ByteString.EMPTY, ByteString.EMPTY);
                logger.info(String.format("Cleaned up! duration=[%s]s", (System.currentTimeMillis() - startTime) / 1000));
                try {
                    rawKVClient.close();
                    tiSession.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.exit(0);
            }
        }

        String task = properties.getProperty(Model.TASK);
        String importMode = properties.getProperty(Model.MODE);
        String scenes = properties.getProperty(Model.SCENES);
        String checkSumFilePath = properties.getProperty(Model.CHECK_SUM_FILE_PATH);
        String checkSumDelimiter = properties.getProperty(Model.CHECK_SUM_DELIMITER);
        int checkSumThreadNum = Integer.parseInt(properties.getProperty(Model.CHECK_SUM_THREAD_NUM));

        // Prometheus JVM
        DefaultExports.initialize();

        if (StringUtils.isNotBlank(task)) {
            switch (task) {
                // Import task
                case Model.IMPORT:
                    if (StringUtils.isNotBlank(importMode)) {
                        if (Model.INDEX_TYPE.equals(scenes)) {
                            IndexType2T.runIndexType(properties, tiSession);
                            return;
                        }
                        IndexInfo2T.RunIndexInfo2T(properties, tiSession, fileCounter);
                    } else {
                        logger.error(String.format("The configuration parameter [%s] must not be empty!", Model.MODE));
                    }
                    break;
                // Check sum task
                case Model.CHECK_SUM:
                    long checkStartTime = System.currentTimeMillis();
                    String simpleCheckSum = properties.getProperty(Model.SIMPLE_CHECK_SUM);
                    List<File> checkSumFileList;
                    if (!Model.ON.equals(simpleCheckSum)) {
                        checkSumFileList = FileUtil.showFileList(checkSumFilePath, true, properties);
                    } else {
                        checkSumFileList = FileUtil.showFileList(properties.getProperty(Model.FILE_PATH), true, properties);
                    }
                    totalCheckSumFileCounter.labels("check sum").inc(checkSumFileList.size());
                    ThreadPoolExecutor checkSumThreadPoolExecutor = ThreadPoolUtil.startJob(checkSumThreadNum, checkSumThreadNum, properties, checkSumFilePath);
                    if (checkSumFileList != null) {
                        for (File checkSumFile : checkSumFileList) {
                            checkSumThreadPoolExecutor.execute(new checkSumJsonJob(checkSumFile.getAbsolutePath(), checkSumDelimiter, tiSession, properties, fileCounter));
                        }
                    } else {
                        logger.error(String.format("Check sum file [%s] is not exists!", checkSumFilePath));
                        return;
                    }
                    checkSumThreadPoolExecutor.shutdown();
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
                case Model.EXPORT:
                    String exportMode = properties.getProperty(Model.EXPORT_MODE);
                    String exportFilePath = properties.getProperty(Model.EXPORT_FILE_PATH);
                    File file = new File(exportFilePath);
                    file.mkdir();
                    logger.info("Start to export all raw kv data.");
                    switch (exportMode) {
                        case Model.REGION_EXPORT:
                            RegionExporter.runRegionExporter(exportFilePath, properties, tiSession);
                            break;
                        case Model.LIMIT_EXPORT:
                            LimitExporter.runLimitExporter(exportFilePath, properties, tiSession);
                            break;
                        default:
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
