package com.pingcap;

import com.pingcap.enums.Model;
import com.pingcap.export.LimitExporter;
import com.pingcap.export.RegionExporter;
import com.pingcap.importer.Importer;
import com.pingcap.importer.IndexTypeImporter;
import com.pingcap.job.CheckSumJsonJob;
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

/**
 * @author yuyang
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    /**
     * Total import file count
     */
    static final Counter FILE_COUNTER = Counter.build().name("file_counter").help("File counter.").labelNames("file_counter").register();

    public static void main(String[] args) {

        logger.info("Welcome to to_rawKV!");

        // Get properties by file path
        String propertiesPath = System.getProperty("p");
        if (propertiesPath == null) {
            propertiesPath = "src/main/resources/importer.properties";
        }
        Properties properties = PropertiesUtil.getProperties(propertiesPath);
        logger.info(String.format("Properties=%s", properties));

        TiSession tiSession = null;
        try {
            tiSession = TiSessionUtil.getTiSession(properties);
            logger.info("Create global tiSession success!");
        } catch (Exception e) {
            logger.error("Create global tiSession failed! Exit!");
            System.exit(0);
        }

        if (System.getProperty("m") != null) {
            // Get value by key
            if (Model.GET.equals(System.getProperty("m")) && System.getProperty("k") != null) {
                String key = System.getProperty("k");
                RawKVClient rawKvClient = tiSession.createRawClient();
                String value = RawKvUtil.get(rawKvClient, key);
                logger.info(String.format("[key]%s=[value]%s}", key, value));
                try {
                    rawKvClient.close();
                    tiSession.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.exit(0);
            }
            // Check value by key from RawKV
            if (Model.CHECK.equals(System.getProperty("m")) && System.getProperty("f") != null) {
                RawKVClient rawKvClient = tiSession.createRawClient();
                RawKvUtil.batchGetCheck(System.getProperty("f"), tiSession, properties);
                try {
                    rawKvClient.close();
                    tiSession.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.exit(0);
            }
            // Clean up all raw kv data
            if (Model.TRUNCATE.equals(System.getProperty("m"))) {
                RawKVClient rawKvClient = tiSession.createRawClient();
                logger.info("Start to clear all data in raw kv...");
                long startTime = System.currentTimeMillis();
                rawKvClient.deleteRange(ByteString.EMPTY, ByteString.EMPTY);
                logger.info(String.format("Cleaned up! duration=[%s]s", (System.currentTimeMillis() - startTime) / 1000));
                try {
                    rawKvClient.close();
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
        String prometheusEnable = properties.getProperty(Model.PROMETHEUS_ENABLE);
        int prometheusPort = Integer.parseInt(properties.getProperty(Model.PROMETHEUS_PORT));

        if (Model.ON.equals(prometheusEnable)) {
            // Prometheus default port 7777
            try {
                new HTTPServer(prometheusPort);
                logger.info(String.format("Start prometheus metrics success! Port=[%s]", prometheusPort));
            } catch (IOException e) {
                logger.error(String.format("Failed to start prometheus, port=[%s]", prometheusPort), e);
            }
            // Prometheus JVM
            DefaultExports.initialize();
        }

        logger.info(String.format("Task = [%s]", task));

        if (StringUtils.isNotBlank(task)) {
            switch (task) {
                // Import task
                case Model.IMPORT:
                    if (StringUtils.isNotBlank(importMode)) {
                        if (Model.INDEX_TYPE.equals(scenes)) {
                            IndexTypeImporter.runIndexType(properties, tiSession);
                            return;
                        }
                        logger.info("Start to run importer!");
                        Importer.runImporter(properties, tiSession, FILE_COUNTER);
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
                        checkSumFileList = FileUtil.showFileList(checkSumFilePath, true);
                    } else {
                        checkSumFileList = FileUtil.showFileList(properties.getProperty(Model.FILE_PATH), true);
                    }
                    ThreadPoolExecutor checkSumThreadPoolExecutor = ThreadPoolUtil.startJob(checkSumThreadNum, checkSumThreadNum, checkSumFilePath);
                    if (checkSumFileList != null) {
                        for (File checkSumFile : checkSumFileList) {
                            checkSumThreadPoolExecutor.execute(new CheckSumJsonJob(checkSumFile.getAbsolutePath(), checkSumDelimiter, tiSession, properties, FILE_COUNTER));
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
                    logger.error(String.format("The configuration parameter [%s] error!", Model.TASK));
            }
        } else {
            logger.error(String.format("[%s] should not be empty!", Model.TASK));
        }

        try {
            tiSession.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        logger.info("to_rawKV has finished running.");
        System.exit(0);

    }

}
