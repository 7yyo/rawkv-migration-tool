package com.pingcap.importer;

import com.pingcap.enums.Model;
import com.pingcap.timer.ImportTimer;
import com.pingcap.util.CountUtil;
import com.pingcap.util.FileUtil;
import com.pingcap.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;

import java.io.File;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class ImporterJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    private final String importFilePath;
    private final Map<String, String> properties;
    private final TiSession tiSession;

    private final AtomicInteger totalImportCount = new AtomicInteger(0);
    private final AtomicInteger totalSkipCount = new AtomicInteger(0);
    private final AtomicInteger totalParseErrorCount = new AtomicInteger(0);
    private final AtomicInteger totalBatchPutFailCount = new AtomicInteger(0);
    private final AtomicInteger totalDuplicateCount = new AtomicInteger(0);

    public ImporterJob(String importFilePath, TiSession tiSession, Map<String, String> properties) {
        this.importFilePath = importFilePath;
        this.properties = properties;
        this.tiSession = tiSession;
    }

    @Override
    public void run() {

        long startTime = System.currentTimeMillis();

        PropertiesUtil.checkConfig(properties, Model.TTL_SKIP_TYPE);
        List<String> ttlSkipTypeList = new ArrayList<>(Arrays.asList(properties.get(Model.TTL_SKIP_TYPE).split(",")));
        // Used to count the number of skipped entries for each ttl type.
        LinkedHashMap<String, Long> ttlSkipTypeMap = new LinkedHashMap<>();
        if (!ttlSkipTypeList.isEmpty()) {
            ttlSkipTypeMap = FileUtil.getTtlSkipTypeMap(ttlSkipTypeList);
        }

        PropertiesUtil.checkConfig(properties, Model.TTL_PUT_TYPE);
        List<String> ttlPutList = new ArrayList<>(Arrays.asList(properties.get(Model.TTL_PUT_TYPE).split(",")));

        // Start the file sub-thread, import the data of the file through the sub-thread, and divide the data in advance according to the number of sub-threads.
        File importFile = new File(importFilePath);
        int importFileLineNum = FileUtil.getFileLines(importFile);

        PropertiesUtil.checkConfig(properties, Model.INTERNAL_THREAD_NUM);
        int internalThreadNum = Integer.parseInt(properties.get(Model.INTERNAL_THREAD_NUM));
        List<String> threadPerLineList = CountUtil.getPerThreadFileLines(importFileLineNum, internalThreadNum, importFile.getAbsolutePath());

        Timer timer = new Timer();
        ImportTimer importTimer = new ImportTimer(totalImportCount, importFileLineNum, importFilePath);
        PropertiesUtil.checkConfig(properties, Model.TIMER_INTERVAL);
        timer.schedule(importTimer, 5000, Long.parseLong(properties.get(Model.TIMER_INTERVAL)));

        // Block until all child threads end.
        CountDownLatch countDownLatch = new CountDownLatch(threadPerLineList.size());

        for (String fileBlock : threadPerLineList) {
            BatchPutJob batchPutJob = new BatchPutJob(
                    tiSession,
                    totalImportCount,
                    totalSkipCount,
                    totalParseErrorCount,
                    totalBatchPutFailCount,
                    importFilePath,
                    ttlSkipTypeList,
                    ttlSkipTypeMap,
                    fileBlock,
                    properties,
                    countDownLatch,
                    totalDuplicateCount,
                    ttlPutList);
            batchPutJob.start();
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long duration = System.currentTimeMillis() - startTime;
        StringBuilder result = new StringBuilder(
                "[Import summary] " +
                        "file=" + importFile.getAbsolutePath() + ", " +
                        "total=" + importFileLineNum + ", " +
                        "imported=" + totalImportCount + ", " +
                        "skip=" + totalSkipCount + ", " +
                        "parseErr=" + totalParseErrorCount + ", " +
                        "putErr=" + totalBatchPutFailCount + ", " +
                        "duplicate=" + totalDuplicateCount + ", " +
                        "duration=" + duration / 1000 + "s, ");
        result.append("Skip type[");
        for (Map.Entry<String, Long> item : ttlSkipTypeMap.entrySet()) {
            result.append("<").append(item.getKey()).append(">").append("[").append(item.getValue()).append("]").append("]");
        }

        timer.cancel();
        logger.info(result.toString());

    }
}
