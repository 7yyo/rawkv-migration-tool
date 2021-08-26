package com.pingcap.importer;

import com.pingcap.enums.Model;
import com.pingcap.timer.ImportTimer;
import com.pingcap.util.CountUtil;
import com.pingcap.util.FileUtil;
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
//    private final int checkSumFilePathNum;

    private final AtomicInteger totalImportCount = new AtomicInteger(0);
    private final AtomicInteger totalSkipCount = new AtomicInteger(0);
    private final AtomicInteger totalParseErrorCount = new AtomicInteger(0);
    private final AtomicInteger totalBatchPutFailCount = new AtomicInteger(0);
    private final AtomicInteger totalDuplicateCount = new AtomicInteger(0);

    public ImporterJob(String importFilePath, TiSession tiSession, Map<String, String> properties) {
        this.importFilePath = importFilePath;
        this.properties = properties;
        this.tiSession = tiSession;
//        this.checkSumFilePathNum = checkSumFilePathNum;
    }

    @Override
    public void run() {

        long startTime = System.currentTimeMillis();

        HashMap<String, Long> ttlSkipTypeMap = new HashMap<>();
        List<String> ttlSkipTypeList = new ArrayList<>(Arrays.asList(properties.get(Model.TTL_SKIP_TYPE).split(",")));
        if (!ttlSkipTypeList.isEmpty()) {
            ttlSkipTypeMap = FileUtil.getTtlSkipTypeMap(ttlSkipTypeList);
        }

        List<String> ttlPutList = new ArrayList<>(Arrays.asList(properties.get(Model.TTL_PUT_TYPE).split(",")));

        // Start the file sub-thread,
        // import the data of the file through the sub-thread, and divide the data in advance according to the number of sub-threads.
        File file = new File(importFilePath);
        int importFileLines = FileUtil.getFileLines(file);
        int threadNum = Integer.parseInt(properties.get(Model.INTERNAL_THREAD_NUM));
        List<String> threadPerLineList = CountUtil.getPerThreadFileLines(importFileLines, threadNum, file.getAbsolutePath());

        // Import timer
        Timer timer = new Timer();
        ImportTimer importTimer = new ImportTimer(totalImportCount, importFileLines, importFilePath);
        timer.schedule(importTimer, 5000, Long.parseLong(properties.get(Model.TIMER_INTERVAL)));

        // Return when all threads are processed.
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
                "[Import Summary] " +
                        "File=" + file.getAbsolutePath() + ", " +
                        "Total=" + importFileLines + ", " +
                        "Imported=" + totalImportCount + ", " +
                        "Skip=" + totalSkipCount + ", " +
                        "ParseErr=" + totalParseErrorCount + ", " +
                        "PutErr=" + totalBatchPutFailCount + ", " +
                        "Duplicate=" + totalDuplicateCount + ", " +
                        "Duration=" + duration / 1000 + "s, ");
        result.append("Skip type[");
        for (Map.Entry<String, Long> item : ttlSkipTypeMap.entrySet()) {
            result.append("<").append(item.getKey()).append(">").append("[").append(item.getValue()).append("]").append("]");
        }

        timer.cancel();
        logger.info(result.toString());

    }
}
