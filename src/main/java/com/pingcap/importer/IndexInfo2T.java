package com.pingcap.importer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.enums.Model;
import com.pingcap.job.checkSumJsonJob;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.TempIndexInfo;
import com.pingcap.timer.ImportTimer;
import com.pingcap.util.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexInfo2T {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    public static void RunIndexInfo2T(Properties properties, TiSession tiSession) {

        String filesPath = properties.getProperty(Model.FILE_PATH);
        int corePoolSize = Integer.parseInt(properties.getProperty(Model.CORE_POOL_SIZE));
        int maxPoolSize = Integer.parseInt(properties.getProperty(Model.MAX_POOL_SIZE));
        String checkSumDelimiter = properties.getProperty(Model.CHECK_SUM_DELIMITER);
        String checkSumFilePath = properties.getProperty(Model.CHECK_SUM_FILE_PATH);
        String ttlType = properties.getProperty(Model.TTL_TYPE);

        long importStartTime = System.currentTimeMillis();

        // Traverse all the files that need to be written.
        List<File> fileList = FileUtil.showFileList(filesPath, false, properties);
        // Clear the check sum folder before starting.
        FileUtil.deleteFolder(properties.getProperty(Model.CHECK_SUM_FILE_PATH));
        FileUtil.deleteFolders(properties.getProperty(Model.CHECK_SUM_FILE_PATH));
        // Generate ttl type map.
        List<String> ttlTypeList = new ArrayList<>(Arrays.asList(ttlType.split(",")));

        // Start the Main thread for each file.showFileList
        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(corePoolSize, maxPoolSize, properties, filesPath);
        if (fileList != null) {
            for (File file : fileList) {
                // Pass in the file to be processed and the ttl map.
                // The ttl map is shared by all file threads, because it is a table for processing, which is summarized here.
                threadPoolExecutor.execute(new IndexInfo2TJob(file.getAbsolutePath(), tiSession, properties, ttlTypeList));
            }
        } else {
            logger.error("Data file is empty!");
            return;
        }

        threadPoolExecutor.shutdown();

        try {
            threadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            long duration = System.currentTimeMillis() - importStartTime;
            logger.info(String.format("All files import is complete! It takes [%s] seconds", (duration / 1000)));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long checkStartTime = System.currentTimeMillis();
        int checkSumThreadNum = Integer.parseInt(properties.getProperty(Model.CHECK_SUM_THREAD_NUM));

        ThreadPoolExecutor checkSumThreadPoolExecutor;
        if (Model.ON.equals(properties.getProperty(Model.ENABLE_CHECK_SUM))) {
            List<File> checkSumFileList = FileUtil.showFileList(checkSumFilePath, true, properties);
            checkSumThreadPoolExecutor = ThreadPoolUtil.startJob(checkSumThreadNum, checkSumThreadNum, properties, filesPath);
            if (!checkSumFileList.isEmpty()) {
                for (File checkSumFile : checkSumFileList) {
                    checkSumThreadPoolExecutor.execute(new checkSumJsonJob(checkSumFile.getAbsolutePath(), checkSumDelimiter, tiSession, properties));
                }
                checkSumThreadPoolExecutor.shutdown();
            } else {
                logger.error(String.format("Check sum file [%s] is not exists!", checkSumFilePath));
                return;
            }

            try {
                checkSumThreadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                long duration = System.currentTimeMillis() - checkStartTime;
                logger.info(String.format("All files check sum is complete! It takes [%s] seconds", (duration / 1000)));
                logger.info(String.format("Total duration=[%s] seconds", ((System.currentTimeMillis() - importStartTime) / 1000)));
                System.exit(0);
            } catch (
                    InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

class IndexInfo2TJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    private final String filePath;
    private final Properties properties;
    // Generate ttl type map.
    private final List<String> ttlTypeList;
    private final TiSession tiSession;

    private final AtomicInteger totalImportCount = new AtomicInteger(0);
    private final AtomicInteger totalSkipCount = new AtomicInteger(0);
    private final AtomicInteger totalParseErrorCount = new AtomicInteger(0);
    private final AtomicInteger totalBatchPutFailCount = new AtomicInteger(0);

    public IndexInfo2TJob(String filePath, TiSession tiSession, Properties properties, List<String> ttlTypeList) {
        this.filePath = filePath;
        this.properties = properties;
        this.ttlTypeList = ttlTypeList;
        this.tiSession = tiSession;
    }

    @Override
    public void run() {

        int insideThread = Integer.parseInt(properties.getProperty(Model.INTERNAL_THREAD_NUM));

        long startTime = System.currentTimeMillis();

        HashMap<String, Long> ttlTypeCountMap = null;
        if (!ttlTypeList.isEmpty()) {
            ttlTypeCountMap = FileUtil.getTtlTypeMap(ttlTypeList);
        }

        // Start the file sub-thread,
        // import the data of the file through the sub-thread, and divide the data in advance according to the number of sub-threads.
        File file = new File(filePath);
        int lines = FileUtil.getFileLines(file);
        List<String> threadPerLineList = CountUtil.getPerThreadFileLines(lines, insideThread, file.getAbsolutePath());

        Timer timer = new Timer();
        ImportTimer importTimer = new ImportTimer(totalImportCount, lines, filePath);
        timer.schedule(importTimer, 5000, Long.parseLong(properties.getProperty(Model.TIMER_INTERVAL)));

        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(insideThread, insideThread, properties, file.getName());
        for (String s : threadPerLineList) {
            threadPoolExecutor.execute(new BatchPutIndexInfoJob(tiSession, totalImportCount, totalSkipCount, totalParseErrorCount, totalBatchPutFailCount, filePath, ttlTypeList, ttlTypeCountMap, s, properties));
        }

        threadPoolExecutor.shutdown();

        while (true) {
            if (threadPoolExecutor.isTerminated()) {
                break;
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        StringBuilder result = new StringBuilder("[Import Report] File[" + file.getAbsolutePath() + "],TotalRows[" + lines + "],ImportedRows[" + totalImportCount + "],SkipRows[" + totalSkipCount + "],ParseERROR[" + totalParseErrorCount + "],BatchPutERROR[" + totalBatchPutFailCount + "],Duration[" + duration / 1000 + "s],");
        result.append("Skip type[");

        assert ttlTypeCountMap != null;
        if (!ttlTypeCountMap.isEmpty()) {
            for (Map.Entry<String, Long> item : ttlTypeCountMap.entrySet()) {
                result.append("<").append(item.getKey()).append(">").append("[").append(item.getValue()).append("]").append("]");
            }
        }
        timer.cancel();
        logger.info(result.toString());
    }
}

class BatchPutIndexInfoJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger auditLog = LoggerFactory.getLogger(Model.AUDIT_LOG);

    private final String filePath;
    private final TiSession tiSession;
    private final List<String> ttlTypeList;
    private final HashMap<String, Long> ttlTypeCountMap;
    private final String fileBlock;
    private final AtomicInteger totalImportCount;
    private final AtomicInteger totalSkipCount;
    private final AtomicInteger totalParseErrorCount;
    private final AtomicInteger totalBatchPutFailCount;
    private final Properties properties;
    private FileChannel fileChannel;

    public BatchPutIndexInfoJob(TiSession tiSession, AtomicInteger totalImportCount, AtomicInteger totalSkipCount, AtomicInteger totalParseErrorCount, AtomicInteger totalBatchPutFailCount, String filePath, List<String> ttlTypeList, HashMap<String, Long> ttlTypeCountMap, String fileBlock, Properties properties) {
        this.totalImportCount = totalImportCount;
        this.tiSession = tiSession;
        this.totalSkipCount = totalSkipCount;
        this.totalParseErrorCount = totalParseErrorCount;
        this.totalBatchPutFailCount = totalBatchPutFailCount;
        this.filePath = filePath;
        this.ttlTypeList = ttlTypeList;
        this.ttlTypeCountMap = ttlTypeCountMap;
        this.fileBlock = fileBlock;
        this.properties = properties;
    }

    @Override
    public void run() {

        String envId = properties.getProperty(Model.ENV_ID);
        String appId = properties.getProperty(Model.APP_ID);
        String importMode = properties.getProperty(Model.MODE);
        String scenes = properties.getProperty(Model.SCENES);
        int batchSize = Integer.parseInt(properties.getProperty(Model.BATCH_SIZE));
        int checkSumPercentage = Integer.parseInt(properties.getProperty(Model.CHECK_SUM_PERCENTAGE));
        String isCheckSum = properties.getProperty(Model.ENABLE_CHECK_SUM);
        String delimiter_1 = properties.getProperty(Model.DELIMITER_1);
        String delimiter_2 = properties.getProperty(Model.DELIMITER_2);

        File file = new File(filePath);

        int start = Integer.parseInt(fileBlock.split(",")[0]);
        int todo = Integer.parseInt(fileBlock.split(",")[1]);


        if (Model.ON.equals(isCheckSum)) {
            fileChannel = CheckSumUtil.initCheckSumLog(properties, fileChannel, file);
        }

        LineIterator lineIterator = null;
        try {
            lineIterator = FileUtils.lineIterator(file, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int m = 0; m < start; m++) {
            lineIterator.nextLine();
        }

        int count = 0;
        int totalCount = 0;
        String line;
        String indexInfoKey = null;
        JSONObject jsonObject;
        ConcurrentHashMap<ByteString, ByteString> kvPairs = new ConcurrentHashMap<>();
        RawKVClient rawKVClient = tiSession.createRawClient();
        String checkSumDelimiter = properties.getProperty(Model.CHECK_SUM_DELIMITER);

        Random random = new Random();
        ByteString key;
        ByteString value;
        SimpleDateFormat simpleDateFormat;
        String time;
        IndexInfo indexInfoS;
        IndexInfo indexInfoT = new IndexInfo();
        TempIndexInfo tempIndexInfoS;
        TempIndexInfo tempIndexInfoT = new TempIndexInfo();
        String id;
        String type;

        for (int n = 0; n < todo; n++) {
            try {

                count++;
                totalCount++;

                line = lineIterator.nextLine();
                if (StringUtils.isBlank(line)) {
                    logger.warn(String.format("This is blank in file=%s, line=%s", file.getAbsolutePath(), start + totalCount));
                    continue;
                }
                key = null;
                value = null;

                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                time = simpleDateFormat.format(new Date());

                switch (importMode) {
                    case Model.JSON_FORMAT:
                        try {
                            jsonObject = JSONObject.parseObject(line);
                        } catch (Exception e) {
                            auditLog.error(String.format("Failed to parse json, file='%s', json='%s',line=%s,", file, line, start + totalCount));
                            totalParseErrorCount.addAndGet(1);
                            // if _todo_ == totalCount in json failed, batch put.
                            count = RawKVUtil.batchPut(totalCount, todo, count, batchSize, rawKVClient, kvPairs, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);
                            continue;
                        }
                        switch (scenes) {
                            case Model.INDEX_INFO:
                                indexInfoS = JSON.toJavaObject(jsonObject, IndexInfo.class);
                                // Skip the type that exists in the tty type map.
                                if (ttlTypeList.contains(indexInfoS.getType())) {
                                    ttlTypeCountMap.put(indexInfoS.getType(), ttlTypeCountMap.get(indexInfoS.getType()) + 1);
                                    auditLog.warn(String.format("[Skip TTL] [%s] in [%s],line=[%s]", indexInfoKey, file.getAbsolutePath(), start + totalCount));
                                    totalSkipCount.addAndGet(1);
                                    count = RawKVUtil.batchPut(totalCount, todo, count, batchSize, rawKVClient, kvPairs, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);
                                    continue;
                                }
                                if (envId != null) {
                                    indexInfoKey = String.format(IndexInfo.INDEX_INFO_KET_FORMAT, envId, indexInfoS.getType(), indexInfoS.getId());
                                } else {
                                    indexInfoKey = String.format(IndexInfo.INDEX_INFO_KET_FORMAT, indexInfoS.getEnvId(), indexInfoS.getType(), indexInfoS.getId());
                                }
                                // TiKV indexInfo
                                indexInfoT = IndexInfo.initIndexInfoT(indexInfoT, indexInfoS, time);
                                indexInfoT.setAppId(indexInfoS.getAppId());
                                key = ByteString.copyFromUtf8(indexInfoKey);
                                value = ByteString.copyFromUtf8(JSONObject.toJSONString(indexInfoT));
                                logger.debug(String.format("[%s], K=%s, V={%s}", file.getAbsolutePath(), indexInfoKey, JSONObject.toJSONString(indexInfoT)));
                                break;
                            case Model.TEMP_INDEX_INFO:
                                tempIndexInfoS = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                                if (envId != null) {
                                    indexInfoKey = String.format(TempIndexInfo.TEMP_INDEX_INFO_KEY_FORMAT, envId, tempIndexInfoS.getId());
                                } else {
                                    indexInfoKey = String.format(TempIndexInfo.TEMP_INDEX_INFO_KEY_FORMAT, tempIndexInfoS.getEnvId(), tempIndexInfoS.getId());
                                }
                                // TiKV tempIndexInfo
                                tempIndexInfoT = TempIndexInfo.initTempIndexInfo(tempIndexInfoT, tempIndexInfoS);
                                key = ByteString.copyFromUtf8(indexInfoKey);
                                value = ByteString.copyFromUtf8(JSONObject.toJSONString(tempIndexInfoT));
                                logger.debug(String.format("[%s], K=%s, V={%s}", file.getAbsolutePath(), indexInfoKey, JSONObject.toJSONString(tempIndexInfoT)));
                                break;
                        }
                        break;
                    case Model.CSV_FORMAT:
                        try {
                            id = line.split(delimiter_1)[0];
                            type = line.split(delimiter_1)[1];

                            indexInfoS = new IndexInfo();
                            if (envId != null) {
                                indexInfoKey = String.format(IndexInfo.INDEX_INFO_KET_FORMAT, envId, type, id);
                            } else {
                                indexInfoKey = String.format(IndexInfo.INDEX_INFO_KET_FORMAT, indexInfoS.getEnvId(), type, id);
                            }

                            // Skip the type that exists in the tty type map.
                            if (ttlTypeList.contains(type)) {
                                ttlTypeCountMap.put(type, ttlTypeCountMap.get(type) + 1);
                                auditLog.warn(String.format("[Skip TTL] [%s] in [%s],line=[%s]", indexInfoKey, file.getAbsolutePath(), start + totalCount));
                                totalSkipCount.addAndGet(1);
                                count = RawKVUtil.batchPut(totalCount, todo, count, batchSize, rawKVClient, kvPairs, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);
                                continue;
                            }

                            indexInfoT = IndexInfo.initIndexInfo(line, delimiter_1, delimiter_2);
                            indexInfoT.setAppId(appId);
                            indexInfoT.setUpdateTime(time);
                            key = ByteString.copyFromUtf8(indexInfoKey);
                            value = ByteString.copyFromUtf8(JSONObject.toJSONString(indexInfoT));
                        } catch (Exception e) {
                            logger.error(String.format("Failed to parse csv, file='%s', csv='%s',line=%s,", file, line, start + totalCount));
                            totalParseErrorCount.addAndGet(1);
                            // if _todo_ == totalCount in json failed, batch put.
                            count = RawKVUtil.batchPut(totalCount, todo, count, batchSize, rawKVClient, kvPairs, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);
                            continue;
                        }

                        break;
                    default:
                        logger.error(String.format("Illegal format: %s", importMode));
                        return;
                }

                // Sampling data is written into the check sum file
                if (Model.ON.equals(isCheckSum)) {
                    int nn = random.nextInt(100 / checkSumPercentage) + 1;
                    if (nn == 1) {
                        fileChannel.write(StandardCharsets.UTF_8.encode(indexInfoKey + checkSumDelimiter + (start + totalCount) + "\n"));
                    }
                }

                assert key != null;
                kvPairs.put(key, value);

                count = RawKVUtil.batchPut(totalCount, todo, count, batchSize, rawKVClient, kvPairs, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            if (Model.ON.equals(isCheckSum)) {
                fileChannel.close();
            }
            lineIterator.close();
            rawKVClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}