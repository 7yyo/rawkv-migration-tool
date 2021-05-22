package com.pingcap.importer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.enums.Model;
import com.pingcap.job.checkSumIndexInfoJsonJob;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.TempIndexInfo;
import com.pingcap.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexInfo2T {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    public static void RunIndexInfo2T(Properties properties) {

        String filesPath = properties.getProperty(Model.FILE_PATH);
        int corePoolSize = Integer.parseInt(properties.getProperty(Model.CORE_POOL_SIZE));
        int maxPoolSize = Integer.parseInt(properties.getProperty(Model.MAX_POOL_SIZE));
        String mode = properties.getProperty(Model.MODE);
        String scenes = properties.getProperty(Model.SCENES);
        String checkSumDelimiter = properties.getProperty(Model.CHECK_SUM_DELIMITER);
        String checkSumFilePath = properties.getProperty(Model.CHECK_SUM_FILE_PATH);
        TiSession tiSession = TiSessionUtil.getTiSession(properties);

        long startImportTime = System.currentTimeMillis();

        // Traverse all the files that need to be written.
        List<File> fileList = FileUtil.showFileList(filesPath, false, properties);

        FileUtil.deleteFolder(properties.getProperty(Model.CHECK_SUM_FILE_PATH));
        FileUtil.deleteFolders(properties.getProperty(Model.CHECK_SUM_FILE_PATH));

        // Start the Main thread for each file.showFileList
        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(corePoolSize, maxPoolSize);
        for (File file : fileList) {
            // Pass in the file to be processed and the ttl map.
            // The ttl map is shared by all file threads, because it is a table for processing, which is summarized here.
            threadPoolExecutor.execute(new IndexInfo2TJob(file.getAbsolutePath(), properties));
        }

        threadPoolExecutor.shutdown();
        try {
            threadPoolExecutor.awaitTermination(3000, TimeUnit.SECONDS);
            long duration = System.currentTimeMillis() - startImportTime;
            logger.info(String.format("All files import is complete! It takes [%s] seconds", (duration / 1000)));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long startCheckSumTime = System.currentTimeMillis();
        int checkSumThreadNum = Integer.parseInt(properties.getProperty(Model.CHECK_SUM_THREAD_NUM));
        ThreadPoolExecutor checkSumThreadPoolExecutor = null;
        if (!"0".equals(properties.getProperty(Model.ENABLE_CHECK_SUM))) {

            List<File> checkSumFileList = FileUtil.showFileList(checkSumFilePath, true, properties);
            checkSumThreadPoolExecutor = ThreadPoolUtil.startJob(checkSumThreadNum, checkSumThreadNum);
            for (File checkSumFile : checkSumFileList) {
                switch (mode) {
                    case Model.JSON_FORMAT:
                        switch (scenes) {
                            case Model.INDEX_INFO:
                                checkSumThreadPoolExecutor.execute(new checkSumIndexInfoJsonJob(checkSumFile.getAbsolutePath(), checkSumDelimiter, tiSession, properties));
                                break;
                            case Model.TEMP_INDEX_INFO:
//                            CheckSumUtil.checkSumTmpIndexInfoJson(fp, checkSumDelimiter, tiSession, file, properties);
                                break;
                            default:
                                throw new IllegalStateException("Unexpected value: " + scenes);
                        }
                        break;
                    case Model.TEMP_INDEX_INFO:
                        System.out.println();
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + mode);
                }
            }
        }

        checkSumThreadPoolExecutor.shutdown();
        while (true) {
            if (checkSumThreadPoolExecutor.isTerminated()) {
                long duration = System.currentTimeMillis() - startCheckSumTime;
                logger.info(String.format("All files check sum is complete! It takes [%s] seconds", (duration / 1000)));
                logger.info(String.format("Total duration=[%s] seconds", ((System.currentTimeMillis() - startImportTime) / 1000)));
                System.exit(0);
            }
        }

    }
}

class IndexInfo2TJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    private final String filePath;
    private final Properties properties;

    private final AtomicInteger totalImportCount = new AtomicInteger(0);
    private final AtomicInteger totalSkipCount = new AtomicInteger(0);
    private final AtomicInteger totalParseErrorCount = new AtomicInteger(0);
    private final AtomicInteger totalBatchPutFailCount = new AtomicInteger(0);

    public IndexInfo2TJob(String filePath, Properties properties) {
        this.filePath = filePath;
        this.properties = properties;
    }

    @Override
    public void run() {

        int insideThread = Integer.parseInt(properties.getProperty(Model.INTERNAL_THREAD_NUM));
        String ttlType = properties.getProperty(Model.TTL_TYPE);

        long startTime = System.currentTimeMillis();

        // Generate ttl type map.
        List<String> ttlTypeList = new ArrayList<>(Arrays.asList(ttlType.split(",")));
        ConcurrentHashMap<String, Long> ttlTypeCountMap = null;
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
        timer.schedule(importTimer, 5000, Long.parseLong(properties.getProperty("importer.timer.interval")));

        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(insideThread, insideThread);
        for (String s : threadPerLineList) {
            threadPoolExecutor.execute(new BatchPutIndexInfoJob(totalImportCount, totalSkipCount, totalParseErrorCount, totalBatchPutFailCount, filePath, ttlTypeList, ttlTypeCountMap, s, properties));
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
    private final List<String> ttlTypeList;
    private final ConcurrentHashMap<String, Long> ttlTypeCountMap;
    private final String fileBlock;
    private final AtomicInteger totalImportCount;
    private final AtomicInteger totalSkipCount;
    private final AtomicInteger totalParseErrorCount;
    private final AtomicInteger totalBatchPutFailCount;
    private final Properties properties;

    public BatchPutIndexInfoJob(AtomicInteger totalImportCount, AtomicInteger totalSkipCount, AtomicInteger totalParseErrorCount, AtomicInteger totalBatchPutFailCount, String filePath, List<String> ttlTypeList, ConcurrentHashMap<String, Long> ttlTypeCountMap, String fileBlock, Properties properties) {
        this.totalImportCount = totalImportCount;
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

        TiSession tiSession = TiSessionUtil.getTiSession(properties);

        String envId = properties.getProperty(Model.ENV_ID);
        String appId = properties.getProperty(Model.APP_ID);
        String mode = properties.getProperty(Model.MODE);
        String scenes = properties.getProperty(Model.SCENES);
        int batchSize = Integer.parseInt(properties.getProperty(Model.BATCH_SIZE));
        int checkSumPercentage = Integer.parseInt(properties.getProperty(Model.CHECK_SUM_PERCENTAGE));
        int isCheckSum = Integer.parseInt(properties.getProperty(Model.ENABLE_CHECK_SUM));

        File file = new File(filePath);
        BufferedReader bufferedReader = null;
        FileInputStream fileInputStream = null;
        BufferedInputStream bufferedInputStream = null;

        try {
            fileInputStream = new FileInputStream(file);
            bufferedInputStream = new BufferedInputStream(fileInputStream);
            bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream, StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        int start = Integer.parseInt(fileBlock.split(",")[0]);
        int todo = Integer.parseInt(fileBlock.split(",")[1]);

        String checkSumFilePath = properties.getProperty(Model.CHECK_SUM_FILE_PATH);
        String fp = checkSumFilePath.replaceAll("\"", "") + "/" + file.getName().replaceAll("\\.", "") + "/" + Thread.currentThread().getId() + ".txt";

        BufferedWriter bufferedWriter = null;
        if (0 != isCheckSum) {
            bufferedWriter = CheckSumUtil.initCheckSumLog(properties, file);
        }

        for (int m = 0; m < start; m++) {
            try {
                assert bufferedReader != null;
                bufferedReader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        int count = 0;
        int totalCount = 0;
        String line;
        String indexInfoKey = null;
        JSONObject jsonObject;
        ConcurrentHashMap<ByteString, ByteString> kvPairs = new ConcurrentHashMap<>();
        RawKVClient rawKVClient = tiSession.createRawClient();
        String checkSumDelimiter;

        Random random = new Random();

        for (int n = 0; n < todo; n++) {
            try {

                count++;
                totalCount++;

                assert bufferedReader != null;
                line = bufferedReader.readLine();
                ByteString key = null;
                ByteString value = null;

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String time = simpleDateFormat.format(new Date());

                switch (mode) {
                    case Model.JSON_FORMAT:
                        checkSumDelimiter = properties.getProperty(Model.CHECK_SUM_DELIMITER);
                        try {
                            jsonObject = JSONObject.parseObject(line);
                        } catch (Exception e) {
                            auditLog.error(String.format("Failed to parse json, file='%s', json='%s',line=%s,", file, line, start + totalCount));
                            totalParseErrorCount.addAndGet(1);
                            // if _todo_ == totalCount in json failed, batch put.
                            count = PutUtil.batchPut(totalCount, todo, count, batchSize, rawKVClient, kvPairs, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);
                            continue;
                        }
                        switch (scenes) {
                            case Model.INDEX_INFO:
                                IndexInfo indexInfoS = JSON.toJavaObject(jsonObject, IndexInfo.class);
                                // Skip the type that exists in the tty type map.
                                if (ttlTypeList.contains(indexInfoS.getType())) {
                                    ttlTypeCountMap.put(indexInfoS.getType(), ttlTypeCountMap.get(indexInfoS.getType()) + 1);
                                    auditLog.warn(String.format("[Skip TTL] [%s] in [%s],line=[%s]", indexInfoKey, file.getAbsolutePath(), start + totalCount));
                                    totalSkipCount.addAndGet(1);
                                    count = PutUtil.batchPut(totalCount, todo, count, batchSize, rawKVClient, kvPairs, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);
                                    continue;
                                }
                                if (envId != null) {
                                    indexInfoKey = String.format(IndexInfo.INDEX_INFO_KET_FORMAT, envId, indexInfoS.getType(), indexInfoS.getId());
                                } else {
                                    indexInfoKey = String.format(IndexInfo.INDEX_INFO_KET_FORMAT, indexInfoS.getEnvId(), indexInfoS.getType(), indexInfoS.getId());
                                }
                                indexInfoS.setUpdateTime(time);
                                // TiKV indexInfo
                                IndexInfo indexInfoT = IndexInfo.initIndexInfoT(indexInfoS, time);
                                key = ByteString.copyFromUtf8(indexInfoKey);
                                value = ByteString.copyFromUtf8(JSONObject.toJSONString(indexInfoT));
                                logger.debug(String.format("[%s], K=%s, V={%s}", file.getAbsolutePath(), indexInfoKey, JSONObject.toJSONString(indexInfoT)));
                                break;
                            case Model.TEMP_INDEX_INFO:
                                TempIndexInfo tempIndexInfos = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                                if (envId != null) {
                                    indexInfoKey = String.format(TempIndexInfo.TEMP_INDEX_INFO_KEY_FORMAT, envId, tempIndexInfos.getId());
                                } else {
                                    indexInfoKey = String.format(TempIndexInfo.TEMP_INDEX_INFO_KEY_FORMAT, tempIndexInfos.getEnvId(), tempIndexInfos.getId());
                                }
                                tempIndexInfos.setAppId(appId);
                                tempIndexInfos.setTargetId(tempIndexInfos.getTargetId());
                                // TiKV tempIndexInfo
                                TempIndexInfo tempIndexInfoT = TempIndexInfo.initTempIndexInfo(tempIndexInfos);
                                key = ByteString.copyFromUtf8(indexInfoKey);
                                value = ByteString.copyFromUtf8(JSONObject.toJSONString(tempIndexInfoT));
                                logger.debug(String.format("[%s], K=%s, V={%s}", file.getAbsolutePath(), indexInfoKey, JSONObject.toJSONString(tempIndexInfoT)));
                                break;
                        }
                        break;
                    case Model.CSV_FORMAT:
                        String delimiter_1 = properties.getProperty(Model.DELIMITER_1);
                        String delimiter_2 = properties.getProperty(Model.DELIMITER_2);
//                        envId = line.split(delimiter_1)[0];
//                        String type = line.split(delimiter_1)[1];
//                        String id = line.split(delimiter_1)[2].split(delimiter_2)[0];
//                        indexInfoS = new IndexInfo();
//                        if (envId != null) {
//                            indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, envId, type, id);
//                        } else {
//                            indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, indexInfoS.getEnvId(), type, id);
//                        }
//                        // TODO
//                        String v = line.split(delimiter_1)[2];
//                        String targetId = v.split(delimiter_2)[0];
//                        serviceTag = new ServiceTag();
//                        serviceTag.setBLKMDL_ID(v.split(delimiter_2)[1]);
//                        serviceTag.setPD_SALE_FTA_CD(v.split(delimiter_2)[2]);
//                        serviceTag.setACCT_DTL_TYPE(v.split(delimiter_2)[3]);
//                        serviceTag.setTu_FLAG(v.split(delimiter_2)[4]);
//                        serviceTag.setCMTRST_CST_ACCNO(v.split(delimiter_2)[5]);
//                        serviceTag.setAR_ID(v.split(delimiter_2)[6]);
//                        serviceTag.setQCRCRD_IND("");
//                        String serviceTagJson = JSON.toJSONString(serviceTag);
//                        IndexInfo indexInfoT = new IndexInfo();
//                        indexInfoT.setAppId(appId);
//                        indexInfoT.setServiceTag(serviceTagJson);
//                        indexInfoT.setTargetId(targetId);
//                        indexInfoT.setUpdateTime(time);
//                        key = ByteString.copyFromUtf8(indexInfoKey);
//                        value = ByteString.copyFromUtf8(JSONObject.toJSONString(indexInfoT));
//                        break;
                    default:
                        logger.error(String.format("Illegal format: %s", mode));
                        return;
                }

                // Sampling data is written into the check sum file
                if (0 != isCheckSum) {
                    int nn = random.nextInt(100 / checkSumPercentage) + 1;
                    if (nn == 1) {
                        bufferedWriter.write(indexInfoKey + checkSumDelimiter + (start + totalCount) + "\n");
                    }
                }

                assert key != null;
                kvPairs.put(key, value);

                count = PutUtil.batchPut(totalCount, todo, count, batchSize, rawKVClient, kvPairs, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, properties);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            if (0 != isCheckSum) {
                bufferedWriter.flush();
                bufferedWriter.close();
            }
            assert bufferedReader != null;
            bufferedReader.close();
            fileInputStream.close();
            bufferedInputStream.close();
            rawKVClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}