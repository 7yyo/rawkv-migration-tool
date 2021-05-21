package com.pingcap.importer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.pojo.IndexInfoS;
import com.pingcap.pojo.IndexInfoT;
import com.pingcap.pojo.ServiceTag;
import com.pingcap.util.*;
import org.apache.commons.lang.StringUtils;
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
import java.util.concurrent.atomic.AtomicInteger;

public class IndexInfo2T {

    private static final Logger logger = LoggerFactory.getLogger("logBackLog");

    public static void RunIndexInfo2T(Properties properties) {

        String filesPath = properties.getProperty("importer.in.filePath");
        int corePoolSize = Integer.parseInt(properties.getProperty("importer.tikv.corePoolSize"));
        int maxPoolSize = Integer.parseInt(properties.getProperty("importer.tikv.maxPoolSize"));

        long startTime = System.currentTimeMillis();

        // Traverse all the files that need to be written.
        List<File> fileList = FileUtil.showFileList(filesPath, false);

        FileUtil.deleteFolder(properties.getProperty("importer.tikv.checkSumFilePath"));
        FileUtil.deleteFolders(properties.getProperty("importer.tikv.checkSumFilePath"));

        // Start the Main thread for each file.showFileList
        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(corePoolSize, maxPoolSize, "");
        for (File file : fileList) {
            // Pass in the file to be processed and the ttl map.
            // The ttl map is shared by all file threads, because it is a table for processing, which is summarized here.
            threadPoolExecutor.execute(new IndexInfo2TJob(file.getAbsolutePath(), properties));
        }
        threadPoolExecutor.shutdown();

        while (true) {
            if (threadPoolExecutor.isTerminated()) {
                long duration = System.currentTimeMillis() - startTime;
                logger.info(String.format("All files import is complete! It takes [%s] seconds", (duration / 1000)));
                System.exit(0);
            }
        }

    }
}

class IndexInfo2TJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger("logBackLog");

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

        int insideThread = Integer.parseInt(properties.getProperty("importer.tikv.insideThread"));
        String ttlType = properties.getProperty("importer.ttl.type");

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

        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(insideThread, insideThread, file.getName());
        for (String s : threadPerLineList) {
            threadPoolExecutor.execute(new BatchPutIndexInfoJob(totalImportCount, totalSkipCount, totalParseErrorCount, totalBatchPutFailCount, filePath, ttlTypeList, ttlTypeCountMap, s, properties));
        }

        TimerUtil timerUtil = new TimerUtil(totalImportCount, lines, filePath, properties);
        timerUtil.start();

        threadPoolExecutor.shutdown();

        while (true) {
            if (threadPoolExecutor.isTerminated()) {
                break;
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        StringBuilder result = new StringBuilder("Import Report: File[" + file.getAbsolutePath() + "], TotalRows[" + lines + "], ImportedRows[" + totalImportCount + "], SkipRows[" + totalSkipCount + "], ParseERROR[" + totalParseErrorCount + "], BatchPutERROR[" + totalBatchPutFailCount + "], Duration[" + duration / 1000 + "s],");
        result.append(" Skip type=");
        assert ttlTypeCountMap != null;
        if (!ttlTypeCountMap.isEmpty()) {
            for (Map.Entry<String, Long> item : ttlTypeCountMap.entrySet()) {
                result.append("<").append(item.getKey()).append(">").append("[").append(item.getValue()).append("]");
            }
        }
        logger.info(result.toString());
    }
}

class BatchPutIndexInfoJob implements Runnable {

    private static final String INDEX_INFO_KET_FORMAT = "indexInfo_:_%s_:_%s_:_%s";
    private static final String JSON_FORMAT = "json";
    private static final String ORIGINAL_FORMAT = "original";

    private static final Logger logger = LoggerFactory.getLogger("logBackLog");
    private static final Logger auditLog = LoggerFactory.getLogger("auditLog");

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

        String envId = properties.getProperty("importer.out.envId");
        String appId = properties.getProperty("importer.out.appId");
        String mode = properties.getProperty("importer.in.mode");
        int batchSize = Integer.parseInt(properties.getProperty("importer.tikv.batchSize"));
        String delimiter_1 = properties.getProperty("importer.in.delimiter_1");
        String delimiter_2 = properties.getProperty("importer.in.delimiter_2");
        int checkSumCount = Integer.parseInt(properties.getProperty("importer.tikv.checkSumPercentage"));

        File file = new File(filePath);
        BufferedReader bufferedReader = null;

        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(file)), StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        int start = Integer.parseInt(fileBlock.split(",")[0]);
        int todo = Integer.parseInt(fileBlock.split(",")[1]);
        if (todo == 0) {
            return;
        }

        String checkSumFilePath = properties.getProperty("importer.tikv.checkSumFilePath");
        String fp = checkSumFilePath.replaceAll("\"", "") + "/" + file.getName().replaceAll("\\.", "") + "/" + Thread.currentThread().getId() + ".txt";
        File checkSumFile = new File(fp);

        BufferedWriter bufferedWriter = CheckSumUtil.initCheckSumLog(properties, file, checkSumFile);

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
        String indexInfoKey;
        JSONObject jsonObject;
        ConcurrentHashMap<ByteString, ByteString> kvPairs = new ConcurrentHashMap<>();
        RawKVClient rawKVClient = tiSession.createRawClient();
        String checkSumDelimiter = "";

        for (int n = 0; n < todo; n++) {
            try {

                count++;
                totalCount++;

                assert bufferedReader != null;
                line = bufferedReader.readLine();

                IndexInfoS indexInfoS;
                ServiceTag serviceTag;
                ByteString key;
                ByteString value;

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String time = simpleDateFormat.format(new Date());

                switch (mode) {
                    case JSON_FORMAT:
                        try {
                            jsonObject = JSONObject.parseObject(line);
                            indexInfoS = JSON.toJavaObject(jsonObject, IndexInfoS.class);
                            indexInfoS.setUpdateTime(time);
                            if (envId != null) {
                                indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, envId, indexInfoS.getType(), indexInfoS.getId());
                            } else {
                                indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, indexInfoS.getEnvId(), indexInfoS.getType(), indexInfoS.getId());
                            }
                            IndexInfoT indexInfoT = new IndexInfoT();
                            indexInfoT.setTargetId(indexInfoS.getTargetId());
                            if (StringUtils.isNotBlank(indexInfoS.getServiceTag())) {
                                indexInfoT.setServiceTag(indexInfoS.getServiceTag());
                            }
                            indexInfoT.setUpdateTime(time);
                            indexInfoT.setCreateTime(indexInfoS.getCreateTime().replaceAll("T", " ").replaceAll("Z", ""));
                            logger.debug(String.format(" File: %s - Thread - %s , K: {%s}, V: {%s}", file.getAbsolutePath(), Thread.currentThread().getId(), indexInfoKey, JSONObject.toJSONString(indexInfoT)));

                            checkSumDelimiter = properties.getProperty("importer.tikv.checkSumDelimiter");

                            key = ByteString.copyFromUtf8(indexInfoKey);
                            value = ByteString.copyFromUtf8(JSONObject.toJSONString(indexInfoT));

                        } catch (Exception e) {
                            logger.error(String.format("Failed to parse json, file='%s', json='%s',line=%s,", file, line, start + totalCount));
                            bufferedWriter.write("PARSE ERROR line = [" + line + "]" + checkSumDelimiter + (start + totalCount) + "\n");
                            totalParseErrorCount.addAndGet(1);
                            // if todo_ == totalCount is json failed, batch put
                            count = PutUtil.batchPut(totalCount, todo, count, batchSize, rawKVClient, kvPairs, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, mode);
                            continue;

                        }
                        break;

                    case ORIGINAL_FORMAT:

                        envId = line.split(delimiter_1)[0];
                        String type = line.split(delimiter_1)[1];
                        String id = line.split(delimiter_1)[2].split(delimiter_2)[0];

                        indexInfoS = new IndexInfoS();

                        if (envId != null) {
                            indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, envId, type, id);
                        } else {
                            indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, indexInfoS.getEnvId(), type, id);
                        }

                        // TODO
                        String v = line.split(delimiter_1)[2];
                        String targetId = v.split(delimiter_2)[0];
                        serviceTag = new ServiceTag();
                        serviceTag.setBLKMDL_ID(v.split(delimiter_2)[1]);
                        serviceTag.setPD_SALE_FTA_CD(v.split(delimiter_2)[2]);
                        serviceTag.setACCT_DTL_TYPE(v.split(delimiter_2)[3]);
                        serviceTag.setTu_FLAG(v.split(delimiter_2)[4]);
                        serviceTag.setCMTRST_CST_ACCNO(v.split(delimiter_2)[5]);
                        serviceTag.setAR_ID(v.split(delimiter_2)[6]);
                        serviceTag.setQCRCRD_IND("");

                        String serviceTagJson = JSON.toJSONString(serviceTag);

                        IndexInfoT indexInfoT = new IndexInfoT();
                        indexInfoT.setAppId(appId);
                        indexInfoT.setServiceTag(serviceTagJson);
                        indexInfoT.setTargetId(targetId);
                        indexInfoT.setUpdateTime(time);

                        key = ByteString.copyFromUtf8(indexInfoKey);
                        value = ByteString.copyFromUtf8(JSONObject.toJSONString(indexInfoT));

                        break;
                    default:
                        logger.error(String.format("Illegal format: %s", mode));
                        return;
                }

                // Sampling data is written into the check sum file
                if (totalCount % checkSumCount == 0) {
                    bufferedWriter.write(indexInfoKey + checkSumDelimiter + (start + totalCount) + "\n");
                }

                // Skip the type that exists in the tty type map.
                if (ttlTypeList.contains(indexInfoS.getType())) {
                    ttlTypeCountMap.put(indexInfoS.getType(), ttlTypeCountMap.get(indexInfoS.getType()) + 1);
                    auditLog.warn(String.format("Skip key - ttl: %s in '%s',line = %s", indexInfoKey, file.getAbsolutePath(), start + totalCount));
                    totalSkipCount.addAndGet(1);
                    continue;
                } else {
                    kvPairs.put(key, value);
                }

                count = PutUtil.batchPut(totalCount, todo, count, batchSize, rawKVClient, kvPairs, file, totalImportCount, totalSkipCount, totalBatchPutFailCount, start + totalCount, mode);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        CheckSumUtil.checkSumIndexInfo(fp, checkSumDelimiter, tiSession, file, mode, properties);

    }

}