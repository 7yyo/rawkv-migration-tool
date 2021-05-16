package com.pingcap.importer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.pojo.IndexInfoS;
import com.pingcap.pojo.IndexInfoT;
import com.pingcap.util.*;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import shade.com.google.protobuf.ByteString;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexInfoS2T {

    private static final Logger logger = Logger.getLogger(IndexInfoS2T.class);

    private static final Properties properties = PropertiesUtil.getProperties();
    private static final String importFilesPath_indexInfo = properties.getProperty("importer.in.importFilesPath_indexInfo");
    private static final int corePoolSize = Integer.parseInt(properties.getProperty("importer.tikv.corePoolSize"));
    private static final int maxPoolSize = Integer.parseInt(properties.getProperty("importer.tikv.maxPoolSize"));

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        // Traverse all the files that need to be written.
        List<File> fileList = FileUtil.showFileList(importFilesPath_indexInfo);

        // Start the Main thread for each file.
        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(corePoolSize, maxPoolSize);
        for (File file : fileList) {
            logger.info(String.format(" Start running the Main thread of [%s]", file.getAbsolutePath()));
            // Pass in the file to be processed and the ttl map.
            // The ttl map is shared by all file threads, because it is a table for processing, which is summarized here.
            threadPoolExecutor.execute(new IndexInfoS2TJob(file.getAbsolutePath()));
        }
        threadPoolExecutor.shutdown();

        // When all threads are over, stop the thread pool.
        while (true) {
            if (threadPoolExecutor.isTerminated()) {
                long duration = System.currentTimeMillis() - startTime;
                logger.info(String.format(" File import is complete. It takes %s seconds", (duration / 1000)));
                System.exit(0);
            }
        }

    }
}

class IndexInfoS2TJob implements Runnable {

    private static final Logger logger = Logger.getLogger(IndexInfoS2TJob.class);

    private static final Properties properties = PropertiesUtil.getProperties();
    private static final int insideThread = Integer.parseInt(properties.getProperty("importer.tikv.insideThread"));
    private static final String ttlType = properties.getProperty("importer.ttl.type");

    private final String filePath;

    private final AtomicInteger totalLineCount = new AtomicInteger(0);

    public IndexInfoS2TJob(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run() {

        long startTime = System.currentTimeMillis();

        // Generate ttl type map.
        List<String> ttlTypeList = new ArrayList<>(Arrays.asList(ttlType.split(",")));
        ConcurrentHashMap<String, Long> ttlTypeCountMap = FileUtil.getTtlTypeMap(ttlTypeList);

        // Start the file sub-thread,
        // import the data of the file through the sub-thread, and divide the data in advance according to the number of sub-threads.
        // -1 for remainder
        File file = new File(filePath);
        int lines = FileUtil.getFileLines(file);
        List<String> threadPerLineList = CountUtil.getPerThreadFileLines(lines, insideThread, file.getAbsolutePath());

        ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(insideThread);
        for (String s : threadPerLineList) {
            threadPoolExecutor.execute(new BatchPutIndexInfoJob(totalLineCount, filePath, ttlTypeList, ttlTypeCountMap, s));
        }
        threadPoolExecutor.shutdown();

        while (true) {
            if (threadPoolExecutor.isTerminated()) {
                break;
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        StringBuilder result = new StringBuilder("\n**************************************************************\n[Import Report] \nFile= '" + file.getAbsolutePath() + "'. \nTotal number of rows: " + lines + " | Imported number of rows: " + totalLineCount + " | Skip rows: " + 11111 + " | Duration: " + duration / 1000 + "s.\n");
        result.append("Skip TTL Type: ");
        for (Map.Entry<String, Long> item : ttlTypeCountMap.entrySet()) {
            result.append(item.getKey()).append("=").append(item.getValue()).append(" | ");
        }
        result.append("\n**************************************************************");
        logger.info(result.toString());

    }
}

class BatchPutIndexInfoJob implements Runnable {

    private static final String INDEX_INFO_KET_FORMAT = "indexInfo_:_%s_:_%s_:_%s";

    private static final Logger logger = Logger.getLogger(BatchPutIndexInfoJob.class);

    private static final Properties properties = PropertiesUtil.getProperties();
    private static final String envId = properties.getProperty("importer.out.envId");
    private static final String appId = properties.getProperty("importer.out.appId");
    private static final int batchSize = Integer.parseInt(properties.getProperty("importer.tikv.batchSize"));

    private final TiSession tiSession = TiSessionUtil.getTiSession();

    private final String filePath;
    private final List<String> ttlTypeList;
    private final ConcurrentHashMap<String, Long> ttlTypeCountMap;
    private final String fileBlock;
    private final AtomicInteger totalLineCount;

    public BatchPutIndexInfoJob(AtomicInteger totalLineCount, String filePath, List<String> ttlTypeList, ConcurrentHashMap<String, Long> ttlTypeCountMap, String fileBlock) {
        this.totalLineCount = totalLineCount;
        this.filePath = filePath;
        this.ttlTypeList = ttlTypeList;
        this.ttlTypeCountMap = ttlTypeCountMap;
        this.fileBlock = fileBlock;
    }

    @Override
    public void run() {

        File file = new File(filePath);
        BufferedReader bufferedReader = null;

        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(file)), StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        int start = Integer.parseInt(fileBlock.split(",")[0]);
        int todo = Integer.parseInt(fileBlock.split(",")[1]);

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
        IndexInfoS indexInfoS;
        HashMap<ByteString, ByteString> kvPairs = new HashMap<>();
        RawKVClient rawKVClient = tiSession.createRawClient();

        for (int n = 0; n < todo; n++) {
            try {

                assert bufferedReader != null;
                line = bufferedReader.readLine();
//                logger.error("Original == " + line);

                jsonObject = JSONObject.parseObject(line);

                indexInfoS = JSON.toJavaObject(jsonObject, IndexInfoS.class);

                if (envId != null) {
                    indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, envId, indexInfoS.getType(), indexInfoS.getId());
                } else {
                    indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, indexInfoS.getEnvId(), indexInfoS.getType(), indexInfoS.getId());
                }

                count++;
                totalCount++;

                // Skip the type that exists in the tty type map.
                if (ttlTypeList.contains(indexInfoS.getType())) {
                    ttlTypeCountMap.put(indexInfoS.getType(), ttlTypeCountMap.get(indexInfoS.getType()) + 1);
                    logger.error(String.format(" Skip key: %s in '%s'", indexInfoKey, file.getAbsolutePath()));
                    continue;
                } else {
                    IndexInfoT indexInfoT = new IndexInfoT();
                    indexInfoT.setAppId(appId);
                    indexInfoT.setTargetId(indexInfoS.getTargetId());
                    if (StringUtils.isNotBlank(indexInfoS.getServiceTag())) {
                        indexInfoT.setServiceTag(indexInfoS.getServiceTag());
                    }
                    indexInfoT.setUpdateTime(indexInfoS.getCreateTime().replaceAll("Z", " ").replaceAll("T", ""));
                    logger.debug(String.format(" File: %s - Thread - %s , K: {%s}, V: {%s}", file.getAbsolutePath(), Thread.currentThread().getId(), indexInfoKey, JSONObject.toJSONString(indexInfoT)));

                    ByteString key = ByteString.copyFromUtf8(indexInfoKey);
                    ByteString value = ByteString.copyFromUtf8(JSONObject.toJSONString(indexInfoT));

                    kvPairs.put(key, value);
                }

                if (totalCount == todo || count == batchSize) {
                    // TODO
                    for (Map.Entry<ByteString, ByteString> item : kvPairs.entrySet()) {
                        rawKVClient.delete(item.getKey());
                    }
                    String k;
                    for (Iterator<Map.Entry<ByteString, ByteString>> iterator = kvPairs.entrySet().iterator(); iterator.hasNext(); ) {
                        Map.Entry<ByteString, ByteString> item = iterator.next();
                        k = item.getKey().toStringUtf8();
                        // If the key already exists, do not insert.
                        if (!rawKVClient.get(item.getKey()).isEmpty()) {
                            iterator.remove();
                            totalLineCount.addAndGet(-1);
//                            logger.error(String.format("Skip key [ %s ], file is [ %s ]", k, file.getAbsolutePath()));
                        }
                    }
//                    for (Map.Entry<ByteString, ByteString> item : kvPairs.entrySet()) {
//                        logger.error("Map === key=" + item.getKey().toStringUtf8() + "||||" + "value=" + item.getValue());
//                    }
                    BatchPutUtil.batchPut(kvPairs, rawKVClient, file);
                    totalLineCount.addAndGet(kvPairs.size());
                    kvPairs.clear();
//                    System.out.println("batch put");
                    count = 0;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}