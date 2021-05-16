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
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class IndexInfoS2T {

    private static final Logger logger = Logger.getLogger(IndexInfoS2T.class);

    private static final Properties properties = PropertiesUtil.getProperties();
    private static final String importFilesPath_indexInfo = properties.getProperty("importer.in.importFilesPath_indexInfo");
    private static final String ttlType = properties.getProperty("importer.ttl.type");
    private static final int corePoolSize = Integer.parseInt(properties.getProperty("importer.tikv.corePoolSize"));
    private static final int maxPoolSize = Integer.parseInt(properties.getProperty("importer.tikv.maxPoolSize"));

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        // Traverse all the files that need to be written.
        List<File> fileList = FileUtil.showFileList(importFilesPath_indexInfo);

        // Generate ttl type map.
        List<String> ttlTypeList = new ArrayList<>(Arrays.asList(ttlType.split(",")));
        ConcurrentHashMap<String, Long> ttlTypeCountMap = FileUtil.getTtlTypeMap(ttlTypeList);

        // Start the main thread for each file.
        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(corePoolSize, maxPoolSize);
        for (File file : fileList) {
            logger.info(String.format(" Start running the main thread of [%s]", file.getAbsolutePath()));
            // Pass in the file to be processed and the ttl map.
            // The ttl map is shared by all file threads, because it is a table for processing, which is summarized here.
            threadPoolExecutor.execute(new IndexInfoS2TJob(file, ttlTypeList, ttlTypeCountMap));
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

    private final File file;
    private final List<String> ttlTypeList;
    private final ConcurrentHashMap<String, Long> ttlTypeCountMap;

    private static final AtomicInteger totalLineCount = new AtomicInteger(0);

    public IndexInfoS2TJob(File file, List<String> ttlTypeList, ConcurrentHashMap<String, Long> ttlTypeCountMap) {
        this.file = file;
        this.ttlTypeList = ttlTypeList;
        this.ttlTypeCountMap = ttlTypeCountMap;
    }

    @Override
    public void run() {

        long startTime = System.currentTimeMillis();

        // Start the file sub-thread,
        // import the data of the file through the sub-thread, and divide the data in advance according to the number of sub-threads.
        // -1 for remainder
        int lines = FileUtil.getFileLines(file);
        List<String> threadPerLineList = CountUtil.getPerThreadFileLines(lines, insideThread - 1, file.getAbsolutePath());

        ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(insideThread);
        for (String s : threadPerLineList) {
            threadPoolExecutor.execute(new BatchPutIndexInfoJob(totalLineCount, file, ttlTypeList, ttlTypeCountMap, s));
        }
        threadPoolExecutor.shutdown();

        while (true) {
            if (threadPoolExecutor.isTerminated()) {
                break;
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        StringBuilder result = new StringBuilder("\n**************************************************************\n[[[[[[[ Import report ]]]]]]] \nFile= '" + file.getAbsolutePath() + "'. \nTotal number of rows: " + lines + " | Imported number of rows: " + totalLineCount + " | Skip rows: " + 11111 + " | Duration: " + duration / 1000 + "s.\n");
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

    private static final TiSession tiSession = TiSessionUtil.getTiSession();

    private final File file;
    private final List<String> ttlTypeList;
    private final ConcurrentHashMap<String, Long> ttlTypeCountMap;
    private final String fileBlock;
    private final AtomicInteger totalLineCount;

    public BatchPutIndexInfoJob(AtomicInteger totalLineCount, File file, List<String> ttlTypeList, ConcurrentHashMap<String, Long> ttlTypeCountMap, String fileBlock) {
        this.totalLineCount = totalLineCount;
        this.file = file;
        this.ttlTypeList = ttlTypeList;
        this.ttlTypeCountMap = ttlTypeCountMap;
        this.fileBlock = fileBlock;
    }

    @Override
    public void run() {

        BufferedInputStream bufferedInputStream = null;
        BufferedReader bufferedReader = null;

        try {
            bufferedInputStream = new BufferedInputStream(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            logger.error(String.format(" Failed to read file %s.", file), e);
            e.printStackTrace();
        }

        if (bufferedInputStream != null) {
            bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream, StandardCharsets.UTF_8));
        }

        JSONObject jsonObject;
        IndexInfoS indexInfoS;
        String line;
        String indexInfoKey;
        String k;

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time = simpleDateFormat.format(new Date());

        int start = Integer.parseInt(fileBlock.split(",")[0]);
        int todo = Integer.parseInt(fileBlock.split(",")[1]);

        if (bufferedReader != null) {

            // Adjust the reading range to the correct position.
            for (int j = 0; j < start; j++) {
                try {
                    bufferedReader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {

                RawKVClient rawKVClient = tiSession.createRawClient();

                int totalCount = 0;
                HashMap<ByteString, ByteString> kvPairs = new HashMap<>();

                while ((line = bufferedReader.readLine()) != null) {

                    if (StringUtils.isBlank(line)) {
                        continue;
                    }

                    try {

                        jsonObject = JSONObject.parseObject(line);
                        indexInfoS = JSON.toJavaObject(jsonObject, IndexInfoS.class);
                        indexInfoS.setCreateTime(indexInfoS.getCreateTime().replaceAll("Z", " ").replaceAll("T", ""));

                    } catch (Exception e) {
                        logger.error(String.format("%s Failed to parse %s, the failure json is %s", Thread.currentThread().getId(), file.getAbsolutePath(), line), e);
                        break;
                    }

                    if (envId != null) {
                        indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, envId, indexInfoS.getType(), indexInfoS.getId());
                    } else {
                        indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, indexInfoS.getEnvId(), indexInfoS.getType(), indexInfoS.getId());
                    }
                    // Skip the type that exists in the tty type map.
                    if (ttlTypeList.contains(indexInfoS.getType())) {
                        ttlTypeCountMap.put(indexInfoS.getType(), ttlTypeCountMap.get(indexInfoS.getType()) + 1);
                        logger.warn(String.format(" Skip key: %s in '%s'", indexInfoKey, file.getAbsolutePath()));
                        continue;
                    } else {
                        IndexInfoT indexInfoT = new IndexInfoT();
                        indexInfoT.setAppId(appId);
                        if (StringUtils.isNotBlank(indexInfoS.getServiceTag())) {
                            indexInfoT.setServiceTag(indexInfoS.getServiceTag());
                        }
                        indexInfoT.setTargetId(indexInfoS.getTargetId());
                        indexInfoT.setUpdateTime(time); // time?
                        logger.debug(String.format(" File: %s - Thread - %s , K: {%s}, V: {%s}", file.getAbsolutePath(), Thread.currentThread().getId(), indexInfoKey, JSONObject.toJSONString(indexInfoT)));
                        ByteString key = ByteString.copyFromUtf8(indexInfoKey);
                        ByteString value = ByteString.copyFromUtf8(JSONObject.toJSONString(indexInfoT));
                        kvPairs.put(key, value);
                        totalLineCount.addAndGet(1);
                    }

                    totalCount++;

                    if (totalCount % batchSize == 0 || totalCount == todo) {
                        // TODO
                        for (Map.Entry<ByteString, ByteString> item : kvPairs.entrySet()) {
                            rawKVClient.delete(item.getKey());
                        }
                        for (Iterator<Map.Entry<ByteString, ByteString>> iterator = kvPairs.entrySet().iterator(); iterator.hasNext(); ) {
                            Map.Entry<ByteString, ByteString> item = iterator.next();
                            k = item.getKey().toStringUtf8();
                            // If the key already exists, do not insert.
                            if (!rawKVClient.get(item.getKey()).isEmpty()) {
                                iterator.remove();
                                totalLineCount.addAndGet(-1);
                                logger.warn(String.format("Skip key [ %s ], file is [ %s ]", k, file.getAbsolutePath()));
                            }
                        }
                        BatchPutUtil.batchPut(kvPairs, rawKVClient, file);
                        // All data import ends, break.
                        if (totalCount == todo) {
                            break;
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}