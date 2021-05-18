package com.pingcap.importer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.pojo.IndexInfoS;
import com.pingcap.pojo.IndexInfoT;
import com.pingcap.pojo.ServiceTag;
import com.pingcap.util.*;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexInfoS2T {

    private static final Logger logger = Logger.getLogger(IndexInfoS2T.class);

    private static final Properties properties = PropertiesUtil.getProperties();
    private static final String filesPath = properties.getProperty("importer.in.filePath");
    private static final int corePoolSize = Integer.parseInt(properties.getProperty("importer.tikv.corePoolSize"));
    private static final int maxPoolSize = Integer.parseInt(properties.getProperty("importer.tikv.maxPoolSize"));

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        // Traverse all the files that need to be written.
        List<File> fileList = FileUtil.showFileList(filesPath);

        // Start the Main thread for each file.
        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(corePoolSize, maxPoolSize);
        for (File file : fileList) {
            logger.debug(String.format("Start running the Main thread for [%s]", file.getAbsolutePath()));
            // Pass in the file to be processed and the ttl map.
            // The ttl map is shared by all file threads, because it is a table for processing, which is summarized here.
            threadPoolExecutor.execute(new IndexInfoS2TJob(file.getAbsolutePath()));
        }
        threadPoolExecutor.shutdown();

        // When all threads are over, stop the thread pool.
        while (true) {
            if (threadPoolExecutor.isTerminated()) {
                long duration = System.currentTimeMillis() - startTime;
                logger.info(String.format("File import is complete! It takes %s seconds", (duration / 1000)));
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
    private final AtomicInteger totalSkipCount = new AtomicInteger(0);
    private final AtomicInteger totalErrorCount = new AtomicInteger(0);

    public IndexInfoS2TJob(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run() {

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

        ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(insideThread);
        for (String s : threadPerLineList) {
            threadPoolExecutor.execute(new BatchPutIndexInfoJob(totalLineCount, totalSkipCount, totalErrorCount, filePath, ttlTypeList, ttlTypeCountMap, s));
        }

        TimerUtil timerUtil = new TimerUtil(totalLineCount, lines, filePath);
        timerUtil.run();

        threadPoolExecutor.shutdown();

        while (true) {
            if (threadPoolExecutor.isTerminated()) {
                break;
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        StringBuilder result = new StringBuilder("Import Report: File[" + file.getAbsolutePath() + "], Total rows[" + lines + "], Imported rows[" + totalLineCount + "], Skip rows[" + totalSkipCount + "],Error count[" + totalErrorCount + "], Duration[" + duration / 1000 + "s],");
        result.append(" Skip ttl: ");
        assert ttlTypeCountMap != null;
        if (!ttlTypeCountMap.isEmpty()) {
            for (Map.Entry<String, Long> item : ttlTypeCountMap.entrySet()) {
                result.append(item.getKey()).append("=").append(item.getValue()).append(",");
            }
        }
        logger.info(result.toString());

    }
}

class BatchPutIndexInfoJob implements Runnable {

    private static final String INDEX_INFO_KET_FORMAT = "indexInfo_:_%s_:_%s_:_%s";
    private static final String JSON_FORMAT = "json";
    private static final String ORIGINAL_FORMAT = "original";

    private static final Logger logger = Logger.getLogger(BatchPutIndexInfoJob.class);

    private static final Properties properties = PropertiesUtil.getProperties();
    private static final String envId = properties.getProperty("importer.out.envId");
    private static final String appId = properties.getProperty("importer.out.appId");
    private static final String mode = properties.getProperty("importer.in.mode");
    private static final int batchSize = Integer.parseInt(properties.getProperty("importer.tikv.batchSize"));

    private final TiSession tiSession = TiSessionUtil.getTiSession();

    private final String filePath;
    private final List<String> ttlTypeList;
    private final ConcurrentHashMap<String, Long> ttlTypeCountMap;
    private final String fileBlock;
    private final AtomicInteger totalLineCount;
    private final AtomicInteger totalSkipCount;
    private final AtomicInteger totalErrorCount;

    public BatchPutIndexInfoJob(AtomicInteger totalLineCount, AtomicInteger totalSkipCount, AtomicInteger totalErrorCount, String filePath, List<String> ttlTypeList, ConcurrentHashMap<String, Long> ttlTypeCountMap, String fileBlock) {
        this.totalLineCount = totalLineCount;
        this.totalSkipCount = totalSkipCount;
        this.totalErrorCount = totalErrorCount;
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
        ConcurrentHashMap<ByteString, ByteString> kvPairs = new ConcurrentHashMap<>();
        RawKVClient rawKVClient = tiSession.createRawClient();

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
                            indexInfoT.setAppId(appId);
                            indexInfoT.setTargetId(indexInfoS.getTargetId());
                            if (StringUtils.isNotBlank(indexInfoS.getServiceTag())) {
                                indexInfoT.setServiceTag(indexInfoS.getServiceTag());
                            }
                            indexInfoT.setUpdateTime(indexInfoS.getCreateTime().replaceAll("T", " ").replaceAll("Z", ""));
                            logger.debug(String.format(" File: %s - Thread - %s , K: {%s}, V: {%s}", file.getAbsolutePath(), Thread.currentThread().getId(), indexInfoKey, JSONObject.toJSONString(indexInfoT)));

                            key = ByteString.copyFromUtf8(indexInfoKey);
                            value = ByteString.copyFromUtf8(JSONObject.toJSONString(indexInfoT));
                        } catch (Exception e) {
                            logger.error(String.format("Failed to parse json, file='%s', line=%s, json='%s'", file, start + count, line));
                            totalErrorCount.addAndGet(1);
                            PutUtil.batchPut(totalCount, todo, count, batchSize, rawKVClient, kvPairs, file, totalLineCount, totalSkipCount, start + count);
                            continue;
                        }
                        break;
                    case ORIGINAL_FORMAT:

                        String envId = line.split("\\|")[0];
                        String type = line.split("\\|")[1];
                        String targetId = line.split("\\|")[2].split("##")[0];

                        indexInfoS = new IndexInfoS();
                        indexInfoS.setType(type);

                        if (envId != null) {
                            indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, envId, type, targetId);
                        } else {
                            indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, indexInfoS.getEnvId(), indexInfoS.getType(), indexInfoS.getId());
                        }

                        // TODO
                        String st = line.split("\\|")[2];
                        serviceTag = new ServiceTag();
                        serviceTag.setBLKMDL_ID(st.split("##")[1]);
                        serviceTag.setPD_SALE_FTA_CD(st.split("##")[2]);
                        serviceTag.setACCT_DTL_TYPE(st.split("##")[3]);
                        serviceTag.setTu_FLAG(st.split("##")[4]);
                        serviceTag.setCMTRST_CST_ACCNO(st.split("##")[5]);
                        serviceTag.setAR_ID(st.split("##")[6]);
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

                // Skip the type that exists in the tty type map.
                if (ttlTypeList.contains(indexInfoS.getType())) {
                    ttlTypeCountMap.put(indexInfoS.getType(), ttlTypeCountMap.get(indexInfoS.getType()) + 1);
                    logger.warn(String.format("Skip key - ttl: %s in '%s',line = %s", indexInfoKey, file.getAbsolutePath(), start + count));
                    // TODO
                    rawKVClient.delete(ByteString.copyFromUtf8(indexInfoKey));
                    totalSkipCount.addAndGet(1);
                    continue;
                } else {
                    kvPairs.put(key, value);
                }
                count = PutUtil.batchPut(totalCount, todo, count, batchSize, rawKVClient, kvPairs, file, totalLineCount, totalSkipCount, start + count);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}