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
import java.util.concurrent.ThreadPoolExecutor;

public class IndexInfoS2TSingle {

    private static final Logger logger = Logger.getLogger(IndexInfoS2T.class);

    private static final Properties properties = PropertiesUtil.getProperties();
    private static final String importFilesPath_indexInfo = properties.getProperty("importer.in.importFilesPath_indexInfo");
    private static final int corePoolSize = Integer.parseInt(properties.getProperty("importer.tikv.corePoolSize"));
    private static final int maxPoolSize = Integer.parseInt(properties.getProperty("importer.tikv.maxPoolSize"));

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        logger.info(String.format("Welcome to TiKV importer! Properties: %s", properties));
        List<File> fileList = FileUtil.loadDirectory(new File(importFilesPath_indexInfo));
        logger.info("--------------------------------------------------------------------------------> Need to import the following files.");
        assert fileList != null;
        if (fileList.isEmpty()) {
            logger.error(String.format("This filePath [%s] has no file.", importFilesPath_indexInfo));
        } else {
            for (File file : fileList) {
                int line = FileUtil.getFileLines(file);
                logger.info(String.format(" [FILE] '%s' ---- [TOTAL LINE] %d.", file.getAbsolutePath(), line));
            }
        }
        logger.info(String.format("<-------------------------------------------------------------------------------- [TOTAL FILE] %s.", fileList.size()));

        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(corePoolSize, maxPoolSize);
        for (int i = 0; i < fileList.size(); i++) {
            threadPoolExecutor.execute(new IndexInfoS2TSJob(fileList));
        }
        threadPoolExecutor.shutdown();

        while (true) {
            if (threadPoolExecutor.isTerminated()) {
                long duration = System.currentTimeMillis() - startTime;
                logger.info(String.format("[TOTAL RESULT] All Thread had finished! Total duration -> [%ss]", duration / 1000));
                System.exit(0);
            }
        }
    }

}

class IndexInfoS2TSJob implements Runnable {

    private static final Logger logger = Logger.getLogger(IndexInfoS2TJob.class);
    private static final String INDEX_INFO_KET_FORMAT = "indexInfo_:_%s_:_%s_:_%s";

    private static final Properties properties = PropertiesUtil.getProperties();

    private static final TiSession tiSession = TiSessionUtil.getTiSession();

    private static final int batchSize = Integer.parseInt(properties.getProperty("importer.tikv.batchSize"));

    private static final String envId = properties.getProperty("importer.out.envId");
    private static final String appId = properties.getProperty("importer.out.appId");
    private static final String ttlType = properties.getProperty("importer.ttl.type");

    private final List<File> fileList;
    private static Integer num = 0;

    public IndexInfoS2TSJob(List<File> fileList) {
        this.fileList = fileList;
    }

    @Override
    public void run() {

        long startTime = System.currentTimeMillis();

        List<String> ttlTypeList = new ArrayList<>(Arrays.asList(ttlType.split(",")));
        ConcurrentHashMap<String, Long> ttlTypeCountMap = FileUtil.getTtlTypeMap(ttlTypeList);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time = simpleDateFormat.format(new Date());

        BufferedInputStream bufferedInputStream = null;
        File file = null;

        synchronized (IndexInfoS2TJob.class) {
            try {
                file = fileList.get(num);
                logger.info(String.format("[%s] will process No.[%s] file -- { %s }", Thread.currentThread().getName(), num, file.getAbsolutePath()));
                bufferedInputStream = new BufferedInputStream(new FileInputStream(fileList.get(num)));
                num++;
            } catch (FileNotFoundException e) {
                logger.error(String.format("Load file [ %s ] failed!", file), e);
                e.printStackTrace();
            }
        }

        assert bufferedInputStream != null;
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream, StandardCharsets.UTF_8));
        RawKVClient rawKVClient = tiSession.createRawClient();

        JSONObject jsonObject;
        IndexInfoS indexInfoS;

        String line;
        int count = 0;
        int skipCount = 0;
        HashMap<ByteString, ByteString> kvPairs = new HashMap<>();

        try {
            while ((line = bufferedReader.readLine()) != null) {
                if (StringUtils.isBlank(line)) {
                    continue;
                }
                try {
                    jsonObject = JSONObject.parseObject(line);
                    indexInfoS = JSON.toJavaObject(jsonObject, IndexInfoS.class);
                    indexInfoS.setCreateTime(indexInfoS.getCreateTime().replaceAll("Z", " ").replaceAll("T", ""));
                } catch (Exception e) {
                    logger.error(String.format(" Parse file [ %s ] failed!", file.getAbsolutePath()));
                    return;
                }
                if (ttlTypeList.contains(indexInfoS.getType())) {
                    ttlTypeCountMap.put(indexInfoS.getType(), ttlTypeCountMap.get(indexInfoS.getType()) + 1);
                    skipCount++;
                    continue;
                } else {
                    IndexInfoT indexInfoT = new IndexInfoT();
                    indexInfoT.setAppId(appId);
                    if (StringUtils.isNotBlank(indexInfoS.getServiceTag())) {
                        indexInfoT.setServiceTag(indexInfoS.getServiceTag());
                    }
                    indexInfoT.setTargetId(indexInfoS.getTargetId());
                    indexInfoT.setUpdateTime(time); // time?
                    String indexInfoKey;
                    if (envId != null) {
                        indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, envId, indexInfoS.getType(), indexInfoS.getId());
                    } else {
                        indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, indexInfoS.getEnvId(), indexInfoS.getType(), indexInfoS.getId());
                    }
                    // logger.info(String.format("K -> {%s}, V -> {%s}", indexInfoKey, JSONObject.toJSONString(indexInfoT)));
                    ByteString key = ByteString.copyFromUtf8(indexInfoKey);
                    ByteString value = ByteString.copyFromUtf8(JSONObject.toJSONString(indexInfoT));
                    kvPairs.put(key, value);
                    count++;
                }

                if (count % batchSize == 0) {
                    for (Map.Entry<ByteString, ByteString> item : kvPairs.entrySet()) {
                        rawKVClient.delete(item.getKey());
                    }
                    String k;
                    for (Iterator<Map.Entry<ByteString, ByteString>> iterator = kvPairs.entrySet().iterator(); iterator.hasNext(); ) {
                        Map.Entry<ByteString, ByteString> item = iterator.next();
                        k = item.getKey().toStringUtf8();
                        if (!rawKVClient.get(item.getKey()).isEmpty()) {
                            iterator.remove();
                            count--;
                            skipCount++;
                            logger.warn(String.format("Skip key [ %s ], file is [ %s ]", k, file.getAbsolutePath()));
                        }
                    }
                    if (!kvPairs.isEmpty()) {
                        try {
                            rawKVClient.batchPut(kvPairs);
                        } catch (Exception e) {
                            logger.error(String.format("Batch put Tikv failed, file is [ %s ]", file.getAbsolutePath()), e);
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        long duration = System.currentTimeMillis() - startTime;
        StringBuilder result = new StringBuilder("[Import result - SUCCESS] Write file [" + file.getAbsolutePath() + "] success! Total count = " + count + ", skip count = " + skipCount + ", duration = " + duration + "ms. ");
        for (Map.Entry<String, Long> item : ttlTypeCountMap.entrySet()) {
            result.append(" | Skip ttl type: ").append(item.getKey()).append(", count: ").append(item.getValue()).append(" | ");
        }
        logger.info(result.toString());

    }

}