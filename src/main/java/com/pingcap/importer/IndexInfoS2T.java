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
import java.util.concurrent.ThreadPoolExecutor;

public class IndexInfoS2T {

    private static final Logger logger = Logger.getLogger(IndexInfoS2T.class);

    private static final Properties properties = PropertiesUtil.getProperties();
    private static final String importFilesPath = properties.getProperty("importer.in.importFilesPath");

    private static final int corePoolSize = Integer.parseInt(properties.getProperty("importer.tikv.corePoolSize"));
    private static final int maxPoolSize = Integer.parseInt(properties.getProperty("importer.tikv.maxPoolSize"));

    public static void main(String[] args) throws InterruptedException {

        List<File> fileList = FileUtil.loadDirectory(new File(importFilesPath));
        logger.info(">>>>>>>>>> Welcome to TiKV importer. Need to import the following files.>>>>>>>>>>");
        assert fileList != null;
        for (File file : fileList) {
            int line = FileUtil.getFileLines(file);
            logger.info(String.format("[ file ] { %s } , [ line ] { %d }.", file.getAbsolutePath(), line));
        }
        logger.info(String.format(">>>>>>>>>>>>>>>>>>>>>>>>>>>> Total is [ %s ] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>", fileList.size()));

        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(corePoolSize, maxPoolSize, new IndexInfoS2TJob(fileList));
        for (int i = 0; i < fileList.size(); i++) {
            threadPoolExecutor.execute(new IndexInfoS2TJob(fileList));
        }
        threadPoolExecutor.shutdown();

        while (true) {
            if (threadPoolExecutor.isTerminated()) {
                logger.info("All Thread had finished! Import success!");
                System.exit(0);
            }
            Thread.sleep(3000);
        }
    }

}

class IndexInfoS2TJob implements Runnable {

    private static final Logger logger = Logger.getLogger(IndexInfoS2TJob.class);

    private static final Properties properties = PropertiesUtil.getProperties();
    private static final int batchSize = Integer.parseInt(properties.getProperty("importer.tikv.batchSize"));

    private static final String envId = properties.getProperty("importer.out.envId");
    private static final String appId = properties.getProperty("importer.out.appId");

    private static final TiSession tiSession = TiSessionUtil.getTiSession();

    private static final String INDEX_INFO_KET_FORMAT = "indexInfo_:_%s_:_%s_:_%s";

    private static final List<String> ttlTypeList = new ArrayList<>();
    private static final Map<String, Long> ttlTypeCountMap = new HashMap<>();

    private final List<File> fileList;
    private static Integer num = 0;

    public IndexInfoS2TJob(List<File> fileList) {
        this.fileList = fileList;
    }

    @Override
    public void run() {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String time = simpleDateFormat.format(new Date());

        BufferedInputStream bufferedInputStream = null;
        File file = null;

        synchronized (IndexInfoS2TJob.class) {
            try {
                file = fileList.get(num);
                logger.info(String.format("Thread -- [%s] will process No.[%s] file -- { %s }", Thread.currentThread().getName(), num, file.getAbsolutePath()));
                bufferedInputStream = new BufferedInputStream(new FileInputStream(fileList.get(num)));
                num++;
            } catch (FileNotFoundException e) {
                logger.error(String.format("Load file [%s] failed!", file), e);
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
                } catch (Exception e) {
                    logger.error(String.format("Parse file [%s] failed!", file.getAbsolutePath()));
                    return;
                }
                if (ttlTypeList.contains(indexInfoS.getType())) {
                    ttlTypeCountMap.put(indexInfoS.getType(), ttlTypeCountMap.get(indexInfoS.getType()) + 1);
                    continue;
                } else {
                    IndexInfoT indexInfoT = new IndexInfoT();
                    indexInfoT.setAppId(appId);
                    indexInfoT.setServiceTag(indexInfoS.getServiceTag());
                    indexInfoT.setTargetId(indexInfoS.getTargetId());
                    indexInfoT.setUpdateTime(time);
                    String indexInfoKey;
                    if (envId != null) {
                        indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, envId, indexInfoS.getType(), indexInfoS.getId());
                    } else {
                        indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, indexInfoS.getEnvId(), indexInfoS.getType(), indexInfoS.getId());
                    }
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
                    for (Iterator<Map.Entry<ByteString, ByteString>> it = kvPairs.entrySet().iterator(); it.hasNext(); ) {
                        Map.Entry<ByteString, ByteString> item = it.next();
                        k = item.getKey().toStringUtf8();
                        if (!rawKVClient.get(item.getKey()).isEmpty()) {
                            it.remove();
                            count--;
                            skipCount++;
                            logger.warn(String.format("Skip key %s, file is %s", k, file.getAbsolutePath()));
                        }
                    }
                    if (!kvPairs.isEmpty()) {
                        rawKVClient.batchPut(kvPairs);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info(String.format("Write file [%s] success! Total count = { %d }, skip count = { %d }", file.getAbsolutePath(), count, skipCount));
    }

}
