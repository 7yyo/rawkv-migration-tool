package com.pingcap.export;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.enums.Model;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.TempIndexInfo;
import com.pingcap.timer.ExportTimer;
import com.pingcap.util.FileUtil;
import com.pingcap.util.ThreadPoolUtil;
import io.prometheus.client.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yuyang
 * <p>
 * Export every limit data until the raw kv data is completely exported.
 */
public class LimitExporter {

    static final Histogram IMPORT_LATENCY = Histogram.build().name("import_duration").help("Import duration in seconds.").labelNames("import_duration").register();

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    private static final AtomicInteger TOTAL_EXPORT_NUM = new AtomicInteger(0);
    private static final String EXPORT_FILE_PATH = "/export_%s.txt";

    public static void run(Properties properties, TiSession tiSession) {

        int limit = Integer.parseInt(properties.getProperty(Model.EXPORT_LIMIT)) + 1;
        int thread = Integer.parseInt(properties.getProperty(Model.EXPORT_THREAD));
        String exportFilePath = properties.getProperty(Model.EXPORT_FILE_PATH);

        File file;
        FileOutputStream fileOutputStream;
        FileChannel fileChannel;
        List<FileChannel> fileChannelList = new ArrayList<>();

        FileUtil.deleteFolder(exportFilePath);

        if (new File(exportFilePath).mkdir()) {
            // Create data export files according to the number of threads
            for (int i = 0; i < thread; i++) {
                file = new File(String.format(exportFilePath + EXPORT_FILE_PATH, i));
                try {
                    if (file.createNewFile()) {
                        fileOutputStream = new FileOutputStream(file);
                        fileChannel = fileOutputStream.getChannel();
                        fileChannelList.add(fileChannel);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        int interval = Integer.parseInt(properties.getProperty(Model.TIMER_INTERVAL));
        String keyDelimiter = properties.getProperty(Model.KEY_DELIMITER);

        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(thread, thread);

        Timer timer = new Timer();
        ExportTimer exportTimer = new ExportTimer(TOTAL_EXPORT_NUM);
        timer.schedule(exportTimer, 5000, interval);

        ByteString startKey = ByteString.EMPTY;
        ByteString lastStartKey = ByteString.EMPTY;
        List<Kvrpcpb.KvPair> kvPairList;
        RawKVClient rawKvClient = tiSession.createRawClient();
        boolean isStart = true;
        Random random = new Random();
        while (isStart || startKey != null) {

            Histogram.Timer scanDuration = IMPORT_LATENCY.labels("scan duration").startTimer();
            kvPairList = rawKvClient.scan(startKey, limit);
            scanDuration.observeDuration();
            if (!kvPairList.isEmpty()) {
                String l = lastStartKey == null ? "null" : lastStartKey.toStringUtf8();
                String s = startKey.toStringUtf8();
                if (!isStart && l.equals(s)) {
                    threadPoolExecutor.shutdown();
                    try {
                        threadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                        System.exit(0);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    logger.info(String.format("Complete data export! Total number of exported rows=[%s]", TOTAL_EXPORT_NUM));
                }
                // All threads randomly write a data export file
                FileChannel randomFileChannel = fileChannelList.get(random.nextInt(fileChannelList.size()));
                threadPoolExecutor.execute(new LimitExportJob(TOTAL_EXPORT_NUM, keyDelimiter, IMPORT_LATENCY, isStart, kvPairList, randomFileChannel));
                isStart = false;
                lastStartKey = startKey;
                startKey = kvPairList.get(kvPairList.size() - 1).getKey();
            }


        }

    }
}

class LimitExportJob implements Runnable {

    private final Histogram importLatency;
    private final boolean isStart;
    private final List<Kvrpcpb.KvPair> kvPairList;
    private final FileChannel fileChannel;
    private final String keyDelimiter;
    private final AtomicInteger totalExportNum;

    private static final String INDEX_INFO_PREFIX = "indexInfo";
    private static final String TEMP_INDEX_INFO_PREFIX = "tempIndex";

    public LimitExportJob(AtomicInteger totalExportNum, String keyDelimiter, Histogram importLatency, boolean isStart, List<Kvrpcpb.KvPair> kvPairList, FileChannel fileChannel) {
        this.keyDelimiter = keyDelimiter;
        this.importLatency = importLatency;
        this.isStart = isStart;
        this.kvPairList = kvPairList;
        this.fileChannel = fileChannel;
        this.totalExportNum = totalExportNum;
    }

    @Override
    public void run() {

        StringBuilder kvPair = new StringBuilder();
        IndexInfo indexInfo;
        TempIndexInfo tempIndexInfo;
        JSONObject jsonObject;
        Histogram.Timer transformDuration;
        Histogram.Timer writeDuration;

        int n = 0;
        for (int i = 0; i < kvPairList.size(); i++) {

            transformDuration = importLatency.labels("transform duration").startTimer();
            if (!isStart) {
                if (i == 0) {
                    continue;
                }
            }
            String key = kvPairList.get(i).getKey().toStringUtf8();
            String value = kvPairList.get(i).getValue().toStringUtf8();
            String json;
            if (key.startsWith(INDEX_INFO_PREFIX)) {
                jsonObject = JSONObject.parseObject(value);
                indexInfo = JSON.toJavaObject(jsonObject, IndexInfo.class);
                // key = indexInfo_:_{envid}_:_{type}_:_{id}
                indexInfo.setEnvId(key.split(keyDelimiter)[1]);
                indexInfo.setType(key.split(keyDelimiter)[2]);
                indexInfo.setId(key.split(keyDelimiter)[3]);
                json = JSON.toJSONString(indexInfo);
                kvPair.append(json).append("\n");
            } else if (key.startsWith(TEMP_INDEX_INFO_PREFIX)) {
                jsonObject = JSONObject.parseObject(value);
                tempIndexInfo = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                // key = tempIndex_:_{envid}_:_{id}
                tempIndexInfo.setEnvId(key.split(keyDelimiter)[1]);
                tempIndexInfo.setId(key.split(keyDelimiter)[2]);
                json = JSON.toJSONString(tempIndexInfo);
                kvPair.append(json).append("\n");
            } else {
                // key@value
                json = key + "@" + value;
                kvPair.append(json).append("\n");
            }
            transformDuration.observeDuration();
            n++;
        }

        try {
            ByteBuffer line = StandardCharsets.UTF_8.encode(CharBuffer.wrap(kvPair));
            writeDuration = importLatency.labels("write duration").startTimer();
            fileChannel.write(line);
            writeDuration.observeDuration();
            totalExportNum.addAndGet(n);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
