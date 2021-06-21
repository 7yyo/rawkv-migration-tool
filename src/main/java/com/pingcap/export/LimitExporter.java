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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yuyang
 */
public class LimitExporter {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    static final Histogram IMPORT_LATENCY = Histogram.build().name("import_duration").help("Import duration in seconds.").labelNames("import_duration").register();

    private static final AtomicInteger TOTAL_EXPORT_NUM = new AtomicInteger(0);
    private static final String EXPORT_FILE_PATH = "/export_%s.txt";

    public static void runLimitExporter(String exportFilePath, Properties properties, TiSession tiSession) {

        int exportLimit = Integer.parseInt(properties.getProperty(Model.EXPORT_LIMIT));
        int exportThread = Integer.parseInt(properties.getProperty(Model.EXPORT_THREAD));

        FileUtil.deleteFolder(exportFilePath);
        FileUtil.deleteFolders(exportFilePath);

        File file;
        FileOutputStream fileOutputStream;
        FileChannel fileChannel;
        List<FileChannel> fileChannelList = new ArrayList<>();

        for (int i = 0; i < exportThread; i++) {
            file = new File(String.format(exportFilePath + EXPORT_FILE_PATH, i));
            try {
                file.createNewFile();
                fileOutputStream = new FileOutputStream(file);
                fileChannel = fileOutputStream.getChannel();
                fileChannelList.add(fileChannel);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        int interval = Integer.parseInt(properties.getProperty(Model.TIMER_INTERVAL));
        String keyDelimiter = properties.getProperty(Model.KEY_DELIMITER);

        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(exportThread, exportThread, null);

        Random random = new Random();

        Timer timer = new Timer();
        ExportTimer exportTimer = new ExportTimer(TOTAL_EXPORT_NUM);
        timer.schedule(exportTimer, 5000, interval);

        boolean isStart = true;
        ByteString startKey = ByteString.EMPTY;
        ByteString endKey = null;
        List<Kvrpcpb.KvPair> kvPairList;
        RawKVClient rawKvClient = tiSession.createRawClient();

        while (isStart || endKey != null) {

            Histogram.Timer scanDuration = IMPORT_LATENCY.labels("scan duration").startTimer();
            if (isStart) {
                kvPairList = rawKvClient.scan(startKey, exportLimit);
            } else {
                kvPairList = rawKvClient.scan(endKey, exportLimit);
            }
            if (kvPairList.size() == 1) {
                logger.info(String.format("Complete data export! Total export num =[%s]", TOTAL_EXPORT_NUM));
                System.exit(0);
            }
            threadPoolExecutor.execute(new LimitExportJob(TOTAL_EXPORT_NUM, keyDelimiter, IMPORT_LATENCY, isStart, kvPairList, fileChannelList.get(random.nextInt(fileChannelList.size()))));
            isStart = false;
            endKey = kvPairList.get(kvPairList.size() - 1).getKey();
            scanDuration.observeDuration();
        }

    }
}

class LimitExportJob implements Runnable {

    private final Histogram importLatency;
    private final boolean isStart;
    private final List<Kvrpcpb.KvPair> kvPairList;
    private final FileChannel fileChannel;
    private final String keyDelimiter;
    private AtomicInteger totalExportNum;

    private static final String INDEX_INFO = "indexInfo_";
    private static final String TEMP_INDEX_INFO = "tempIndex_";

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
            if (key.startsWith(INDEX_INFO)) {
                jsonObject = JSONObject.parseObject(value);
                indexInfo = JSON.toJavaObject(jsonObject, IndexInfo.class);
                // key = indexInfo_:_{envid}_:_{type}_:_{id}
                indexInfo.setEnvId(key.split(keyDelimiter)[1]);
                indexInfo.setType(key.split(keyDelimiter)[2]);
                indexInfo.setId(key.split(keyDelimiter)[3]);
                json = JSON.toJSONString(indexInfo);
                kvPair.append(json).append("\n");
            } else if (key.startsWith(TEMP_INDEX_INFO)) {
                jsonObject = JSONObject.parseObject(value);
                tempIndexInfo = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                // key = tempIndex_:_{envid}_:_{id}
                tempIndexInfo.setEnvId(key.split(keyDelimiter)[1]);
                tempIndexInfo.setId(key.split(keyDelimiter)[2]);
                json = JSON.toJSONString(tempIndexInfo);
                kvPair.append(json).append("\n");
            } else {
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
