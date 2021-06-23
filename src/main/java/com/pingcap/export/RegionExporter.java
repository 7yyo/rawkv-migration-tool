package com.pingcap.export;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.enums.Model;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.TempIndexInfo;
import com.pingcap.rawkv.RawKv;
import com.pingcap.util.ThreadPoolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.common.region.TiRegion;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author yuyang
 * <p>
 * Export raw kv data with region as the unit.
 * Because it is not available, it is not complete yet.
 */
public class RegionExporter {

    public static void runRegionExporter(Properties properties, TiSession tiSession) {

        String exportFilePath = properties.getProperty(Model.EXPORT_FILE_PATH);
        File exportFile = new File(exportFilePath);
        exportFile.mkdir();

        int corePoolSize = Integer.parseInt(properties.getProperty(Model.CORE_POOL_SIZE));
        int maxPoolSize = Integer.parseInt(properties.getProperty(Model.MAX_POOL_SIZE));

        List<TiRegion> tiRegionList = RawKv.getTiRegionList(tiSession);

        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(corePoolSize, maxPoolSize, null);
        RawKVClient rawKVClient;
        for (TiRegion tiRegion : tiRegionList) {
            // export_regionId.txt
            String exportFileName = exportFilePath + "/" + "export_" + tiRegion.getId() + ".txt";
            File file = new File(exportFileName);
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
            FileOutputStream fileOutputStream;
            FileChannel fileChannel = null;
            try {
                fileOutputStream = new FileOutputStream(file);
                fileChannel = fileOutputStream.getChannel();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            rawKVClient = tiSession.createRawClient();
            threadPoolExecutor.execute(new ExportJob(tiRegion, rawKVClient, fileChannel, properties));
        }

    }

}

class ExportJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    private final TiRegion tiRegion;
    private final RawKVClient rawKVClient;
    private final FileChannel fileChannel;
    private final Properties properties;

    public ExportJob(TiRegion tiRegion, RawKVClient rawKVClient, FileChannel fileChannel, Properties properties) {
        this.tiRegion = tiRegion;
        this.rawKVClient = rawKVClient;
        this.fileChannel = fileChannel;
        this.properties = properties;
    }

    @Override
    public void run() {

        logger.info(String.format("Thread - %s starts to export data of region[%s]", Thread.currentThread(), tiRegion.getId()));

        String scenes = properties.getProperty(Model.SCENES);
        String keyDelimiter = properties.getProperty(Model.KEY_DELIMITER);

        ByteString startKey = tiRegion.getStartKey();
        ByteString endKey = tiRegion.getEndKey();
        List<Kvrpcpb.KvPair> kvPairList = new ArrayList<>();
        try {
            kvPairList = rawKVClient.scan(startKey, endKey);
        } catch (Exception e) {
            logger.error(String.format("Failed to query data in region %s", tiRegion.getId()));
        }

        ByteBuffer byteBuffer;
        String key;
        String value;
        IndexInfo indexInfo;
        TempIndexInfo tempIndexInfo;
        JSONObject jsonObject;
        String json;
        for (Kvrpcpb.KvPair kvPair : kvPairList) {

            key = kvPair.getKey().toStringUtf8();
            value = kvPair.getValue().toStringUtf8();

            jsonObject = JSONObject.parseObject(value);

            switch (scenes) {
                case Model.INDEX_INFO:
                    try {
                        // json value
                        indexInfo = JSON.toJavaObject(jsonObject, IndexInfo.class);
                        // key = indexInfo_:_{envid}_:_{type}_:_{id}
                        indexInfo.setEnvId(key.split(keyDelimiter)[1]);
                        indexInfo.setType(key.split(keyDelimiter)[2]);
                        indexInfo.setId(key.split(keyDelimiter)[3]);
                    } catch (Exception e) {
                        logger.error(String.format("Raw KV data parse failed! k=%s, v=%s", key, value));
                        continue;
                    }
                    json = JSON.toJSONString(indexInfo);
                    break;
                case Model.TEMP_INDEX_INFO:
                    try {
                        // json value
                        tempIndexInfo = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                        // key = tempIndex_:_{envid}_:_{id}
                        tempIndexInfo.setEnvId(key.split(keyDelimiter)[1]);
                        tempIndexInfo.setId(key.split(keyDelimiter)[2]);
                    } catch (Exception e) {
                        logger.error(String.format("Raw KV data parse failed! k=%s, v=%s", key, value));
                        continue;
                    }
                    json = JSON.toJSONString(tempIndexInfo);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + scenes);
            }
            try {
                byteBuffer = StandardCharsets.UTF_8.encode(json + "\n");
                fileChannel.write(byteBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            fileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}

