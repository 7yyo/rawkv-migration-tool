package com.pingcap.redo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.enums.Model;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.TempIndexInfo;
import com.pingcap.util.CountUtil;
import com.pingcap.util.FileUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.pingcap.enums.Model.ENV_ID;

public class Redo {

    private static final Logger redoLog = LoggerFactory.getLogger(Model.REDO_LOG);

    public static void run(Map<String, String> properties, TiSession tiSession) {

        long startTime = System.currentTimeMillis();

        // redo 文件夹路径
        String redoFilePath = properties.get(Model.REDO_FILE_PATH);
        // move 文件夹路径
        String moveFilePath = properties.get(Model.REDO_MOVE_PATH);
        // indexInfo 或 tempIndexInfo
        String type = properties.get(Model.REDO_TYPE);
        // 每 batchSize 条做一次 redo
        int batchSize = Integer.parseInt(properties.get(Model.REDO_BATCH_SIZE));

        // 创建 redo 每天迁移目录，格式为 moveFilePath/yyyyMMddhhmmss
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddhhmmss");
        String now = simpleDateFormat.format(new Date());
        // Redo 迁移文件夹
        FileUtil.createFolder(moveFilePath);
        // Redo 每日迁移文件夹
        FileUtil.createFolder(moveFilePath + "/" + now);

        String redoFailPath = moveFilePath + "/" + now + "/" + "redoFail.txt";
        File redoFailFile = FileUtil.createFile(redoFailPath);
        FileOutputStream fileOutputStream;
        FileChannel redoFileFileChannel = null;
        try {
            fileOutputStream = new FileOutputStream(redoFailFile);
            redoFileFileChannel = fileOutputStream.getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String redoNotExistsPath = moveFilePath + "/" + now + "/" + "redoNotExists.txt";
        File redoNotExistsFile = FileUtil.createFile(redoNotExistsPath);
        FileOutputStream fileOutputStream1;
        FileChannel redoNotExistsFileChannel = null;
        try {
            fileOutputStream1 = new FileOutputStream(redoNotExistsFile);
            redoNotExistsFileChannel = fileOutputStream1.getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Map<ByteString, ByteString> kvParis;
        RedoSummary redoSummary;
        JSONObject jsonObject;
        LineIterator lineIterator;
        int redoFileLineCount;
        String redoLine, replaceRedoLine, k, v, envId;
        RawKVClient rawKVClient = tiSession.createRawClient();

        // indexInfo 对比时间戳
        IndexInfo indexInfoRedo, indexInfoRedoValue, indexInfoTiKVValue;
        // indexInfo 不对比时间戳
        TempIndexInfo tempIndexInfoRedo, tempIndexInfoRedoValue;
//        TempIndexInfo tempIndexInfoTiKVValue;

        // Redo 文件列表
        List<File> redoFileList = FileUtil.showFileList(redoFilePath);
        for (File redoFile : redoFileList) {
            kvParis = new HashMap<>();
            redoSummary = new RedoSummary();
            // Redo 的文件行数
            redoFileLineCount = FileUtil.getFileLines(redoFile);
            try {
                lineIterator = FileUtils.lineIterator(redoFile);
                int totalCount = 0;
                int aroundCount = 0;

                while (lineIterator.hasNext()) {
                    totalCount++;
                    aroundCount++;
                    redoLine = lineIterator.nextLine();
                    try {
                        replaceRedoLine = redoLine.replaceAll("<<deleteFlag>>", "");
                        jsonObject = JSONObject.parseObject(replaceRedoLine);
                    } catch (Exception e) {
                        redoLog.error("Parse failed, file={}, data={}, line={}", redoFile, totalCount, totalCount);
                        redoSummary.setParseErr(redoSummary.getParseErr() + 1);
                        aroundCount = batchPut(aroundCount, redoFileLineCount, totalCount, rawKVClient, kvParis, redoSummary, batchSize, redoFileFileChannel, redoLine);
                        continue;
                    }
                    switch (type) {
                        case Model.INDEX_INFO:
                            indexInfoRedo = JSON.toJavaObject(jsonObject, IndexInfo.class);
                            if (!properties.get(ENV_ID).isEmpty()) {
                                envId = properties.get(ENV_ID);
                            } else {
                                envId = indexInfoRedo.getEnvId();
                            }
                            k = String.format(IndexInfo.KET_FORMAT, envId, indexInfoRedo.getType(), indexInfoRedo.getId());
                            // 根据 key 查询 raw kv 中的 value
                            v = rawKVClient.get(ByteString.copyFromUtf8(k)).toStringUtf8();
                            // 查询 raw kv 是否有这条数据，如果有则对比时间戳，进行 put 或 delete，若没有则记录 redo log
                            if (!v.isEmpty()) {
                                indexInfoTiKVValue = JSON.parseObject(v, IndexInfo.class);
                                // 如果 redo > tikv, 则进行 batch put 或 delete
                                if (CountUtil.compareTime(indexInfoRedo.getUpdateTime(), indexInfoTiKVValue.getUpdateTime()) == 1) {
                                    // delete redo
                                    if (redoLine.startsWith(Model.DELETE)) {
                                        delete(rawKVClient, k, redoSummary, redoFileFileChannel, redoLine);
                                    } else {
                                        // put redo
                                        indexInfoRedoValue = new IndexInfo();
                                        IndexInfo.initValueIndexInfoTiKV(indexInfoRedoValue, indexInfoRedo);
                                        kvParis.put(ByteString.copyFromUtf8(k), ByteString.copyFromUtf8(JSONObject.toJSONString(indexInfoRedoValue)));
                                        System.out.println(k + "===" + JSONObject.toJSONString(indexInfoRedoValue));
                                    }
                                } else {
                                    // 如果 redo 时间戳小, 则不做操作, 记录 redo log.
                                    redoLog.info("Key={}, redoTso={} <= rawkvTso={}, so skipped.", k, indexInfoRedo.getUpdateTime(), indexInfoTiKVValue.getUpdateTime());
                                    redoSummary.setSkip(redoSummary.getSkip() + 1);
                                }
                            } else {
                                redoLog.warn("Key={} is not exists, may be deleted, so skip.", k);
                                ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(redoLine + "\n");
                                redoNotExistsFileChannel.write(byteBuffer);
                                redoSummary.setSkip(redoSummary.getSkip() + 1);
                                continue;
                            }
                            break;
                        case Model.TEMP_INDEX_INFO:
                            tempIndexInfoRedo = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                            tempIndexInfoRedoValue = new TempIndexInfo();
                            k = String.format(TempIndexInfo.KEY_FORMAT, tempIndexInfoRedo.getEnvId(), tempIndexInfoRedo.getId());
                            v = rawKVClient.get(ByteString.copyFromUtf8(k)).toStringUtf8();
                            // 查询 raw kv 是否有这条数据，如果有则对比时间戳，进行 put 或 delete，若没有则记录 redo log
                            if (!v.isEmpty()) {
//                                tempIndexInfoTiKVValue = JSON.parseObject(v, TempIndexInfo.class);
                                // 如果 redo > tikv, 则进行 batch put 或 delete
//                                if (CountUtil.compareTime(tempIndexInfoRedo.getUpdateTime(), tempIndexInfoRedo.getUpdateTime()) > 1) {
                                // TempIndexInfo 直接覆盖
                                // delete
                                if (redoLine.startsWith(Model.DELETE)) {
                                    delete(rawKVClient, k, redoSummary, redoFileFileChannel, redoLine);
                                } else {
                                    // put
                                    TempIndexInfo.initValueTempIndexInfo(tempIndexInfoRedoValue, tempIndexInfoRedo);
                                    kvParis.put(ByteString.copyFromUtf8(k), ByteString.copyFromUtf8(JSONObject.toJSONString(tempIndexInfoRedoValue)));
                                }
//                                } else {
//                                    // 如果 redo 时间戳小, 则不做操作, 记录 redo log.
//                                    redoLog.info("Key[{}]===redo tso[{}] < tikv tso[{}], skip.", k, tempIndexInfoRedo.getUpdateTime(), indexInfoTikv.getUpdateTime());
//                                    redoSummary.setSkip(redoSummary.getSkip() + 1);
//                                }
                                // 如果不存在，说明该条记录可能被删除，跳过插入，记录 redo log.
                            } else {
                                redoLog.warn("Key={} is not exists, may be deleted, so skip.", k);
                                ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(redoLine + "\n");
                                redoNotExistsFileChannel.write(byteBuffer);
                                redoSummary.setSkip(redoSummary.getSkip() + 1);
                                continue;
                            }
                            break;
                        default:
                            redoLog.error("error type={}", type);
                            System.exit(0);
                    }
                    aroundCount = batchPut(aroundCount, redoFileLineCount, totalCount, rawKVClient, kvParis, redoSummary, batchSize, redoFileFileChannel, redoLine);

                }

                redoLog.info("Redo file={} complete. total={}, redoPut={}, redoErr={}, parseErr={}, skip={}, redoDelete={}", redoFile.getAbsolutePath(), totalCount, redoSummary.getRedo(), redoSummary.getPutErr(), redoSummary.getParseErr(), redoSummary.getSkip(), redoSummary.getDelete());

                // 回放完成后，将回放成功的文件转移到 move / 日期 的路径下
                File moveFile = new File(moveFilePath + "/" + now + "/" + redoFile.getName());
                FileUtils.moveFile(redoFile, moveFile);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        rawKVClient.close();
        redoLog.info("Redo all complete, duration={}s", (System.currentTimeMillis() - startTime) / 1000);
    }

    public static int batchPut(int aroundCount, int redoFileLine, int totalCount, RawKVClient rawKVClient, Map<ByteString, ByteString> kvParis, RedoSummary redoSummary, int batchSize, FileChannel fileChannel, String redoLine) {
        if (aroundCount == batchSize || redoFileLine == totalCount) {
            try {
                rawKVClient.batchPut(kvParis);
                redoSummary.setRedo(redoSummary.getRedo() + kvParis.size());
            } catch (Exception e) {
                StringBuilder errMsg = new StringBuilder();
                errMsg.append("Batch put failed, keys=\n");
                for (Map.Entry<ByteString, ByteString> kv : kvParis.entrySet()) {
                    errMsg.append(kv.getKey()).append("\n");
                }
                redoSummary.setPutErr(redoSummary.getPutErr() + kvParis.size());
                redoLog.error(errMsg.toString(), e);
                ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(redoLine + "\n");
                try {
                    fileChannel.write(byteBuffer);
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            } finally {
                kvParis.clear();
                aroundCount = 0;
            }
        }
        return aroundCount;
    }

    public static void delete(RawKVClient rawKVClient, String k, RedoSummary redoSummary, FileChannel fileChannel, String redoLine) {
        try {
            rawKVClient.delete(ByteString.copyFromUtf8(k));
            redoSummary.setDelete(redoSummary.getDelete() + 1);
        } catch (Exception e) {
            ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(redoLine + "\n");
            try {
                fileChannel.write(byteBuffer);
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
            redoLog.error("Delete failed, key={}", k);
            redoSummary.setPutErr(redoSummary.getPutErr() + 1);
        }
    }

    public static void main(String[] args) {
        TiConfiguration conf;
        TiSession tiSession;
        try {
            conf = TiConfiguration.createRawDefault("172.16.4.32:5555,172.16.4.33:5555,172.16.4.34:5555");
            tiSession = TiSession.create(conf);
            RawKVClient rawKVClient = tiSession.createRawClient();
            rawKVClient.delete(ByteString.copyFromUtf8("indexInfo_:_00998877_:_type09_:_9"));
        } catch (Exception e) {
            System.exit(0);
        }
    }

}

class RedoSummary {

    private int redo = 0;
    private int parseErr = 0;
    private int delete = 0;
    private int skip = 0;
    private int putErr = 0;

    public int getRedo() {
        return redo;
    }

    public void setRedo(int redo) {
        this.redo = redo;
    }

    public int getParseErr() {
        return parseErr;
    }

    public void setParseErr(int parseErr) {
        this.parseErr = parseErr;
    }

    public int getDelete() {
        return delete;
    }

    public void setDelete(int delete) {
        this.delete = delete;
    }

    public int getSkip() {
        return skip;
    }

    public void setSkip(int skip) {
        this.skip = skip;
    }

    public int getPutErr() {
        return putErr;
    }

    public void setPutErr(int putErr) {
        this.putErr = putErr;
    }

}

