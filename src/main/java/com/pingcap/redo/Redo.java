package com.pingcap.redo;

import com.alibaba.fastjson.JSONObject;
import com.pingcap.enums.Model;
import com.pingcap.util.FileUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Redo {

    private static final Logger redoLog = LoggerFactory.getLogger(Model.REDO_LOG);
    private static final int batchSize = 5000;

    public static void redo(String redoFilePath, String type, RawKVClient rawKVClient) {
        List<File> redoFileList = FileUtil.showFileList(redoFilePath);
        int redoFileLine = 0;
        int totalImported = 0;
        int totalParseErr = 0;
        String redoLine = "";
        LineIterator lineIterator;
        JSONObject jsonObject;
        Map<ByteString, ByteString> kvParis = new HashMap<>();
        for (File file : redoFileList) {
            redoFileLine = FileUtil.getFileLines(file);
            try {
                lineIterator = FileUtils.lineIterator(file);
                int totalCount = 0;
                int aroundCount = 0;
                while (lineIterator.hasNext()) {
                    ++totalCount;
                    ++aroundCount;
                    redoLine = lineIterator.nextLine();
                    switch (type) {
                        case Model.INDEX_INFO:
                            try {
                                // Regardless of tempIndexInfo or indexInfo, the conversion of JSONObject is the same logic.
                                jsonObject = JSONObject.parseObject(redoLine);
                            } catch (Exception e) {
                                redoLog.error("Failed to parse json, file[{}], json[{}], line[{}]", file, redoFileLine, totalCount);
                                totalParseErr++;
                                batchPut(batchSize, totalCount, redoFileLine, aroundCount, rawKVClient, kvParis);
                                // If parse failed, we need to judge whether we need to insert in batches at this time
                                continue;
                            }
                            break;
                        case Model.TEMP_INDEX_INFO:
                            break;
                        default:
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static int batchPut(int batchSize, int totalCount, int redoFileLine, int aroundCount, RawKVClient rawKVClient, Map<ByteString, ByteString> kvParis) {
        if (aroundCount == batchSize || redoFileLine == totalCount) {
            rawKVClient.batchPut(kvParis);
        }
        aroundCount = 0;
        return aroundCount;
    }

}
