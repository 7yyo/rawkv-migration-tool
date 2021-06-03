package com.pingcap.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.enums.Model;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.TempIndexInfo;
import com.pingcap.timer.CheckSumTimer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CheckSumUtil {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger checkSumLog = LoggerFactory.getLogger(Model.CHECK_SUM_LOG);

    public static FileChannel initCheckSumLog(Properties properties, FileChannel fileChannel, File originalFile) {

        String checkSumFilePath = properties.getProperty(Model.CHECK_SUM_FILE_PATH);
        String checkSumFileName = checkSumFilePath.replaceAll("\"", "") + "/" + originalFile.getName().replaceAll("\\.", "") + "/" + Thread.currentThread().getId() + ".txt";

        File checkSumFile = new File(checkSumFileName);
        try {
            checkSumFile.getParentFile().mkdirs();
            checkSumFile.createNewFile();
            FileOutputStream fileOutputStream = new FileOutputStream(new File(checkSumFileName));
            fileChannel = fileOutputStream.getChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            // Record original file in check sum file first line
            ByteBuffer originalLine = StandardCharsets.UTF_8.encode(originalFile.getAbsolutePath() + "\n");
            fileChannel.write(originalLine);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileChannel;
    }

    public static void checkSum(String checkSumFilePath, String checkSumDelimiter, TiSession tiSession, Properties properties) {

        logger.info(String.format("************ Start data verification for [%s] ************", checkSumFilePath));

        long interval = Long.parseLong(properties.getProperty(Model.TIMER_INTERVAL));
        String importMode = properties.getProperty(Model.MODE);
        String scenes = properties.getProperty(Model.SCENES);
        String keyDelimiter = properties.getProperty(Model.KEY_DELIMITER);
        String delimiter_1 = properties.getProperty(Model.DELIMITER_1);
        String delimiter_2 = properties.getProperty(Model.DELIMITER_2);


        int checkParseErrorNum = 0;
        int checkNotInsertErrorNum = 0;
        int checkFailNum = 0;
        AtomicInteger totalCheckNum = new AtomicInteger(0);
        RawKVClient rawKVClient = tiSession.createRawClient();

        JSONObject jsonObject;

        String originalLine = "";
        String csFileLine = "";
        String csKey = "";
        int csFileLineNum = 0;
        int lastFileLine = 0;
        String value;

        File checkSumFile = new File(checkSumFilePath);

        Timer timer = new Timer();
        CheckSumTimer checkSumTimer = new CheckSumTimer(checkSumFilePath, totalCheckNum, FileUtil.getFileLines(checkSumFile));
        timer.schedule(checkSumTimer, 5000, interval);

        // Get original file path from check sum file first line.
        String originalFilePath = "";
        LineIterator checkSumFileIt = null;
        try {
            checkSumFileIt = FileUtils.lineIterator(checkSumFile, "UTF-8");
            if (checkSumFileIt.hasNext()) {
                // The first line of the check sum file is the path of the corresponding original data file.
                originalFilePath = checkSumFileIt.nextLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // original file iterator
        LineIterator originalFileIt = null;
        File originalFile = new File(originalFilePath);
        try {
            originalFileIt = FileUtils.lineIterator(originalFile, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }

        // get original line to check sum
        int originalLineNum;
        int rowSpan;
        IndexInfo indexInfo_checkSum;
        TempIndexInfo tempIndexInfo_checkSum;
        IndexInfo indexInfo_original;
        TempIndexInfo tempIndexInfo_original;
        if (checkSumFileIt != null) {
            while (checkSumFileIt.hasNext()) {
                originalLineNum = 0;
                // Total check num
                totalCheckNum.addAndGet(1);
                csFileLine = checkSumFileIt.nextLine();

//                try {
//                    // check sum file line, Uniform format, no branch logic required
//                    csFileLine = checkSumFileIt.nextLine();
//                    csKey = csFileLine.split(checkSumDelimiter)[0];
//                    csFileLineNum = Integer.parseInt(csFileLine.split(checkSumDelimiter)[1]);
//                } catch (Exception e) {
//                    // Illegal format
//                    checkParseErrorNum++;
//                    checkSumLog.error(String.format("The check sum file data line has an illegal format! Line=%s, original file line num=%s", csFileLine, csFileLineNum));
//                    continue;
//                }
//
//                // Get value by check sum file key
//                value = rawKVClient.get(ByteString.copyFromUtf8(csKey)).toStringUtf8();
//                if (value.isEmpty()) {
//                    checkSumLog.warn(String.format("The key [%s] is not be inserted! original file line num=%s", csKey, csFileLineNum));
//                    checkNotInsertErrorNum++;
//                    continue;
//                }

                rowSpan = csFileLineNum - lastFileLine;

                // begin to check sum original & checkSum
                if (originalFileIt != null) {
                    while (originalFileIt.hasNext()) {
                        // original file line
                        originalLine = originalFileIt.nextLine();
                        if (originalLineNum++ == rowSpan) {
                            break;
                        }
                    }
                    lastFileLine = Integer.parseInt(csFileLine.split(checkSumDelimiter)[1]);

                    try {
                        // check sum file line, Uniform format, no branch logic required
                        csKey = csFileLine.split(checkSumDelimiter)[0];
                        csFileLineNum = Integer.parseInt(csFileLine.split(checkSumDelimiter)[1]);
                    } catch (Exception e) {
                        // Illegal format
                        checkParseErrorNum++;
                        checkSumLog.error(String.format("The check sum file data line has an illegal format! key=%s, line=%s", csKey, csFileLineNum));
                        continue;
                    }

                    // Get value by check sum file key
                    value = rawKVClient.get(ByteString.copyFromUtf8(csKey)).toStringUtf8();
                    if (value.isEmpty()) {
                        checkSumLog.warn(String.format("The key [%s] is not be inserted! Original file line =%s", csKey, originalLine));
                        checkNotInsertErrorNum++;
                        continue;
                    }

                    // Init checkSum object
                    // Because the check sum is all < json key + line num >, the format is unified, and there is no csv
                    indexInfo_checkSum = new IndexInfo();
                    tempIndexInfo_checkSum = new TempIndexInfo();
                    jsonObject = JSONObject.parseObject(value);
                    switch (scenes) {
                        case Model.INDEX_INFO:
                            try {
                                // json value
                                indexInfo_checkSum = JSON.toJavaObject(jsonObject, IndexInfo.class);
                                // key = indexInfo_:_{envid}_:_{type}_:_{id}
                                indexInfo_checkSum.setEnvId(csKey.split(keyDelimiter)[1]);
                                indexInfo_checkSum.setType(csKey.split(keyDelimiter)[2]);
                                indexInfo_checkSum.setId(csKey.split(keyDelimiter)[3]);
                            } catch (Exception e) {
                                checkSumLog.error(String.format("Check sum file line parse failed! Line = %s", value));
                                checkParseErrorNum++;
                                continue;
                            }
                            break;
                        case Model.TEMP_INDEX_INFO:
                            try {
                                // json value
                                tempIndexInfo_checkSum = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                                // key = tempIndex_:_{envid}_:_{id}
                                tempIndexInfo_checkSum.setEnvId(csKey.split(keyDelimiter)[1]);
                                tempIndexInfo_checkSum.setId(csKey.split(keyDelimiter)[2]);
                            } catch (Exception e) {
                                checkSumLog.error(String.format("Check sum file line parse failed! Line = %s", value));
                                checkParseErrorNum++;
                                continue;
                            }
                            break;
                        default:
                            throw new IllegalStateException("Unexpected value: " + scenes);
                    }

                    // Init original object
                    switch (importMode) {
                        case Model.JSON_FORMAT:
                            jsonObject = JSONObject.parseObject(originalLine);
                            switch (scenes) {
                                case Model.INDEX_INFO:
                                    try {
                                        indexInfo_original = JSON.toJavaObject(jsonObject, IndexInfo.class);
                                        if (!indexInfo_checkSum.equals(indexInfo_original)) {
                                            checkSumLog.error(String.format("Check sum failed! Line = %s", originalLine));
                                            checkFailNum++;
                                        }
                                    } catch (Exception e) {
                                        checkSumLog.error(String.format("Parse failed! Line = %s", originalLine));
                                        checkParseErrorNum++;
                                        continue;
                                    }
                                    break;
                                case Model.TEMP_INDEX_INFO:
                                    try {
                                        tempIndexInfo_original = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                                        if (!tempIndexInfo_checkSum.equals(tempIndexInfo_original)) {
                                            checkSumLog.error(String.format("Check sum failed! Line = %s", originalLine));
                                            checkFailNum++;
                                        }
                                    } catch (Exception e) {
                                        checkSumLog.error(String.format("Parse failed! Line = %s", originalLine));
                                        checkParseErrorNum++;
                                        continue;
                                    }
                                    break;
                                default:
                                    throw new IllegalStateException("Unexpected value: " + scenes);
                            }
                            break;
                        case Model.CSV_FORMAT:
                            indexInfo_original = IndexInfo.initIndexInfo(originalLine, delimiter_1, delimiter_2);
                            if (!indexInfo_checkSum.equals(indexInfo_original)) {
                                checkSumLog.error(String.format("Check sum failed! Line = %s", originalLine));
                                checkFailNum++;
                            }
                            break;
                        default:
                            throw new IllegalStateException("Unexpected value: " + importMode);
                    }

                }
            }
            try {
                checkSumFileIt.close();
                originalFileIt.close();
                rawKVClient.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        timer.cancel();
        logger.info(String.format("[%s] check sum completed! TotalCheckNum[%s], TotalNotInsertNum[%s], TotalParseErrorNum[%s], TotalCheckFailNum[%s]", checkSumFilePath, totalCheckNum, checkNotInsertErrorNum, checkParseErrorNum, checkFailNum));
    }

    public static void simpleCheckSum(String originalFilePath, TiSession tiSession, Properties properties) {

        logger.info(String.format("************ Start total data verification for [%s] ************", originalFilePath));

        long interval = Long.parseLong(properties.getProperty(Model.TIMER_INTERVAL));
        String importMode = properties.getProperty(Model.MODE);
        String scenes = properties.getProperty(Model.SCENES);
        String delimiter_1 = properties.getProperty(Model.DELIMITER_1);
        String delimiter_2 = properties.getProperty(Model.DELIMITER_2);
        String keyDelimiter = properties.getProperty(Model.KEY_DELIMITER);
        String envId = properties.getProperty(Model.ENV_ID);

        int checkParseErrorNum = 0;
        int checkNotInsertErrorNum = 0;
        int checkFailNum = 0;
        AtomicInteger totalCheckNum = new AtomicInteger(0);
        RawKVClient rawKVClient = tiSession.createRawClient();

        JSONObject jsonObject;

        String originalLine = "";

        File originalFile = new File(originalFilePath);
        Timer timer = new Timer();
        CheckSumTimer checkSumTimer = new CheckSumTimer(originalFilePath, totalCheckNum, FileUtil.getFileLines(originalFile));
        timer.schedule(checkSumTimer, 5000, interval);

        // original file iterator
        LineIterator originalFileIt = null;
        try {
            originalFileIt = FileUtils.lineIterator(originalFile, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }

        IndexInfo indexInfo_rawKV;
        TempIndexInfo tempIndexInfo_rawKV;
        IndexInfo indexInfo_original;
        TempIndexInfo tempIndexInfo_original;
        String key;
        String value;
        if (originalFileIt != null) {
            while (originalFileIt.hasNext()) {
                totalCheckNum.addAndGet(1);
                originalLine = originalFileIt.nextLine();
                // Init original object
                switch (importMode) {
                    case Model.JSON_FORMAT:
                        jsonObject = JSONObject.parseObject(originalLine);
                        switch (scenes) {
                            case Model.INDEX_INFO:
                                try {
                                    indexInfo_original = JSON.toJavaObject(jsonObject, IndexInfo.class);
                                    // get value by original key
                                    key = String.format(IndexInfo.INDEX_INFO_KET_FORMAT, envId, indexInfo_original.getType(), indexInfo_original.getId());
                                    // key = indexInfo_:_{envid}_:_{type}_:_{id}
                                    indexInfo_original.setEnvId(key.split(keyDelimiter)[1]);
                                    indexInfo_original.setType(key.split(keyDelimiter)[2]);
                                    indexInfo_original.setId(key.split(keyDelimiter)[3]);
                                    value = rawKVClient.get(ByteString.copyFromUtf8(key)).toStringUtf8();
                                    if (StringUtils.isBlank(value)) {
                                        checkSumLog.warn(String.format("The original file line = [%s] is not be inserted!", originalLine));
                                        checkNotInsertErrorNum++;
                                        continue;
                                    }
                                    jsonObject = JSONObject.parseObject(value);
                                    indexInfo_rawKV = JSON.toJavaObject(jsonObject, IndexInfo.class);
                                    indexInfo_rawKV.setEnvId(key.split(keyDelimiter)[1]);
                                    indexInfo_rawKV.setType(key.split(keyDelimiter)[2]);
                                    indexInfo_rawKV.setId(key.split(keyDelimiter)[3]);
                                    if (!indexInfo_rawKV.equals(indexInfo_original)) {
                                        checkSumLog.error(String.format("Check sum failed! Line = [%s]", originalLine));
                                        checkFailNum++;
                                        continue;
                                    }
                                } catch (Exception e) {
                                    checkSumLog.error(String.format("Parse failed! Line = %s", originalLine));
                                    checkParseErrorNum++;
                                    continue;
                                }
                                break;
                            case Model.TEMP_INDEX_INFO:
                                try {
                                    tempIndexInfo_original = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                                    key = String.format(TempIndexInfo.TEMP_INDEX_INFO_KEY_FORMAT, envId, tempIndexInfo_original.getId());
                                    value = rawKVClient.get(ByteString.copyFromUtf8(key)).toStringUtf8();
                                    // key = tempIndex_:_{envid}_:_{id}
                                    tempIndexInfo_original.setEnvId(key.split(keyDelimiter)[1]);
                                    tempIndexInfo_original.setId(key.split(keyDelimiter)[2]);
                                    if (StringUtils.isBlank(value)) {
                                        checkSumLog.warn(String.format("The original file line = [%s] is not be inserted!", originalLine));
                                        checkNotInsertErrorNum++;
                                        continue;
                                    }
                                    jsonObject = JSONObject.parseObject(value);
                                    tempIndexInfo_rawKV = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                                    tempIndexInfo_rawKV.setEnvId(key.split(keyDelimiter)[1]);
                                    tempIndexInfo_rawKV.setId(key.split(keyDelimiter)[2]);
                                    if (!tempIndexInfo_rawKV.equals(tempIndexInfo_original)) {
                                        checkSumLog.error(String.format("Check sum failed! Line = %s", originalLine));
                                        checkFailNum++;
                                        continue;
                                    }
                                } catch (Exception e) {
                                    checkSumLog.error(String.format("Parse failed! Line = %s", originalLine));
                                    checkParseErrorNum++;
                                    continue;
                                }
                                break;
                            default:
                                throw new IllegalStateException("Unexpected value: " + scenes);
                        }
                        break;
                    case Model.CSV_FORMAT:
                        try {
                            indexInfo_original = IndexInfo.initIndexInfo(originalLine, delimiter_1, delimiter_2);
                            key = String.format(IndexInfo.INDEX_INFO_KET_FORMAT, envId, indexInfo_original.getType(), indexInfo_original.getId());
                            value = rawKVClient.get(ByteString.copyFromUtf8(key)).toStringUtf8();
                            if (StringUtils.isBlank(value)) {
                                checkSumLog.warn(String.format("The original file line = [%s] is not be inserted!", originalLine));
                                checkNotInsertErrorNum++;
                                continue;
                            }
                            jsonObject = JSONObject.parseObject(value);
                            indexInfo_rawKV = JSON.toJavaObject(jsonObject, IndexInfo.class);
                            if (!indexInfo_rawKV.equals(indexInfo_original)) {
                                checkSumLog.error(String.format("Check sum failed! Line = %s", originalLine));
                                checkFailNum++;
                                continue;
                            }
                        } catch (Exception e) {
                            checkSumLog.error(String.format("Parse failed! Line = %s", originalLine));
                            checkParseErrorNum++;
                        }
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + importMode);
                }
            }
        }
        timer.cancel();
        logger.info(String.format("[%s] check sum completed! TotalCheckNum[%s], TotalNotInsertNum[%s], TotalParseErrorNum[%s], TotalCheckFailNum[%s]", originalFilePath, totalCheckNum, checkNotInsertErrorNum, checkParseErrorNum, checkFailNum));

    }

}
