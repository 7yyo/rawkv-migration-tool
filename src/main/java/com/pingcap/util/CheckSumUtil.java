package com.pingcap.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.enums.Model;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.TempIndexInfo;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CheckSumUtil {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger checkSumLog = LoggerFactory.getLogger(Model.CHECK_SUM_LOG);

    public static BufferedWriter initCheckSumLog(Properties properties, File originalFile) {

        String checkSumFilePath = properties.getProperty(Model.CHECK_SUM_FILE_PATH);
        BufferedWriter bufferedWriter = null;
        String checkSumFileName = checkSumFilePath.replaceAll("\"", "") + "/" + originalFile.getName().replaceAll("\\.", "") + "/" + Thread.currentThread().getId() + ".txt";
        File checkSumFile = new File(checkSumFileName);

        try {
            checkSumFile.getParentFile().mkdirs();
            checkSumFile.createNewFile();
            FileWriter fileWriter = new FileWriter(checkSumFile, true);
            bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(originalFile.getAbsolutePath() + "\n");
            bufferedWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bufferedWriter;
    }

    public static void checkSumIndexInfoJson(String checkSumFilePath, String checkSumDelimiter, TiSession tiSession, Properties properties) {

        logger.info(String.format("************ Start data verification for [%s] ************", checkSumFilePath));

        long interval = Long.parseLong(properties.getProperty(Model.TIMER_INTERVAL));
        String importMode = properties.getProperty(Model.MODE);
        String scenes = properties.getProperty(Model.SCENES);
        String delimiter_1 = properties.getProperty(Model.DELIMITER_1);
        String delimiter_2 = properties.getProperty(Model.DELIMITER_2);
        String keyDelimiter = properties.getProperty(Model.KEY_DELIMITER);

        int checkParseErrorNum = 0;
        int checkNotInsertErrorNum = 0;
        int checkFailNum = 0;
        AtomicInteger totalCheckNum = new AtomicInteger(0);
        RawKVClient rawKVClient = tiSession.createRawClient();

        JSONObject jsonObject;

        String originalLine = "";
        String csFileLine = "";
        String csKey;
        int csFileLineNum;
        int lastFileLine = 0;
        String value;

        File checkSumFile = new File(checkSumFilePath);

        Timer timer = new Timer();
        CheckSumTimer checkSumTimer = new CheckSumTimer(checkSumFilePath, totalCheckNum, FileUtil.getFileLines(checkSumFile) - 1);
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
        while (checkSumFileIt.hasNext()) {
            int originalLineNum = 0;
            // Total check num
            totalCheckNum.addAndGet(1);
            try {
                // check sum file line, Uniform format, no branch logic required
                csFileLine = checkSumFileIt.nextLine();
                csKey = csFileLine.split(checkSumDelimiter)[0];
                csFileLineNum = Integer.parseInt(csFileLine.split(checkSumDelimiter)[1]);
            } catch (Exception e) {
                // Illegal format
                checkParseErrorNum++;
                checkSumLog.error(String.format("The check sum file data line has an illegal format! Line=%s", csFileLine));
                continue;
            }

            // Get value by check sum file key
            value = rawKVClient.get(ByteString.copyFromUtf8(csKey)).toStringUtf8();
            if (value.isEmpty()) {
                checkSumLog.warn(String.format("The key [%s] is not be inserted! Please confirm whether it is incremental data.", csKey));
                checkNotInsertErrorNum++;
                continue;
            }

            int rowSpan = csFileLineNum - lastFileLine;

            // begin to check sum original & checkSum

            while (originalFileIt.hasNext()) {
                // original file line
                originalLine = originalFileIt.nextLine();
                if (++originalLineNum == rowSpan) {
                    break;
                }
            }
            lastFileLine = Integer.parseInt(csFileLine.split(checkSumDelimiter)[1]);

            // Init checkSum object
            // Because the check sum is all < json key + line num >, the format is unified, and there is no csv
            IndexInfo indexInfo_checkSum = new IndexInfo();
            TempIndexInfo tempIndexInfo_checkSum = new TempIndexInfo();
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
                        checkSumLog.error(String.format("Parse failed! Line = %s", value));
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
                        checkSumLog.error(String.format("Parse failed! Line = %s", value));
                        checkParseErrorNum++;
                        continue;
                    }
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + scenes);
            }

            // Init original object
            IndexInfo indexInfo_original;
            TempIndexInfo tempIndexInfo_original;
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
                    switch (scenes) {
                        case Model.INDEX_INFO:
                            indexInfo_original = IndexInfo.initIndexInfo(originalLine, delimiter_1, delimiter_2);
                            if (!indexInfo_checkSum.equals(indexInfo_original)) {
                                checkSumLog.error(String.format("Check sum failed! Line = %s", originalLine));
                                checkFailNum++;
                            }
                            break;
                        case Model.TEMP_INDEX_INFO:
                            tempIndexInfo_original = TempIndexInfo.initTempIndexInfo(originalLine, delimiter_1, delimiter_2);
                            if (!tempIndexInfo_checkSum.equals(tempIndexInfo_original)) {
                                checkSumLog.error(String.format("Check sum failed! Line = %s", originalLine));
                                checkFailNum++;
                            }
                            break;
                        default:
                            throw new IllegalStateException("Unexpected value: " + scenes);
                    }
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + importMode);
            }

        }

        LineIterator.closeQuietly(checkSumFileIt);
        LineIterator.closeQuietly(originalFileIt);

        timer.cancel();

        logger.info(String.format("[%s] check sum over! TotalCheckNum[%s], TotalNotInsertNum[%s], TotalParseErrorNum[%s], TotalCheckFailNum[%s]", checkSumFilePath, totalCheckNum, checkNotInsertErrorNum, checkParseErrorNum, checkFailNum));
    }

}
