package com.pingcap.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.enums.Model;
import com.pingcap.pojo.IndexInfo;
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

        int checkParseErrorNum = 0;
        int checkNotInsertErrorNum = 0;
        int checkFailNum = 0;
        long interval = Long.parseLong(properties.getProperty(Model.TIMER_INTERVAL));

        File checkSumFile = new File(checkSumFilePath);

        RawKVClient rawKVClient = tiSession.createRawClient();
        AtomicInteger totalCheckNum = new AtomicInteger(0);

        Timer timer = new Timer();
        int checkSumFileLineNum = FileUtil.getFileLines(checkSumFile);
        CheckSumTimer checkSumTimer = new CheckSumTimer(checkSumFilePath, totalCheckNum, checkSumFileLineNum - 1);
        timer.schedule(checkSumTimer, 5000, interval);

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

        LineIterator originalFileIt = null;
        File originalFile = new File(originalFilePath);
        try {
            originalFileIt = FileUtils.lineIterator(originalFile, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }


        JSONObject jsonObject;
        String evnId;
        String type;
        String id;


        String originalLine = "";
        String csFileLine = "";
        String csKey;
        int csFileLineNum;
        int lastFileLine = 0;
        String value;
        while (checkSumFileIt.hasNext()) {
            int originalLineNum = 0;
            // Total check num
            totalCheckNum.addAndGet(1);
            try {
                csFileLine = checkSumFileIt.nextLine();
                csKey = csFileLine.split(checkSumDelimiter)[0];
                csFileLineNum = Integer.parseInt(csFileLine.split(checkSumDelimiter)[1]);
            } catch (Exception e) {
                // Illegal format
                checkParseErrorNum++;
                checkSumLog.error(String.format("The check sum file data line has an illegal format! Line=%s", csFileLine));
                continue;
            }

            int rowSpan = csFileLineNum - lastFileLine;

            while (originalFileIt.hasNext()) {
                originalLine = originalFileIt.nextLine();
                if (++originalLineNum == rowSpan) {
                    break;
                }
            }

            lastFileLine = Integer.parseInt(csFileLine.split(checkSumDelimiter)[1]);
            // Get value by key
            value = rawKVClient.get(ByteString.copyFromUtf8(csKey)).toStringUtf8();
            if (value.isEmpty()) {
                checkSumLog.warn(String.format("The key [%s] is not be inserted! Please confirm whether it is incremental data.", csKey));
                checkNotInsertErrorNum++;
                continue;
            }

            evnId = csKey.split("_:_")[1];
            type = csKey.split("_:_")[2];
            id = csKey.split("_:_")[3];

            IndexInfo indexInfo_checkSum;
            try {
                jsonObject = JSONObject.parseObject(value);
                indexInfo_checkSum = JSON.toJavaObject(jsonObject, IndexInfo.class);
            } catch (Exception e) {
                checkSumLog.error(String.format("Parse failed! Line = %s", value));
                checkParseErrorNum++;
                continue;
            }
            IndexInfo indexInfo_original;
            try {
                jsonObject = JSONObject.parseObject(originalLine);
                indexInfo_original = JSON.toJavaObject(jsonObject, IndexInfo.class);
            } catch (Exception e) {
                checkSumLog.error(String.format("Parse failed! Line = %s", originalLine));
                checkParseErrorNum++;
                continue;
            }

            indexInfo_checkSum.setEnvId(evnId);
            indexInfo_checkSum.setType(type);
            indexInfo_checkSum.setId(id);

            if (!indexInfo_checkSum.equals(indexInfo_original)) {
                checkSumLog.error(String.format("Check sum failed! Line = %s", originalLine));
                checkFailNum++;
            }

        }

        LineIterator.closeQuietly(checkSumFileIt);
        LineIterator.closeQuietly(originalFileIt);

        timer.cancel();

        logger.info(String.format("[%s] check sum over! TotalCheckNum[%s], TotalNotInsertNum[%s], TotalParseErrorNum[%s], TotalCheckFailNum[%s]", checkSumFilePath, totalCheckNum, checkNotInsertErrorNum, checkParseErrorNum, checkFailNum));
    }


//        public static void checkSumTmpIndexInfoJson (String checkSumFilePath, String checkSumDelimiter, TiSession
//        tiSession, File originalFile, Properties properties){
//
//            int totalCheckNum = 0;
//            int checkParseErrorNum = 0;
//            int checkNotInsertErrorNum = 0;
//            int checkFailNum = 0;
//
//            checkSumLog.info(String.format("************ Start data verification for [%s] ************", originalFile));
//            File checkSumFile = new File(checkSumFilePath);
//            BufferedReader originalBufferedReader;
//            BufferedReader checkSumBufferedReader;
//            RawKVClient rawKVClient = tiSession.createRawClient();
//
//            FileInputStream checkSumFileInputStream = null;
//            FileInputStream originalFileInputStream = null;
//            BufferedInputStream checkSumBufferedInputStream = null;
//            BufferedInputStream originalBufferedInputStream = null;
//            InputStreamReader checkSumInputStreamReader = null;
//            InputStreamReader originalInputStreamReader = null;
//
//            try {
//                checkSumFileInputStream = new FileInputStream(checkSumFile);
//                originalFileInputStream = new FileInputStream(originalFile);
//                checkSumBufferedInputStream = new BufferedInputStream(checkSumFileInputStream);
//                originalBufferedInputStream = new BufferedInputStream(originalFileInputStream);
//                checkSumInputStreamReader = new InputStreamReader(checkSumBufferedInputStream);
//                originalInputStreamReader = new InputStreamReader(originalBufferedInputStream);
//
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//            }
//
//            // checkSum file buffer reader
//            checkSumBufferedReader = new BufferedReader(checkSumInputStreamReader);
//            // original file buffer reader
//            originalBufferedReader = new BufferedReader(originalInputStreamReader);
//
//            String originalLine = null;
//            ByteString key;
//            int fileLine;
//            String value;
//            JSONObject jsonObject;
//            String evnId;
//            String type;
//            String id;
//
////        checkSumBufferedReader.readLine();
//            int checkSumFileLineNum = FileUtil.getFileLines(checkSumFile);
//            int lastFileLine = 0;
//
//            String checkSumFileLine = "";
//
//            List<TempIndexInfo> tempIndexInfoList = new ArrayList<>();
//
//            // Calculate the number of rows span.
//            for (int i = 0; i < checkSumFileLineNum; i++) {
//                // check sum file line
//                try {
//                    checkSumFileLine = checkSumBufferedReader.readLine();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                totalCheckNum++;
//                key = ByteString.copyFromUtf8(checkSumFileLine.split(checkSumDelimiter)[0]); // key
//                fileLine = Integer.parseInt(checkSumFileLine.split(checkSumDelimiter)[1]); // line in original file
//                // get value from tikv by key
//                value = rawKVClient.get(key).toStringUtf8();
//                if (value.isEmpty()) {
//                    checkSumLog.warn(String.format("The key [%s] is not be inserted! Please confirm whether it is incremental data.", key.toStringUtf8()));
//                    checkNotInsertErrorNum++;
//                    continue;
//                }
//                String keyString = key.toStringUtf8();
//                evnId = keyString.split("_:_")[1];
//                id = keyString.split("_:_")[2];
//                jsonObject = JSONObject.parseObject(value);
//                TempIndexInfo tempIndexInfo_checkSum = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
//                tempIndexInfo_checkSum.setEnvId(evnId);
//                tempIndexInfo_checkSum.setId(id);
//                // row span
//                tempIndexInfo_checkSum.setFileLine(fileLine - lastFileLine);
//                tempIndexInfoList.add(tempIndexInfo_checkSum);
//                lastFileLine = fileLine;
//            }
//
//            for (TempIndexInfo tempIndexInfo : tempIndexInfoList) {
//                for (int i = 0; i < tempIndexInfo.getFileLine(); i++) {
//                    try {
//                        // read originalLine by rows span
//                        originalLine = originalBufferedReader.readLine();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
//                TempIndexInfo tempIndexInfo_original;
//                try {
//                    jsonObject = JSONObject.parseObject(originalLine);
//                    tempIndexInfo_original = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
//                } catch (Exception e) {
//                    checkSumLog.error(String.format("Parse failed! Json = %s", originalLine));
//                    checkParseErrorNum++;
//                    break;
//                }
//                if (!tempIndexInfo.equals(tempIndexInfo_original)) {
//                    checkSumLog.error(String.format("Check sum failed! Line = %s", originalLine));
//                    checkFailNum++;
//                }
//            }
//            checkSumLog.info(String.format("[%s] check sum over! TotalCheckNum[%s], TotalNotInsertNum[%s], TotalParseErrorNum[%s], TotalCheckFailNum[%s]", checkSumFilePath, totalCheckNum, checkNotInsertErrorNum, checkParseErrorNum, checkFailNum));
//            try {
//                checkSumFileInputStream.close();
//                originalFileInputStream.close();
//                checkSumBufferedInputStream.close();
//                originalBufferedInputStream.close();
//                checkSumInputStreamReader.close();
//                originalInputStreamReader.close();
//                checkSumBufferedReader.close();
//                originalBufferedReader.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
}
