package com.pingcap.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.TempIndexInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CheckSumUtil {

    private static final Logger checkSumLog = LoggerFactory.getLogger("checkSumLog");

    public static BufferedWriter initCheckSumLog(Properties properties, File file) {

        String checkSumFilePath = properties.getProperty("importer.tikv.checkSumFilePath");
        BufferedWriter bufferedWriter = null;

        try {
            String fp = checkSumFilePath.replaceAll("\"", "") + "/" + file.getName().replaceAll("\\.", "") + "/" + Thread.currentThread().getId() + ".txt";
            File f = new File(fp);
            f.getParentFile().mkdirs();
            f.createNewFile();
            FileWriter fileWriter = new FileWriter(f, true);
            bufferedWriter = new BufferedWriter(fileWriter);
//            bufferedWriter.write(file.getAbsolutePath() + "\n");
            bufferedWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bufferedWriter;

    }

    public static void checkSumIndexInfoJson(String checkSumFilePath, String checkSumDelimiter, TiSession tiSession, File originalFile, Properties properties) {

        checkSumLog.info(String.format("************ Start data verification for [%s] ************", checkSumFilePath));

        File checkSumFile = new File(checkSumFilePath);
        BufferedReader originalBufferedReader;
        BufferedReader checkSumBufferedReader;
        RawKVClient rawKVClient = tiSession.createRawClient();

        int checkParseErrorNum = 0;
        int checkNotInsertErrorNum = 0;
        int checkFailNum = 0;
        AtomicInteger totalCheckNum = new AtomicInteger(0);

        int checkSumFileLineNum = FileUtil.getFileLines(checkSumFile);

        try {
            // checkSum file buffer reader
            checkSumBufferedReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(checkSumFile)), StandardCharsets.UTF_8));
            // original file buffer reader
            originalBufferedReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(originalFile)), StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            checkSumLog.error("Failed to read the original/checkSum file");
            return;
        }

        String originalLine = null;
        ByteString key = null;
        String value;
        JSONObject jsonObject;
        String evnId;
        String type;
        String id;

//        checkSumBufferedReader.readLine();
        int lastFileLine = 0;

        CheckSumTimer checkSumTimer = new CheckSumTimer(checkSumFilePath, totalCheckNum, checkSumFileLineNum, properties);
        checkSumTimer.start();

        String csFileLine = "";

        try {

            int csFileLineNum = 0;
            String csKey = "";

            for (int i = 0; i < checkSumFileLineNum; i++) {
                totalCheckNum.addAndGet(1);
                try {
                    csFileLine = checkSumBufferedReader.readLine();
                    csKey = csFileLine.split(checkSumDelimiter)[0];
                    csFileLineNum = Integer.parseInt(csFileLine.split(checkSumDelimiter)[1]);
                } catch (Exception e) {
                    checkParseErrorNum++;
                    checkSumLog.error(String.format("Parse failed! Line = %s"), csFileLine);
                    continue;
                }

                int n = csFileLineNum - lastFileLine;
                for (int j = 0; j < n; j++) {
                    originalLine = originalBufferedReader.readLine();
                }
                lastFileLine = Integer.parseInt(csFileLine.split(checkSumDelimiter)[1]);

                value = rawKVClient.get(ByteString.copyFromUtf8(csKey)).toStringUtf8();

                if (value.isEmpty()) {
                    checkSumLog.warn(String.format("The key [%s] is not be inserted! Please confirm whether it is incremental data.", csKey));
                    checkNotInsertErrorNum++;
                    continue;
                }

                evnId = csKey.split("_:_")[1];
                type = csKey.split("_:_")[2];
                id = csKey.split("_:_")[3];

                jsonObject = JSONObject.parseObject(value);
                IndexInfo indexInfo_checkSum = JSON.toJavaObject(jsonObject, IndexInfo.class);
                indexInfo_checkSum.setEnvId(evnId);
                indexInfo_checkSum.setType(type);
                indexInfo_checkSum.setId(id);

                IndexInfo indexInfo_original;
                try {
                    jsonObject = JSONObject.parseObject(originalLine);
                    indexInfo_original = JSON.toJavaObject(jsonObject, IndexInfo.class);
                } catch (Exception e) {
                    checkSumLog.error(String.format("Parse failed! Line = %s", originalLine));
                    checkParseErrorNum++;
                    continue;
                }
                if (!indexInfo_checkSum.equals(indexInfo_original)) {
                    checkSumLog.error(String.format("Check sum failed! Line = %s", originalLine));
                    checkFailNum++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                originalBufferedReader.close();
                checkSumBufferedReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        checkSumLog.info(String.format("[%s] check sum over! TotalCheckNum[%s], TotalNotInsertNum[%s], TotalParseErrorNum[%s], TotalCheckFailNum[%s]", checkSumFilePath, totalCheckNum, checkNotInsertErrorNum, checkParseErrorNum, checkFailNum));
    }


    public static void checkSumTmpIndexInfoJson(String checkSumFilePath, String checkSumDelimiter, TiSession
            tiSession, File originalFile, Properties properties) {

        int totalCheckNum = 0;
        int checkParseErrorNum = 0;
        int checkNotInsertErrorNum = 0;
        int checkFailNum = 0;

        checkSumLog.info(String.format("************ Start data verification for [%s] ************", originalFile));
        File checkSumFile = new File(checkSumFilePath);
        BufferedReader originalBufferedReader;
        BufferedReader checkSumBufferedReader;
        RawKVClient rawKVClient = tiSession.createRawClient();

        FileInputStream checkSumFileInputStream = null;
        FileInputStream originalFileInputStream = null;
        BufferedInputStream checkSumBufferedInputStream = null;
        BufferedInputStream originalBufferedInputStream = null;
        InputStreamReader checkSumInputStreamReader = null;
        InputStreamReader originalInputStreamReader = null;

        try {
            checkSumFileInputStream = new FileInputStream(checkSumFile);
            originalFileInputStream = new FileInputStream(originalFile);
            checkSumBufferedInputStream = new BufferedInputStream(checkSumFileInputStream);
            originalBufferedInputStream = new BufferedInputStream(originalFileInputStream);
            checkSumInputStreamReader = new InputStreamReader(checkSumBufferedInputStream);
            originalInputStreamReader = new InputStreamReader(originalBufferedInputStream);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        // checkSum file buffer reader
        checkSumBufferedReader = new BufferedReader(checkSumInputStreamReader);
        // original file buffer reader
        originalBufferedReader = new BufferedReader(originalInputStreamReader);

        String originalLine = null;
        ByteString key;
        int fileLine;
        String value;
        JSONObject jsonObject;
        String evnId;
        String type;
        String id;

//        checkSumBufferedReader.readLine();
        int checkSumFileLineNum = FileUtil.getFileLines(checkSumFile);
        int lastFileLine = 0;

        String checkSumFileLine = "";

        List<TempIndexInfo> tempIndexInfoList = new ArrayList<>();

        // Calculate the number of rows span.
        for (int i = 0; i < checkSumFileLineNum; i++) {
            // check sum file line
            try {
                checkSumFileLine = checkSumBufferedReader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            totalCheckNum++;
            key = ByteString.copyFromUtf8(checkSumFileLine.split(checkSumDelimiter)[0]); // key
            fileLine = Integer.parseInt(checkSumFileLine.split(checkSumDelimiter)[1]); // line in original file
            // get value from tikv by key
            value = rawKVClient.get(key).toStringUtf8();
            if (value.isEmpty()) {
                checkSumLog.warn(String.format("The key [%s] is not be inserted! Please confirm whether it is incremental data.", key.toStringUtf8()));
                checkNotInsertErrorNum++;
                continue;
            }
            String keyString = key.toStringUtf8();
            evnId = keyString.split("_:_")[1];
            id = keyString.split("_:_")[2];
            jsonObject = JSONObject.parseObject(value);
            TempIndexInfo tempIndexInfo_checkSum = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
            tempIndexInfo_checkSum.setEnvId(evnId);
            tempIndexInfo_checkSum.setId(id);
            // row span
            tempIndexInfo_checkSum.setFileLine(fileLine - lastFileLine);
            tempIndexInfoList.add(tempIndexInfo_checkSum);
            lastFileLine = fileLine;
        }

        for (TempIndexInfo tempIndexInfo : tempIndexInfoList) {
            for (int i = 0; i < tempIndexInfo.getFileLine(); i++) {
                try {
                    // read originalLine by rows span
                    originalLine = originalBufferedReader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            TempIndexInfo tempIndexInfo_original;
            try {
                jsonObject = JSONObject.parseObject(originalLine);
                tempIndexInfo_original = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
            } catch (Exception e) {
                checkSumLog.error(String.format("Parse failed! Json = %s", originalLine));
                checkParseErrorNum++;
                break;
            }
            if (!tempIndexInfo.equals(tempIndexInfo_original)) {
                checkSumLog.error(String.format("Check sum failed! Line = %s", originalLine));
                checkFailNum++;
            }
        }
        checkSumLog.info(String.format("[%s] check sum over! TotalCheckNum[%s], TotalNotInsertNum[%s], TotalParseErrorNum[%s], TotalCheckFailNum[%s]", checkSumFilePath, totalCheckNum, checkNotInsertErrorNum, checkParseErrorNum, checkFailNum));
        try {
            checkSumFileInputStream.close();
            originalFileInputStream.close();
            checkSumBufferedInputStream.close();
            originalBufferedInputStream.close();
            checkSumInputStreamReader.close();
            originalInputStreamReader.close();
            checkSumBufferedReader.close();
            originalBufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
