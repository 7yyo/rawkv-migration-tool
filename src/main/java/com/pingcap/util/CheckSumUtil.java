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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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

    public static void checkSumIndexInfoJson(String checkSumFilePath, String checkSumDelimiter, TiSession tiSession, File originalFile) {

        int totalCheckNum = 0;
        int checkParseErrorNum = 0;
        int checkNotInsertErrorNum = 0;
        int checkFailNum = 0;
//        checkSumLog.info(String.format("************ Start data verification for [%s] ************", originalFile));
        File checkSumFile = new File(checkSumFilePath);
        BufferedReader originalBufferedReader;
        BufferedReader checkSumBufferedReader;
        RawKVClient rawKVClient = tiSession.createRawClient();

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

        List<IndexInfo> indexInfoSList = new ArrayList<>();

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
            type = keyString.split("_:_")[2];
            id = keyString.split("_:_")[3];
            jsonObject = JSONObject.parseObject(value);
            IndexInfo indexInfo_checkSum = JSON.toJavaObject(jsonObject, IndexInfo.class);
            indexInfo_checkSum.setEnvId(evnId);
            indexInfo_checkSum.setType(type);
            indexInfo_checkSum.setId(id);
            // row span
            indexInfo_checkSum.setFileLine(fileLine - lastFileLine);
            indexInfoSList.add(indexInfo_checkSum);
            lastFileLine = fileLine;
        }

        for (IndexInfo indexInfo : indexInfoSList) {
            for (int i = 0; i < indexInfo.getFileLine(); i++) {
                try {
                    // read originalLine by rows span
                    originalLine = originalBufferedReader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            IndexInfo indexInfo_original;
            try {
                jsonObject = JSONObject.parseObject(originalLine);
                indexInfo_original = JSON.toJavaObject(jsonObject, IndexInfo.class);
            } catch (Exception e) {
                checkSumLog.error(String.format("Parse failed! Json = %s", originalLine));
                checkParseErrorNum++;
                break;
            }
            if (!indexInfo.equals(indexInfo_original)) {
                checkSumLog.error(String.format("Check sum failed! Line = %s", originalLine));
                checkFailNum++;
            }
        }
        checkSumLog.info(String.format("[%s] check sum over! TotalCheckNum[%s], TotalNotInsertNum[%s], TotalParseErrorNum[%s], TotalCheckFailNum[%s]", checkSumFilePath, totalCheckNum, checkNotInsertErrorNum, checkParseErrorNum, checkFailNum));
    }


    public static void checkSumTmpIndexInfoJson(String checkSumFilePath, String checkSumDelimiter, TiSession tiSession, File originalFile) {

        int totalCheckNum = 0;
        int checkParseErrorNum = 0;
        int checkNotInsertErrorNum = 0;
        int checkFailNum = 0;
//        checkSumLog.info(String.format("************ Start data verification for [%s] ************", originalFile));
        File checkSumFile = new File(checkSumFilePath);
        BufferedReader originalBufferedReader;
        BufferedReader checkSumBufferedReader;
        RawKVClient rawKVClient = tiSession.createRawClient();

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
    }

}
