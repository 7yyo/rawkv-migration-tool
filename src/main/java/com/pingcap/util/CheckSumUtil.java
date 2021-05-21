package com.pingcap.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.pojo.IndexInfo;
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

    public static BufferedWriter initCheckSumLog(Properties properties, File mainFile, File file) {

        String checkSumFilePath = properties.getProperty("importer.tikv.checkSumFilePath");

        BufferedWriter bufferedWriter = null;

        try {
            String fp = checkSumFilePath.replaceAll("\"", "") + "/" + mainFile.getName().replaceAll("\\.", "") + "/" + Thread.currentThread().getId() + ".txt";
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

    public static void checkSumIndexInfo(String checkSumFilePath, String checkSumDelimiter, TiSession tiSession, File originalFile, String mode, Properties properties) {

//        checkSumLog.info(String.format("************ Start data verification for [%s] ************", originalFile));

        File file = new File(checkSumFilePath);
        BufferedReader checkSumBufferedReader;
        BufferedReader originalBufferedReader;
        RawKVClient rawKVClient = tiSession.createRawClient();

        String delimiter_1 = properties.getProperty("importer.in.delimiter_1");
        String delimiter_2 = properties.getProperty("importer.in.delimiter_2");

        try {
            // checkSum file buffer reader
            checkSumBufferedReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(file)), StandardCharsets.UTF_8));
            // original file buffer reader
            originalBufferedReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(originalFile)), StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            checkSumLog.error("Failed to read the original or checkSum file");
            return;
        }

        String checkSumFileLine = "";
        String originalLine = null;
        ByteString key = null;
        int fileLine = 0;
        String value;
        JSONObject jsonObject;
        String evnId;
        String type;
        String id;

        List<IndexInfo> indexInfoSList = new ArrayList<>();

        int totalCheckNum = 0;
        int checkParseErrorNum = 0;
        int checkNotInsertErrorNum = 0;
        int checkFailNum = 0;

//            checkSumBufferedReader.readLine();
        int fLine = FileUtil.getFileLines(file);

        int n = 0;
        int lastFileLine = 0;

        for (int i = 0; i < fLine; i++) {
            // check sum file line
            try {
                checkSumFileLine = checkSumBufferedReader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            IndexInfo indexInfo_checkSum = new IndexInfo();
            totalCheckNum++;
            key = ByteString.copyFromUtf8(checkSumFileLine.split(checkSumDelimiter)[0]);
            fileLine = Integer.parseInt(checkSumFileLine.split(checkSumDelimiter)[1]);
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
            indexInfo_checkSum = JSON.toJavaObject(jsonObject, IndexInfo.class);
            indexInfo_checkSum.setEnvId(evnId);
            indexInfo_checkSum.setType(type);
            indexInfo_checkSum.setId(id);
            indexInfo_checkSum.setFileLine(fileLine - lastFileLine);
            indexInfoSList.add(indexInfo_checkSum);
            n++;
            lastFileLine = fileLine;
        }

        for (IndexInfo indexInfo : indexInfoSList) {
            IndexInfo indexINfoT_bf = null;
            for (int i = 0; i < indexInfo.getFileLine(); i++) {
                try {
                    originalLine = originalBufferedReader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            switch (mode) {
                case "json":
                    indexINfoT_bf = new IndexInfo();
                    try {
                        jsonObject = JSONObject.parseObject(originalLine);
                        indexINfoT_bf = JSON.toJavaObject(jsonObject, IndexInfo.class);
                    } catch (Exception e) {
                        checkSumLog.error(String.format("Parse failed! Json = %s", originalLine));
                        checkParseErrorNum++;
                        break;
                    }
                    break;
                case "original":
                    break;
                default:
            }
            if (!indexInfo.equals(indexINfoT_bf)) {
                checkSumLog.error(String.format("Check sum failed! Line = %s", originalLine));
                checkFailNum++;
            }
        }
        checkSumLog.info(String.format("[%s] check sum over! TotalCheckNum[%s], TotalNotInsertNum[%s], TotalParseErrorNum[%s], TotalCheckFailNum[%s]", checkSumFilePath, totalCheckNum, checkNotInsertErrorNum, checkParseErrorNum, checkFailNum));
    }

}
