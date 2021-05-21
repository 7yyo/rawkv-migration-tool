package com.pingcap.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.pojo.IndexInfoS;
import com.pingcap.pojo.IndexInfoT;
import com.pingcap.pojo.ServiceTag;
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
        FileWriter fileWriter;

        try {
            String fp = checkSumFilePath.replaceAll("\"", "") + "/" + mainFile.getName().replaceAll("\\.", "") + "/" + Thread.currentThread().getId() + ".txt";
            File f = new File(fp);
            f.getParentFile().mkdirs();
            f.createNewFile();
            fileWriter = new FileWriter(f, true);
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

            String line;
            String originalLine = null;
            ByteString key = null;
            int fileLine = 0;
            String value;
            JSONObject jsonObject;
            String evnId;
            String type;
            String id;

            List<IndexInfoS> indexInfoSList = new ArrayList<>();

            int totalCheckNum = 0;
            int checkParseErrorNum = 0;
            int checkNotInsertErrorNum = 0;
            int checkFailNum = 0;
            int checkTtlNum = 0;

//            checkSumBufferedReader.readLine();
            int fLine = FileUtil.getFileLines(file);

            int n = 0;
            int lastFileLine = 0;

            for (int i = 0; i < fLine; i++) {
                line = checkSumBufferedReader.readLine();
                IndexInfoS indexInfoS_af = new IndexInfoS();
                totalCheckNum++;
                // PARSE ERROR - It means that the data line that failed to parse json during the previous import.
                if (!line.contains("PARSE ERROR")) {
                    key = ByteString.copyFromUtf8(line.split(checkSumDelimiter)[0]);
                    fileLine = Integer.parseInt(line.split(checkSumDelimiter)[1]);
                    value = rawKVClient.get(key).toStringUtf8();
                    // value = null - It means that this piece of data is not inserted, it may be a bug, it may be a TTL type
                    if (value.isEmpty()) {
                        checkSumLog.warn(String.format("The key [%s] is not be inserted, please confirm the reason.", key.toStringUtf8(), value));
                        checkNotInsertErrorNum++;
                        n++;
                        indexInfoS_af.setType("NULLBeanHA");
                        indexInfoS_af.setFileLine(fileLine - lastFileLine);
                        lastFileLine = fileLine;
                        indexInfoSList.add(indexInfoS_af);
                        continue;
                    }
                    String keyString = key.toStringUtf8();
                    evnId = keyString.split("_:_")[1];
                    type = keyString.split("_:_")[2];
                    id = keyString.split("_:_")[3];

                    jsonObject = JSONObject.parseObject(value);
                    indexInfoS_af = JSON.toJavaObject(jsonObject, IndexInfoS.class);
                    indexInfoS_af.setEnvId(evnId);
                    indexInfoS_af.setType(type);
                    indexInfoS_af.setId(id);
                    indexInfoS_af.setFileLine(fileLine - lastFileLine);
                    indexInfoSList.add(indexInfoS_af);
                } else {
                    // PARSE ERROR, add checkParseErrorNum.
                    checkParseErrorNum++;
                    indexInfoS_af.setType(line);
                    indexInfoSList.add(indexInfoS_af);
                }

                n++;
                lastFileLine = fileLine;
            }

            for (IndexInfoS indexInfoS : indexInfoSList) {
                IndexInfoT indexINfoT_bf = null;

                for (int i = 0; i < indexInfoS.getFileLine(); i++) {
                    originalLine = originalBufferedReader.readLine();
                }
                switch (mode) {
                    case "json":
                        indexINfoT_bf = new IndexInfoT();
                        try {
                            jsonObject = JSONObject.parseObject(originalLine);
                            indexINfoT_bf = JSON.toJavaObject(jsonObject, IndexInfoT.class);
                        } catch (Exception e) {
                            checkSumLog.error(String.format("Parse failed! json == %s", originalLine));
                            break;
                        }
                        break;
                    default:
                }
                try {
                    // If it is not not inserted or json parsing failed, then compare
                    if (!"NULLBeanHA".equals(indexInfoS.getType()) && !indexInfoS.getType().contains("PARSE ERROR")) {
                        if (!indexInfoS.equals(indexINfoT_bf)) {
                            checkSumLog.error(String.format("Check sum failed! %s == %s", indexInfoS.getId(), indexINfoT_bf.getId()));
                            checkFailNum++;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }


//                for (int i = 0; i < indexInfoS.getFileLine(); i++) {
//
//                    originalLine = originalBufferedReader.readLine();
//                    if ("NULLBeanHA".equals(indexInfoS.getType())) {
//                        break;
//                    }
//                    switch (mode) {
//                        case "json":
//                            indexINfoT_bf = new IndexInfoT();
//                            try {
//                                jsonObject = JSONObject.parseObject(originalLine);
//                                indexINfoT_bf = JSON.toJavaObject(jsonObject, IndexInfoT.class);
//                            } catch (Exception e) {
//                                checkSumLog.error(String.format("Parse failed! %s == %s", indexInfoS.getId(), indexINfoT_bf.getId()));
//                                checkParseErrorNum++;
//                                continue;
//                            }
//                            break;
//                        case "original":
//                            String v = originalLine.split(delimiter_1)[2];
//                            String targetId = v.split(delimiter_2)[0];
//                            ServiceTag serviceTag = new ServiceTag();
//                            serviceTag.setBLKMDL_ID(v.split(delimiter_2)[1]);
//                            serviceTag.setPD_SALE_FTA_CD(v.split(delimiter_2)[2]);
//                            serviceTag.setACCT_DTL_TYPE(v.split(delimiter_2)[3]);
//                            serviceTag.setTu_FLAG(v.split(delimiter_2)[4]);
//                            serviceTag.setCMTRST_CST_ACCNO(v.split(delimiter_2)[5]);
//                            serviceTag.setAR_ID(v.split(delimiter_2)[6]);
//                            serviceTag.setQCRCRD_IND("");
//
//                            String serviceTagJson = JSON.toJSONString(serviceTag);
//
//                            indexINfoT_bf = new IndexInfoT();
//                            indexINfoT_bf.setAppId(properties.getProperty("importer.out.appId"));
//                            indexINfoT_bf.setServiceTag(serviceTagJson);
//                            indexINfoT_bf.setTargetId(targetId);
//                            indexINfoT_bf.setUpdateTime(null);
//                            break;
//                        default:
//                            throw new IllegalStateException("Unexpected value: " + mode);
//                    }
//                }
//                try {
//                    if (!indexInfoS.equals(indexINfoT_bf)) {
//                        checkSumLog.error(String.format("Check sum failed! %s == %s", indexInfoS.getId(), indexINfoT_bf.getId()));
//                        checkFailNum++;
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
                //###### zuichu de luoji
//                if (originalBufferedReader != null) {
//                    for (int j = 0; j < indexInfoS.getFileLine(); j++) {
//                        originalLine = bufferedReader.readLine();
//                        IndexInfoT indexINfoT_bf = null;
//                        if (j == indexInfoS.getFileLine() - 1) {
//                            switch (mode) {
//                                case "json":
//                                    indexINfoT_bf = new IndexInfoT();
//                                    try {
//                                        jsonObject = JSONObject.parseObject(originalLine);
//                                        indexINfoT_bf = JSON.toJavaObject(jsonObject, IndexInfoT.class);
//                                    } catch (Exception e) {
//                                        checkSumLog.error(String.format("Parse failed! %s == %s", indexInfoS.getId(), indexINfoT_bf.getId()));
//                                        checkParseErrorNum++;
//                                    }
//                                    break;
//                                case "original":
//                                    String v = originalLine.split(delimiter_1)[2];
//                                    String targetId = v.split(delimiter_2)[0];
//                                    ServiceTag serviceTag = new ServiceTag();
//                                    serviceTag.setBLKMDL_ID(v.split(delimiter_2)[1]);
//                                    serviceTag.setPD_SALE_FTA_CD(v.split(delimiter_2)[2]);
//                                    serviceTag.setACCT_DTL_TYPE(v.split(delimiter_2)[3]);
//                                    serviceTag.setTu_FLAG(v.split(delimiter_2)[4]);
//                                    serviceTag.setCMTRST_CST_ACCNO(v.split(delimiter_2)[5]);
//                                    serviceTag.setAR_ID(v.split(delimiter_2)[6]);
//                                    serviceTag.setQCRCRD_IND("");
//
//                                    String serviceTagJson = JSON.toJSONString(serviceTag);
//
//                                    indexINfoT_bf = new IndexInfoT();
//                                    indexINfoT_bf.setAppId(properties.getProperty("importer.out.appId"));
//                                    indexINfoT_bf.setServiceTag(serviceTagJson);
//                                    indexINfoT_bf.setTargetId(targetId);
//                                    indexINfoT_bf.setUpdateTime(null);
//                                    break;
//                                default:
//                                    throw new IllegalStateException("Unexpected value: " + mode);
//                            }
//                            if (!indexInfoS.equals(indexINfoT_bf)) {
//                                checkSumLog.error(String.format("Check sum failed! %s == %s", indexInfoS.getId(), indexINfoT_bf.getId()));
//                                checkFailNum++;
//                            }
//                        }
//                    }
//                }
                //###### zuichu de luoji
            }

            checkSumLog.info(String.format("[%s] check sum over! TotalCheckNum[%s], TotalNotInsertNum[%s], TotalParseErrorNum[%s], TotalCheckFailNum[%s]", checkSumFilePath, totalCheckNum, checkNotInsertErrorNum, checkParseErrorNum, checkFailNum));

        } catch (IOException e) {
            checkSumLog.error(String.format("CheckSum failed, %s", e));
            e.printStackTrace();
        }
    }

}
