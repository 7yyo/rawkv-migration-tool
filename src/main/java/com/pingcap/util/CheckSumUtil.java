package com.pingcap.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.pojo.IndexInfoS;
import com.pingcap.pojo.IndexInfoT;
import org.apache.log4j.Logger;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CheckSumUtil {

    private static final Logger logger = Logger.getLogger(CheckSumUtil.class);

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
            bufferedWriter.write(file.getAbsolutePath() + "\n");
            bufferedWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bufferedWriter;
    }

    public static void checkSumIndexInfo(String checkSumFilePath, String checkSumDelimiter, TiSession tiSession, String ttlType, File originalFile) {
        logger.info(String.format("Start data verification for %s", originalFile));
        File file = new File(checkSumFilePath);
        BufferedReader checkSumBufferedReader;
        RawKVClient rawKVClient = tiSession.createRawClient();
        try {
            checkSumBufferedReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(file)), StandardCharsets.UTF_8));
            String line;
            String originalLine;
            ByteString key;
            int fileLine = 0;
            String value;
            JSONObject jsonObject;
            String evnId;
            String type;
            String id;
            List<IndexInfoS> indexInfoSList = new ArrayList<>();
            checkSumBufferedReader.readLine();
            int i = 0;
            while ((line = checkSumBufferedReader.readLine()) != null) {
                key = ByteString.copyFromUtf8(line.split(checkSumDelimiter)[0]);
                try {
                    fileLine = Integer.parseInt(line.split(checkSumDelimiter)[1]);
                } catch (Exception e) {
                    logger.error(line);
                }
                value = rawKVClient.get(key).toStringUtf8();
                if (value.isEmpty()) {
                    logger.warn(String.format("The key %s is not inserted, please confirm the reason.", key.toStringUtf8()));
                    continue;
                }

                IndexInfoS indexInfoS_af = null;

                String s = key.toStringUtf8();
                evnId = s.split("_:_")[1];
                type = s.split("_:_")[2];
                if (ttlType.contains(type)) {
                    logger.warn(String.format("The key %s is not inserted, please confirm the reason.", key.toStringUtf8()));
                    continue;
                }
                id = s.split("_:_")[3];

                jsonObject = JSONObject.parseObject(value);
                try {
                    indexInfoS_af = JSON.toJavaObject(jsonObject, IndexInfoS.class);
                } catch (Exception e) {
                    logger.error(line);
                }

                assert indexInfoS_af != null;
                indexInfoS_af.setEnvId(evnId);
                indexInfoS_af.setType(type);
                indexInfoS_af.setId(id);
                indexInfoS_af.setFileLine(fileLine);
                indexInfoSList.add(indexInfoS_af);

            }

            for (IndexInfoS indexInfoS : indexInfoSList) {
                BufferedReader bufferedReader = null;
                try {
                    bufferedReader = new BufferedReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(originalFile)), StandardCharsets.UTF_8));
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                if (bufferedReader != null) {
                    for (int j = 0; j < indexInfoS.getFileLine(); j++) {
                        originalLine = bufferedReader.readLine();
                        if (j == indexInfoS.getFileLine() - 1) {
                            IndexInfoT indexINfoT_bf = new IndexInfoT();
                            try {
                                jsonObject = JSONObject.parseObject(originalLine);
                                indexINfoT_bf = JSON.toJavaObject(jsonObject, IndexInfoT.class);
                            } catch (Exception e) {
                                logger.error(String.format("Check sum failed! %s == %s", indexInfoS.getId(), indexINfoT_bf.getId()));
                            }
                            if (!indexInfoS.getId().equals(indexINfoT_bf.getId())) {
                                logger.error(String.format("Check sum failed! %s == %s", indexInfoS.getId(), indexINfoT_bf.getId()));
                            }
                        }
                    }
                }
            }

            logger.info("CheckSum success!");

        } catch (IOException e) {
            logger.error(String.format("CheckSum failed, %s", e));
            e.printStackTrace();
        }
//        }
    }

}
