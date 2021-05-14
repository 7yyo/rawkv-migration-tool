package com.pingcap.importer;

import com.pingcap.util.FileUtil;
import com.pingcap.util.PropertiesUtil;
import com.pingcap.util.TiSessionUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import shade.com.google.protobuf.ByteString;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class IndexTypeS2T {

    // indexType_:_{envid}_:_{appid}
    private static final String INDEX_TYPE_KET_FORMAT = "indexType_:_%s_:_%s";

    private static final Logger logger = Logger.getLogger(IndexTypeS2T.class);

    private static final Properties properties = PropertiesUtil.getProperties();
    private static final String importFilesPath_indexTypes = properties.getProperty("importer.in.importFilesPath_indexTypes");

    private static HashMap<ByteString, ByteString> hashMap = new HashMap<>();

    private static final TiSession tiSession = TiSessionUtil.getTiSession();

    public static void main(String[] args) {

        logger.info(String.format("Welcome to TiKV importer."));
        List<File> fileList = FileUtil.loadDirectory(new File(importFilesPath_indexTypes));
        logger.info(">>>>>>>>>> Need to import the following files.>>>>>>>>>>");
        if (fileList.isEmpty()) {
            logger.error("This filePath has no file.");
        } else {
            for (File file : fileList) {
                int line = FileUtil.getFileLines(file);
                logger.info(String.format("[ file ] { %s } , [ line ] { %d }.", file.getAbsolutePath(), line));
            }
        }
        logger.info(String.format(">>>>>>>>>>>>>>>>>>>>>>>>>>>> Total file is [ %s ] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>", fileList.size()));

        RawKVClient rawKVClient = tiSession.createRawClient();
        BufferedReader bufferedReader = null;
        for (File file : fileList) {
            try {
                BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));
                bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream, StandardCharsets.UTF_8));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            String line;
            ByteString key;
            ByteString value;
            try {
                while ((line = bufferedReader.readLine()) != null) {
                    if (StringUtils.isBlank(line)) {
                        continue;
                    }
                    try {
                        key = ByteString.copyFromUtf8(line.split("@")[0]);
                        value = ByteString.copyFromUtf8(line.split("@")[1]);
                        hashMap.put(key, value);
                    } catch (Exception e) {
                        logger.error(String.format("Parse file [ %s ] failed!", file.getAbsolutePath()));
                        return;
                    }
                }
                if (!hashMap.isEmpty()) {
                    try {
                        rawKVClient.batchPut(hashMap);
                    } catch (Exception e) {
                        logger.error(String.format("Batch put Tikv failed, file is [ %s ]", file.getAbsolutePath()), e);
                    }
                    hashMap.clear();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }


}
