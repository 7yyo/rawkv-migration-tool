package com.pingcap.importer;

import com.pingcap.enums.Model;
import com.pingcap.util.FileUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexTypeImporter {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    public static void run(Map<String, String> properties, TiSession tiSession) {

        long startTime = System.currentTimeMillis();

        String importFilePath = properties.get(Model.IMPORT_FILE_PATH);
        List<File> fileList = FileUtil.showFileList(importFilePath);

        RawKVClient rawKvClient = tiSession.createRawClient();

        FileInputStream fileInputStream;
        BufferedReader bufferedReader;
        BufferedInputStream bufferedInputStream;

        HashMap<ByteString, ByteString> kvParis = new HashMap<>();

        String fileLine;
        ByteString key, value;
        int lineNum = 0, errorNum = 0;
        for (File importFile : fileList) {

            try {

                fileInputStream = new FileInputStream(importFile);
                bufferedInputStream = new BufferedInputStream(fileInputStream);
                bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream, StandardCharsets.UTF_8));

                while ((fileLine = bufferedReader.readLine()) != null) {
                    lineNum++;
                    // Blank space.
                    if (StringUtils.isBlank(fileLine)) {
                        continue;
                    }
                    try {
                        key = ByteString.copyFromUtf8(fileLine.split(Model.INDEX_TYPE_DELIMITER)[0]);
                        value = ByteString.copyFromUtf8(fileLine.split(Model.INDEX_TYPE_DELIMITER)[1]);
                        kvParis.put(key, value);
                    } catch (Exception e) {
                        logger.error("Process failed, file[{}], lineNum[{}], line[{}]", importFile, lineNum, fileLine, e);
                        errorNum++;
                    }
                }

                if (!kvParis.isEmpty()) {
                    try {
                        rawKvClient.batchPut(kvParis);
                    } catch (Exception e) {
                        logger.error("Failed to batch put raw kv, file[{}]", importFile.getAbsolutePath(), e);
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            long duration = System.currentTimeMillis() - startTime;
            logger.info("[Import Report] File[" + importFile.getAbsolutePath() + "], Total[" + lineNum + "], Imported[" + kvParis.size() + "], Error[" + errorNum + "], Duration[" + duration / 1000 + "]s");
            kvParis.clear();

        }
    }


}
