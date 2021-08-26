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

        FileInputStream fileInputStream;
        BufferedReader bufferedReader;
        BufferedInputStream bufferedInputStream;

        String content;
        ByteString key, value;
        int fileLineNum = 0, errorNum = 0;
        HashMap<ByteString, ByteString> kvParis = new HashMap<>();

        RawKVClient rawKvClient = tiSession.createRawClient();
        List<File> fileList = FileUtil.showFileList(properties.get(Model.IMPORT_FILE_PATH), false);
        for (File file : fileList) {
            try {
                fileInputStream = new FileInputStream(file);
                bufferedInputStream = new BufferedInputStream(fileInputStream);
                bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream, StandardCharsets.UTF_8));

                while ((content = bufferedReader.readLine()) != null) {
                    fileLineNum++;
                    // Skip spaces.
                    if (StringUtils.isBlank(content)) {
                        continue;
                    }
                    try {
                        key = ByteString.copyFromUtf8(content.split(Model.INDEX_TYPE_DELIMITER)[0]);
                        value = ByteString.copyFromUtf8(content.split(Model.INDEX_TYPE_DELIMITER)[1]);
                        kvParis.put(key, value);
                    } catch (Exception e) {
                        logger.error("Parse failed, file={}, line={}, content={}", file, fileLineNum, content, e);
                        errorNum++;
                    }
                }
                if (!kvParis.isEmpty()) {
                    try {
                        rawKvClient.batchPut(kvParis);
                    } catch (Exception e) {
                        logger.error("Batch put failed, file={}", file.getAbsolutePath(), e);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            long duration = System.currentTimeMillis() - startTime;
            logger.info("Import summary: File=" + file.getAbsolutePath() + ", total=" + fileLineNum + ", imported=" + kvParis.size() + ", errNum=" + errorNum + ", duration=" + duration / 1000 + "s");
            kvParis.clear();
        }

        rawKvClient.close();

    }

}
