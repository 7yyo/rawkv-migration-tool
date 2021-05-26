package com.pingcap.importer;

import com.pingcap.enums.Model;
import com.pingcap.util.FileUtil;
import com.pingcap.util.TiSessionUtil;
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
import java.util.Properties;

public class IndexType2T {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final HashMap<ByteString, ByteString> kvPairs = new HashMap<>();

    public static void runIndexType(Properties properties, TiSession tiSession) {

        String filePath = properties.getProperty(Model.FILE_PATH);

        List<File> fileList = FileUtil.showFileList(filePath, false, properties);
        RawKVClient rawKVClient = tiSession.createRawClient();

        BufferedReader bufferedReader = null;
        BufferedInputStream bufferedInputStream;

        for (File file : fileList) {
            try {
                bufferedInputStream = new BufferedInputStream(new FileInputStream(file));
                bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream, StandardCharsets.UTF_8));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

            String line;
            ByteString key;
            ByteString value;
            int lineNum = 0;
            int skipNum = 0;

            long startTime = System.currentTimeMillis();

            try {
                while ((line = bufferedReader.readLine()) != null) {
                    lineNum++;
                    if (StringUtils.isBlank(line)) {
                        continue;
                    }
                    try {
                        key = ByteString.copyFromUtf8(line.split("@")[0]);
                        value = ByteString.copyFromUtf8(line.split("@")[1]);
                        kvPairs.put(key, value);
                    } catch (Exception e) {
                        logger.error(String.format("Failed to process key@value string, file='%s', line=%s, k@v='%s'", file, lineNum, line));
                        skipNum++;
                    }
                }
                if (!kvPairs.isEmpty()) {
                    try {
                        rawKVClient.batchPut(kvPairs);
                    } catch (Exception e) {
                        logger.error(String.format("Batch put Tikv failed, file is [ %s ]", file.getAbsolutePath()), e);
                    }
                    kvPairs.clear();
                }
                long duration = System.currentTimeMillis() - startTime;
                logger.info("Import Report: File->[" + file.getAbsolutePath() + "], Total rows->[" + lineNum + "], Imported rows->[" + kvPairs.size() + "], Skip rows->[" + skipNum + "], Duration->[" + duration / 1000 + "s]");
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }


}
