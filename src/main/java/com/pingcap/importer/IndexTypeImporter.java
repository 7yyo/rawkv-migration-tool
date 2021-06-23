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
import java.util.Properties;

/**
 * @author yuyang
 */
public class IndexTypeImporter {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final HashMap<ByteString, ByteString> KV_PAIRS = new HashMap<>();
    private static final String INDEX_TYPE_DELIMITER = "@";

    public static void runIndexType(Properties properties, TiSession tiSession) {

        String filePath = properties.getProperty(Model.FILE_PATH);
        List<File> fileList = FileUtil.showFileList(filePath, false);

        BufferedReader bufferedReader = null;
        BufferedInputStream bufferedInputStream;

        RawKVClient rawKvClient = tiSession.createRawClient();

        if (fileList != null) {
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
                int errorNum = 0;
                long startTime = System.currentTimeMillis();

                try {
                    if (bufferedReader != null) {
                        while ((line = bufferedReader.readLine()) != null) {
                            lineNum++;
                            if (StringUtils.isBlank(line)) {
                                continue;
                            }
                            try {
                                key = ByteString.copyFromUtf8(line.split(INDEX_TYPE_DELIMITER)[0]);
                                value = ByteString.copyFromUtf8(line.split(INDEX_TYPE_DELIMITER)[1]);
                                KV_PAIRS.put(key, value);
                            } catch (Exception e) {
                                logger.error(String.format("Failed to parse K@V, file='%s', line=%s, k@v='%s'", file, lineNum, line), e);
                                errorNum++;
                            }
                        }
                    }

                    if (!KV_PAIRS.isEmpty()) {
                        try {
                            rawKvClient.batchPut(KV_PAIRS);
                        } catch (Exception e) {
                            logger.error(String.format("Batch put Tikv failed, file is [ %s ]", file.getAbsolutePath()), e);
                        }
                    }

                    long duration = System.currentTimeMillis() - startTime;
                    logger.info("[Import Report] File[" + file.getAbsolutePath() + "], Total rows[" + lineNum + "], Imported rows[" + KV_PAIRS.size() + "], Error rows[" + errorNum + "], Duration[" + duration / 1000 + "s]");
                    KV_PAIRS.clear();

                    System.exit(0);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}