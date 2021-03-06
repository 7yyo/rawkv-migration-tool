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

        String content;
        String[] data;
        ByteString key, value;

        HashMap<ByteString, ByteString> kvParis = new HashMap<>();

        RawKVClient rawKvClient = tiSession.createRawClient();
        final String rollbackMode = properties.get(Model.ROLLBACK);

        List<File> fileList = FileUtil.showFileList(properties.get(Model.IMPORT_FILE_PATH), false);

        for (File file : fileList) {

            long importStartTime = System.currentTimeMillis();
            int fileLineNum = 0, blankNum = 0, parseErrNum = 0, putErr = 0;

            try {

                FileInputStream fileInputStream = new FileInputStream(file);
                BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream, StandardCharsets.UTF_8));

                while ((content = bufferedReader.readLine()) != null) {
                    fileLineNum++;
                    // Skip spaces.
                    if (StringUtils.isBlank(content)) {
                        blankNum++;
                        logger.warn("There is blank, file={}, line={}", file, fileLineNum);
                        continue;
                    }

                    data = content.split(Model.INDEX_TYPE_DELIMITER);
                    if(2 != data.length) {
                        logger.error("Parse failed, file={}, data={}, line={}", file, content, fileLineNum);
                        parseErrNum++;
                        continue;
                    }
                    if(StringUtils.isBlank(data[0])) {
                        logger.error("Parse failed, file={}, data={}, line={}", file, content, fileLineNum);
                        parseErrNum++;
                        continue;
                    }
                    try {
                        // Key@Value
                        key = ByteString.copyFromUtf8(data[0]);
                        if (StringUtils.isEmpty(key.toStringUtf8())) {
                            throw new Exception("IndexType key is empty");
                        }
                        value = ByteString.copyFromUtf8(data[1]);
                        kvParis.put(key, value);
                    } catch (Exception e) {
                        logger.error("Parse failed, file={}, data={}, line={}", file, content, fileLineNum);
                        parseErrNum++;
                    }
                }
                if (!kvParis.isEmpty()) {
                    try {
                    	if(StringUtils.isBlank(rollbackMode))
                    		rawKvClient.batchPut(kvParis);
                    	else
                    		rawKvClient.batchPut(kvParis, Long.parseLong(rollbackMode));
                    } catch (Exception e) {
                        putErr += kvParis.size();
                        logger.error("Batch put failed, file={}", file.getAbsolutePath(), e);
                        kvParis.clear();
                    }
                }

                bufferedReader.close();
                bufferedInputStream.close();
                fileInputStream.close();

            } catch (IOException e) {
                e.printStackTrace();
            }

            long duration = System.currentTimeMillis() - importStartTime;
            logger.info("Import summary: File=" + file.getAbsolutePath() + ", total=" + fileLineNum + ", imported=" + kvParis.size() + ", blankNum=" + blankNum + ", putErrNum = " + putErr + ", parseErrNum = " + parseErrNum + ", duration = " + duration / 1000 + "s");

            kvParis.clear();

        }

        logger.info("All indexType file import finish.");
        rawKvClient.close();

    }

}
