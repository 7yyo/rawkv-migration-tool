package com.pingcap.checksum;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.enums.Model;
import com.pingcap.job.CheckSumJsonJob;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.TempIndexInfo;
import com.pingcap.timer.CheckSumTimer;
import com.pingcap.util.FileUtil;
import com.pingcap.util.ThreadPoolUtil;
import io.prometheus.client.Histogram;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CheckSum {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger checkSumLog = LoggerFactory.getLogger(Model.CHECK_SUM_LOG);

    static final Histogram DURATION = Histogram.build().name("everything_duration").help("Duration.").labelNames("type").register();

    public static FileChannel initCheckSumLog(Map<String, String> properties, File originalFile, int fileNum) {

        String checkSumFilePath = properties.get(Model.CHECK_SUM_FILE_PATH);
        String checkSumFileName = checkSumFilePath.replaceAll("\"", "") + "/" + originalFile.getName().replaceAll("\\.", "") + "-" + fileNum + "/" + Thread.currentThread().getId() + ".txt";

        File checkSumFile = FileUtil.createFile(checkSumFileName);
        FileOutputStream fileOutputStream;
        FileChannel fileChannel;
        try {
            fileOutputStream = new FileOutputStream(checkSumFile);
            fileChannel = fileOutputStream.getChannel();
            // The path of the original data file is recorded in the first line of the check sum file, so take it out first
            ByteBuffer originalLine = StandardCharsets.UTF_8.encode(originalFile.getAbsolutePath() + "\n");
            fileChannel.write(originalLine);
            return fileChannel;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * See detail <html>https://github.com/7yyo/to_tikv/blob/master/src/main/resources/img/checksum.png</html>
     *
     * @param checkSumFilePath: check sum file path
     */
    public static void checkSum(String checkSumFilePath, TiSession tiSession, Map<String, String> properties) {

        logger.info(String.format("************ Start check sum for [%s] ************", checkSumFilePath));

        String importMode = properties.get(Model.MODE);
        String scenes = properties.get(Model.SCENES);
        String keyDelimiter = properties.get(Model.KEY_DELIMITER);
        String checkSumDelimiter = properties.get(Model.CHECK_SUM_DELIMITER);
        String delimiter1 = properties.get(Model.DELIMITER_1);
        String delimiter2 = properties.get(Model.DELIMITER_2);

        int checkParseErrorNum = 0;
        int checkNotInsertErrorNum = 0;
        int checkFailNum = 0;
        AtomicInteger totalCheckNum = new AtomicInteger(0);

        RawKVClient rawKvClient = tiSession.createRawClient();

        JSONObject jsonObject;
        String originalLine = "";
        String checkSumFileLine;
        String checkSumKey = "";
        int csFileLineNum = 0;
        int lastFileLine = 0;
        String value;

        File checkSumFile = new File(checkSumFilePath);

        Timer timer = new Timer();
        CheckSumTimer checkSumTimer = new CheckSumTimer(checkSumFilePath, totalCheckNum, FileUtil.getFileLines(checkSumFile));
        long interval = Long.parseLong(properties.get(Model.TIMER_INTERVAL));
        timer.schedule(checkSumTimer, 5000, interval);

        // CheckSum file iterator
        LineIterator checkSumFileIt = null;
        // Get original file path from check sum file first line.
        String originalFilePath = "";
        try {
            checkSumFileIt = FileUtils.lineIterator(checkSumFile, "UTF-8");
            if (checkSumFileIt.hasNext()) {
                originalFilePath = checkSumFileIt.nextLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Original file iterator
        LineIterator originalFileIt = null;
        File originalFile = new File(originalFilePath);
        try {
            originalFileIt = FileUtils.lineIterator(originalFile, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }

        int originalLineNum;
        int rowSpan;
        IndexInfo indexInfoCheckSum;
        TempIndexInfo tempIndexInfoCheckSum;
        IndexInfo indexInfoOriginal = new IndexInfo();
        TempIndexInfo tempIndexInfoOriginal = null;
        boolean equalsResult;

        if (checkSumFileIt != null) {

            // Traverse the check sum file,
            // find the line corresponding to the original data file,
            // and compare it, not a one-to-many traversal mode.
            while (checkSumFileIt.hasNext()) {

                Histogram.Timer iteTimer = DURATION.labels("ite duration").startTimer();

                originalLineNum = 0;
                // Total check num
                totalCheckNum.addAndGet(1);

                checkSumFileLine = checkSumFileIt.nextLine();

                // check sum file line, Uniform format, no branch logic required
                try {
                    checkSumKey = checkSumFileLine.split(checkSumDelimiter)[0];
                    csFileLineNum = Integer.parseInt(checkSumFileLine.split(checkSumDelimiter)[1]);
                } catch (Exception e) {
                    // Illegal format
                    checkParseErrorNum++;
                    checkSumLog.error(String.format("The check sum file data line has an illegal format! key=[%s], line=[%s]", checkSumKey, csFileLineNum));
                    continue;
                }

                // The line span of the check sum file and the original data file
                rowSpan = csFileLineNum - lastFileLine;

                if (originalFileIt != null) {
                    while (originalFileIt.hasNext()) {
                        // original file line
                        originalLine = originalFileIt.nextLine();
                        if (++originalLineNum == rowSpan) {
                            break;
                        }
                    }
                    lastFileLine = Integer.parseInt(checkSumFileLine.split(checkSumDelimiter)[1]);
                    iteTimer.observeDuration();

                    // Begin to comparison original & checkSum

                    // Get value by check sum file key
                    Histogram.Timer checkSumGetTimer = DURATION.labels("check sum get duration").startTimer();
                    value = rawKvClient.get(ByteString.copyFromUtf8(checkSumKey)).toStringUtf8();
                    checkSumGetTimer.observeDuration();
                    if (value.isEmpty()) {
                        checkSumLog.warn(String.format("The key [%s] is not be inserted! Original file line=[%s]", checkSumKey, originalLine));
                        checkNotInsertErrorNum++;
                        continue;
                    }

                    /*
                      Init check sum object
                     */
                    indexInfoCheckSum = new IndexInfo();
                    tempIndexInfoCheckSum = new TempIndexInfo();
                    // Raw KV value to jsonObject
                    // Because the check sum is all [ json key + line num ], the format is unified, and there is no csv
                    jsonObject = JSONObject.parseObject(value);
                    try {
                        switch (scenes) {
                            case Model.INDEX_INFO:
                                // key = indexInfo_:_{envid}_:_{type}_:_{id}
                                indexInfoCheckSum = JSON.toJavaObject(jsonObject, IndexInfo.class);
                                IndexInfo.key2IndexInfo(indexInfoCheckSum, checkSumKey, keyDelimiter);
                                break;
                            case Model.TEMP_INDEX_INFO:
                                // key = tempIndex_:_{envid}_:_{id}
                                tempIndexInfoCheckSum = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                                TempIndexInfo.key2TempIndexInfo(tempIndexInfoCheckSum, checkSumKey, keyDelimiter);
                                break;
                            default:
                                throw new IllegalStateException("Unexpected value: " + scenes);
                        }
                    } catch (Exception e) {
                        checkSumLog.error(String.format("Check sum file data line parsing failed! File=[%s], Line=[%s]", checkSumFile.getAbsolutePath(), value));
                        checkParseErrorNum++;
                        continue;
                    }

                    /*
                      Init original object
                     */
                    Histogram.Timer eqTimer = DURATION.labels("eq duration").startTimer();
                    switch (importMode) {

                        // Json to jsonObject
                        case Model.JSON_FORMAT:

                            jsonObject = JSONObject.parseObject(originalLine);

                            try {
                                switch (scenes) {
                                    case Model.INDEX_INFO:
                                        indexInfoOriginal = JSON.toJavaObject(jsonObject, IndexInfo.class);
                                        break;
                                    case Model.TEMP_INDEX_INFO:
                                        tempIndexInfoOriginal = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                                        break;
                                    default:
                                        throw new IllegalStateException("Unexpected value: " + scenes);
                                }
                            } catch (Exception e) {
                                checkSumLog.error(String.format("Parse failed! Line=[%s]", originalLine));
                                checkParseErrorNum++;
                                continue;
                            }

                            // equals
                            switch (scenes) {
                                case Model.INDEX_INFO:
                                    equalsResult = indexInfoCheckSum.equals(indexInfoOriginal);
                                    break;
                                case Model.TEMP_INDEX_INFO:
                                    equalsResult = tempIndexInfoCheckSum.equals(tempIndexInfoOriginal);
                                    break;
                                default:
                                    throw new IllegalStateException("Unexpected value: " + scenes);
                            }
                            if (!equalsResult) {
                                checkSumLog.error(String.format("Check sum failed!File=[%s] Line=[%s], key=[%s]", originalFile.getAbsolutePath(), originalLine, checkSumKey));
                                checkFailNum++;
                            }
                            break;

                        case Model.CSV_FORMAT:
                            IndexInfo.csv2IndexInfo(indexInfoOriginal, originalLine, delimiter1, delimiter2);
                            if (!indexInfoCheckSum.equals(indexInfoOriginal)) {
                                checkSumLog.error(String.format("Check sum failed!File=[%s] Line=[%s], key=[%s]", originalFile.getAbsolutePath(), originalLine, checkSumKey));
                                checkFailNum++;
                            }
                            break;
                        default:
                            throw new IllegalStateException("Unexpected value: " + importMode);
                    }
                    eqTimer.observeDuration();

                }
            }
            try {
                checkSumFileIt.close();
                originalFileIt.close();
                rawKvClient.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        timer.cancel();
        logger.info(String.format("[%s] Check sum completed! TotalCheckNum[%s], TotalNotInsertNum[%s], TotalParseErrorNum[%s], TotalCheckFailNum[%s]", checkSumFilePath, totalCheckNum, checkNotInsertErrorNum, checkParseErrorNum, checkFailNum));

    }

    public static void doRedo(String originalFilePath, TiSession tiSession, Map<String, String> properties) {

        logger.info("************ Start to redo for {} ************", originalFilePath);

        long interval = Long.parseLong(properties.get(Model.TIMER_INTERVAL));

        int checkParseErrorNum = 0;
        int checkNotInsertErrorNum = 0;
        int checkFailNum = 0;
        AtomicInteger totalCheckNum = new AtomicInteger(0);

        RawKVClient rawKvClient = tiSession.createRawClient();

        File originalFile = new File(originalFilePath);
        String originalLine;

        Timer timer = new Timer();
        CheckSumTimer checkSumTimer = new CheckSumTimer(originalFilePath, totalCheckNum, FileUtil.getFileLines(originalFile));
        timer.schedule(checkSumTimer, 5000, interval);

        // original file iterator
        LineIterator originalFileIt = null;
        try {
            originalFileIt = FileUtils.lineIterator(originalFile, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }

        IndexInfo indexInfoRawKv;
        TempIndexInfo tempIndexInfoRawKv;
        IndexInfo indexInfoOriginal = null;
        TempIndexInfo tempIndexInfoOriginal;
        String key;
        String value;

        String importMode = properties.get(Model.MODE);
        String scenes = properties.get(Model.SCENES);

        String keyDelimiter = properties.get(Model.KEY_DELIMITER);
        String delimiter1 = properties.get(Model.DELIMITER_1);
        String delimiter2 = properties.get(Model.DELIMITER_2);

        String envId = properties.get(Model.ENV_ID);
        String ttlType = properties.get(Model.TTL_TYPE);

        JSONObject jsonObject;
        if (originalFileIt != null) {
            while (originalFileIt.hasNext()) {
                totalCheckNum.addAndGet(1);

                Histogram.Timer iteTimer = DURATION.labels("ite duration").startTimer();
                originalLine = originalFileIt.nextLine();
                iteTimer.observeDuration();

                /*
                Init original object
                 */
                switch (importMode) {
                    case Model.JSON_FORMAT:
                        jsonObject = JSONObject.parseObject(originalLine);
                        switch (scenes) {
                            case Model.INDEX_INFO:
                                try {
                                    indexInfoOriginal = JSON.toJavaObject(jsonObject, IndexInfo.class);
                                    // key = indexInfo_:_{envid}_:_{type}_:_{id}
                                    key = String.format(IndexInfo.INDEX_INFO_KET_FORMAT, envId, indexInfoOriginal.getType(), indexInfoOriginal.getId());

                                    if (ttlType.contains(key.split(keyDelimiter)[2])) {
                                        continue;
                                    }

                                    IndexInfo.key2IndexInfo(indexInfoOriginal, key, keyDelimiter);
                                    Histogram.Timer checkSumGetTimer = DURATION.labels("check sum get duration").startTimer();
                                    value = rawKvClient.get(ByteString.copyFromUtf8(key)).toStringUtf8();
                                    checkSumGetTimer.observeDuration();

                                    if (StringUtils.isBlank(value)) {
                                        checkSumLog.warn(String.format("The original file line = [%s] is not be inserted!", originalLine));
                                        checkNotInsertErrorNum++;
                                        continue;
                                    }

                                    Histogram.Timer eqTimer = DURATION.labels("eq duration").startTimer();
                                    jsonObject = JSONObject.parseObject(value);
                                    indexInfoRawKv = JSON.toJavaObject(jsonObject, IndexInfo.class);
                                    IndexInfo.key2IndexInfo(indexInfoRawKv, key, keyDelimiter);

                                    if (!indexInfoRawKv.equals(indexInfoOriginal)) {
                                        checkSumLog.error(String.format("Check sum failed! Line = [%s]", originalLine));
                                        checkFailNum++;
                                        continue;
                                    }
                                    eqTimer.observeDuration();

                                } catch (Exception e) {
                                    // Usually because of a parsing error, continue directly
                                    continue;
                                }
                                break;

                            case Model.TEMP_INDEX_INFO:
                                try {

                                    tempIndexInfoOriginal = JSON.toJavaObject(jsonObject, TempIndexInfo.class);

                                    // key = tempIndex_:_{envid}_:_{id}
                                    key = String.format(TempIndexInfo.TEMP_INDEX_INFO_KEY_FORMAT, envId, tempIndexInfoOriginal.getId());
                                    value = rawKvClient.get(ByteString.copyFromUtf8(key)).toStringUtf8();

                                    TempIndexInfo.key2TempIndexInfo(tempIndexInfoOriginal, key, keyDelimiter);

                                    if (StringUtils.isBlank(value)) {
                                        checkSumLog.warn(String.format("The original file line = [%s] is not be inserted!", originalLine));
                                        checkNotInsertErrorNum++;
                                        continue;
                                    }

                                    Histogram.Timer eqTimer = DURATION.labels("eq duration").startTimer();
                                    jsonObject = JSONObject.parseObject(value);
                                    tempIndexInfoRawKv = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                                    TempIndexInfo.key2TempIndexInfo(tempIndexInfoRawKv, key, keyDelimiter);
                                    if (!tempIndexInfoRawKv.equals(tempIndexInfoOriginal)) {
                                        checkSumLog.error(String.format("Check sum failed! Line = %s", originalLine));
                                        checkFailNum++;
                                        continue;
                                    }
                                    eqTimer.observeDuration();
                                } catch (Exception e) {
                                    continue;
                                }
                                break;
                            default:
                                throw new IllegalStateException("Unexpected value: " + scenes);
                        }
                        break;
                    case Model.CSV_FORMAT:
                        try {

                            IndexInfo.csv2IndexInfo(indexInfoOriginal, originalLine, delimiter1, delimiter2);
                            if (ttlType.contains(indexInfoOriginal.getType())) {
                                continue;
                            }

                            // key = indexInfo_:_{envid}_:_{type}_:_{id}
                            key = String.format(IndexInfo.INDEX_INFO_KET_FORMAT, envId, indexInfoOriginal.getType(), indexInfoOriginal.getId());
                            value = rawKvClient.get(ByteString.copyFromUtf8(key)).toStringUtf8();
                            if (StringUtils.isBlank(value)) {
                                checkSumLog.warn(String.format("The original file line = [%s] is not be inserted!", originalLine));
                                checkNotInsertErrorNum++;
                                continue;
                            }

                            Histogram.Timer eqTimer = DURATION.labels("eq duration").startTimer();
                            jsonObject = JSONObject.parseObject(value);
                            indexInfoRawKv = JSON.toJavaObject(jsonObject, IndexInfo.class);
                            if (!indexInfoRawKv.equals(indexInfoOriginal)) {
                                checkSumLog.error(String.format("Check sum failed! Line = %s", originalLine));
                                checkFailNum++;
                                continue;
                            }
                            eqTimer.observeDuration();

                        } catch (Exception e) {
                            continue;
                        }
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + importMode);
                }
            }
        }

        timer.cancel();
        rawKvClient.close();
        logger.info(String.format("[%s] Check sum completed! TotalCheckNum[%s], TotalNotInsertNum[%s], TotalParseErrorNum[%s], TotalCheckFailNum[%s]", originalFilePath, totalCheckNum, checkNotInsertErrorNum, checkParseErrorNum, checkFailNum));

    }

    /**
     * Start check sum
     */
    public static void run(Map<String, String> properties, TiSession tiSession) {

        long checkStartTime = System.currentTimeMillis();
        String simpleCheckSum = properties.get(Model.SIMPLE_CHECK_SUM);
        String checkSumFilePath = properties.get(Model.CHECK_SUM_FILE_PATH);
        int checkSumThreadNum = Integer.parseInt(properties.get(Model.CHECK_SUM_THREAD_NUM));
        List<File> checkSumFileList;

        if (!Model.ON.equals(simpleCheckSum)) {
            checkSumFileList = FileUtil.showFileList(checkSumFilePath);
        } else {
            checkSumFileList = FileUtil.showFileList(properties.get(Model.IMPORT_FILE_PATH));
        }
        ThreadPoolExecutor checkSumThreadPoolExecutor = ThreadPoolUtil.startJob(checkSumThreadNum, checkSumThreadNum);
        for (File checkSumFile : checkSumFileList) {
            checkSumThreadPoolExecutor.execute(new CheckSumJsonJob(checkSumFile.getAbsolutePath(), tiSession, properties));
        }

        checkSumThreadPoolExecutor.shutdown();

        try {
            if (checkSumThreadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
                long duration = System.currentTimeMillis() - checkStartTime;
                logger.info("All files check sum is complete! It takes {} seconds", (duration / 1000));
                System.exit(0);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
