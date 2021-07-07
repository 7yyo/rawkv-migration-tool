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
import io.prometheus.client.Counter;
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

/**
 * Data verification, divided into full data verification and percentage sampling data verification
 *
 * @author yuyang
 */
public class CheckSum {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger checkSumLog = LoggerFactory.getLogger(Model.CHECK_SUM_LOG);

    static final Histogram CHECK_SUM_LATENCY = Histogram.build().name("check_sum_latency_seconds").help("Check sum latency in seconds.").labelNames("check_sum_latency").register();

    /**
     * Initialize the check sum log file
     *
     * @param properties:   Configuration file
     * @param fileChannel:  NIO
     * @param originalFile: Original data file
     * @return fileChannel
     */
    public static FileChannel initCheckSumLog(Properties properties, FileChannel fileChannel, File originalFile) {

        String checkSumFilePath = properties.getProperty(Model.CHECK_SUM_FILE_PATH);
        String checkSumFileName = checkSumFilePath.replaceAll("\"", "") + "/" + originalFile.getName().replaceAll("\\.", "") + "/" + Thread.currentThread().getId() + ".txt";

        File checkSumFile = new File(checkSumFileName);
        try {
            if (checkSumFile.createNewFile()) {
                FileOutputStream fileOutputStream = new FileOutputStream(checkSumFile);
                fileChannel = fileOutputStream.getChannel();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            // The path of the original data file is recorded in the first line of the check sum file, so take it out first
            ByteBuffer originalLine = StandardCharsets.UTF_8.encode(originalFile.getAbsolutePath() + "\n");
            fileChannel.write(originalLine);
        } catch (
                IOException e) {
            e.printStackTrace();
        }
        return fileChannel;
    }

    /**
     * See detail <html>https://github.com/7yyo/to_tikv/blob/master/src/main/resources/img/checksum.png</html>
     *
     * @param checkSumFilePath:  check sum file path
     * @param checkSumDelimiter: check sum file delimiter
     */
    public static void checkSum(String checkSumFilePath, String checkSumDelimiter, TiSession tiSession, Properties properties) {

        logger.info(String.format("************ Start check sum for [%s] ************", checkSumFilePath));

        long interval = Long.parseLong(properties.getProperty(Model.TIMER_INTERVAL));
        String importMode = properties.getProperty(Model.MODE);
        String scenes = properties.getProperty(Model.SCENES);
        String keyDelimiter = properties.getProperty(Model.KEY_DELIMITER);
        String delimiter1 = properties.getProperty(Model.DELIMITER_1);
        String delimiter2 = properties.getProperty(Model.DELIMITER_2);

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

                Histogram.Timer iteTimer = CHECK_SUM_LATENCY.labels("ite duration").startTimer();

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
                    Histogram.Timer checkSumGetTimer = CHECK_SUM_LATENCY.labels("check sum get duration").startTimer();
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
                    Histogram.Timer eqTimer = CHECK_SUM_LATENCY.labels("eq duration").startTimer();
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

    /**
     * For the full check sum, directly compare with the original data file after the import, and no longer write the sampling check sum file.
     * Note that when this configuration is 1, no matter how much checkSumPercentage is, the full check sum will be performed
     *
     * @param originalFilePath: original file path
     */
    public static void simpleCheckSum(String originalFilePath, TiSession tiSession, Properties properties) {

        logger.info(String.format("************ Start simple check sum for [%s] ************", originalFilePath));

        long interval = Long.parseLong(properties.getProperty(Model.TIMER_INTERVAL));
        String importMode = properties.getProperty(Model.MODE);
        String scenes = properties.getProperty(Model.SCENES);
        String delimiter1 = properties.getProperty(Model.DELIMITER_1);
        String delimiter2 = properties.getProperty(Model.DELIMITER_2);
        String keyDelimiter = properties.getProperty(Model.KEY_DELIMITER);
        String envId = properties.getProperty(Model.ENV_ID);
        String ttlType = properties.getProperty(Model.TTL_TYPE);

        int checkParseErrorNum = 0;
        int checkNotInsertErrorNum = 0;
        int checkFailNum = 0;
        AtomicInteger totalCheckNum = new AtomicInteger(0);

        RawKVClient rawKvClient = tiSession.createRawClient();

        JSONObject jsonObject;

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

        if (originalFileIt != null) {

            while (originalFileIt.hasNext()) {

                totalCheckNum.addAndGet(1);

                Histogram.Timer iteTimer = CHECK_SUM_LATENCY.labels("ite duration").startTimer();
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

                                    Histogram.Timer checkSumGetTimer = CHECK_SUM_LATENCY.labels("check sum get duration").startTimer();
                                    value = rawKvClient.get(ByteString.copyFromUtf8(key)).toStringUtf8();
                                    checkSumGetTimer.observeDuration();

                                    if (StringUtils.isBlank(value)) {
                                        checkSumLog.warn(String.format("The original file line = [%s] is not be inserted!", originalLine));
                                        checkNotInsertErrorNum++;
                                        continue;
                                    }

                                    Histogram.Timer eqTimer = CHECK_SUM_LATENCY.labels("eq duration").startTimer();
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

                                    Histogram.Timer eqTimer = CHECK_SUM_LATENCY.labels("eq duration").startTimer();
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

                            Histogram.Timer eqTimer = CHECK_SUM_LATENCY.labels("eq duration").startTimer();
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
    public static void startCheckSum(Properties properties, TiSession tiSession, Counter fileCounter) {

        long checkStartTime = System.currentTimeMillis();
        String simpleCheckSum = properties.getProperty(Model.SIMPLE_CHECK_SUM);
        String checkSumFilePath = properties.getProperty(Model.CHECK_SUM_FILE_PATH);
        String checkSumDelimiter = properties.getProperty(Model.CHECK_SUM_DELIMITER);
        int checkSumThreadNum = Integer.parseInt(properties.getProperty(Model.CHECK_SUM_THREAD_NUM));
        List<File> checkSumFileList;

        if (!Model.ON.equals(simpleCheckSum)) {
            checkSumFileList = FileUtil.showFileList(checkSumFilePath, true);
        } else {
            checkSumFileList = FileUtil.showFileList(properties.getProperty(Model.FILE_PATH), true);
        }
        ThreadPoolExecutor checkSumThreadPoolExecutor = ThreadPoolUtil.startJob(checkSumThreadNum, checkSumThreadNum, checkSumFilePath);
        if (checkSumFileList != null) {
            for (File checkSumFile : checkSumFileList) {
                checkSumThreadPoolExecutor.execute(new CheckSumJsonJob(checkSumFile.getAbsolutePath(), checkSumDelimiter, tiSession, properties, fileCounter));
            }
        } else {
            logger.error(String.format("Check sum file [%s] is not exists!", checkSumFilePath));
            return;
        }
        checkSumThreadPoolExecutor.shutdown();
        try {
            if (checkSumThreadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
                long duration = System.currentTimeMillis() - checkStartTime;
                logger.info(String.format("All files check sum is complete! It takes [%s] seconds", (duration / 1000)));
                System.exit(0);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
