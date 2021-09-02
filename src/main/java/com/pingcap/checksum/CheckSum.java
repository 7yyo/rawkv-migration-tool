package com.pingcap.checksum;

import com.pingcap.enums.Model;
import com.pingcap.util.FileUtil;
import com.pingcap.util.PropertiesUtil;
import com.pingcap.util.ThreadPoolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CheckSum {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    public static void run(Map<String, String> properties, TiSession tiSession) {

        long checkSumStartTime = System.currentTimeMillis();

        PropertiesUtil.checkConfig(properties, Model.CHECK_SUM_THREAD_NUM);
        int checkSumThreadNum = Integer.parseInt(properties.get(Model.CHECK_SUM_THREAD_NUM));

        PropertiesUtil.checkConfig(properties, Model.IMPORT_FILE_PATH);
        List<File> checkSumFileList = FileUtil.showFileList(properties.get(Model.IMPORT_FILE_PATH), false);

        ThreadPoolExecutor checkSumThreadPoolExecutor = ThreadPoolUtil.startJob(checkSumThreadNum, checkSumThreadNum);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddhhmmss");
        String now = simpleDateFormat.format(new Date());

        PropertiesUtil.checkConfig(properties, Model.CHECK_SUM_MOVE_PATH);
        FileUtil.createFolder(properties.get(Model.CHECK_SUM_MOVE_PATH));
        FileUtil.createFolder(properties.get(Model.CHECK_SUM_MOVE_PATH) + "/" + now);

        AtomicInteger fileNum = new AtomicInteger(0);
        for (File checkSumFile : checkSumFileList) {
            checkSumThreadPoolExecutor.execute(new CheckSumJsonJob(checkSumFile.getAbsolutePath(), tiSession, properties, fileNum, now));
        }

        checkSumThreadPoolExecutor.shutdown();

        try {
            if (checkSumThreadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
                long duration = System.currentTimeMillis() - checkSumStartTime;
                logger.info("All check sum complete. Duration={}s", (duration / 1000));
                System.exit(0);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
