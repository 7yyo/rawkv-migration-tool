package com.pingcap.importer;

import com.pingcap.enums.Model;
import com.pingcap.checksum.CheckSumJsonJob;
import com.pingcap.util.*;
import io.prometheus.client.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Importer {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    static final Counter TOTAL_IMPORT_FILE_COUNT = Counter.build().name("total_import_file_count").help("Total import file count.").labelNames("type").register();
    static final Counter TOTAL_CHECK_SUM_FILE_COUNT = Counter.build().name("total_checkSum_file_count").help("Total check sum file count.").labelNames("type").register();

    // Import
    public static void run(Map<String, String> properties, TiSession tiSession) {

        long importStartTime = System.currentTimeMillis();

        if (Model.INDEX_TYPE.equals(properties.get(Model.SCENES))) {
            IndexTypeImporter.run(properties, tiSession);
            return;
        }

        // Traverse all the files that need to be written.
        String importFilePath = properties.get(Model.IMPORT_FILE_PATH);
        List<File> importFileList = FileUtil.showFileList(importFilePath, false);
        TOTAL_IMPORT_FILE_COUNT.labels("import").inc(importFileList.size());

        // Remove check sum & redo folder
//        FileUtil.deleteFolder(properties.get(Model.CHECK_SUM_FILE_PATH));
        FileUtil.deleteFolder(properties.get(Model.BATCH_PUT_ERR_FILE_PATH));

        // Create check sum folder
//        String checkSumFilePath = properties.get(Model.CHECK_SUM_FILE_PATH);
//        FileUtil.createFolder(checkSumFilePath);

        // Start the Main thread for each file.showFileList.
        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(
                Integer.parseInt(properties.get(Model.CORE_POOL_SIZE)),
                Integer.parseInt(properties.get(Model.MAX_POOL_SIZE)));

        for (File file : importFileList) {
            // Create a check sum folder for each import task
//            String checkSumFileName = properties.get(Model.CHECK_SUM_FILE_PATH) + "/" + importFileList.get(i).getName().replaceAll("\\.", "") + "-" + i;
//            System.out.println(checkSumFileName);
//            FileUtil.createFolder(checkSumFileName);
            threadPoolExecutor.execute(new ImporterJob(file.getAbsolutePath(), tiSession, properties));
        }

        threadPoolExecutor.shutdown();

        long duration = System.currentTimeMillis() - importStartTime;

        try {
            if (threadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
                logger.info("All files import is complete! Duration={}s.", (duration / 1000));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // After importing, start check sum if enable check sum.
//        if (Model.ON.equals(properties.get(Model.ENABLE_CHECK_SUM))) {
//
//            long checkSumStartTime = System.currentTimeMillis();
//            int checkSumThreadNum = Integer.parseInt(properties.get(Model.CHECK_SUM_THREAD_NUM));
//
//            // If turn on simple check sum, check sum file is import file.
////            String simpleCheckSum = properties.get(Model.SIMPLE_CHECK_SUM);
//
//            List<File> checkSumFileList = FileUtil.showFileList(importFilePath, false);
//            ThreadPoolExecutor checkSumThreadPoolExecutor = ThreadPoolUtil.startJob(checkSumThreadNum, checkSumThreadNum);
//            TOTAL_CHECK_SUM_FILE_COUNT.labels("check sum").inc(checkSumFileList.size());
//
//            for (File checkSumFile : checkSumFileList) {
//                checkSumThreadPoolExecutor.execute(new CheckSumJsonJob(checkSumFile.getAbsolutePath(), tiSession, properties));
//            }
//
//            checkSumThreadPoolExecutor.shutdown();
//
//            try {
//                if (checkSumThreadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
//                    duration = System.currentTimeMillis() - checkSumStartTime;
//                    logger.info("All files check sum is complete. Duration={}s.", (duration / 1000));
//                    System.exit(0);
//                }
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//        }

    }

}