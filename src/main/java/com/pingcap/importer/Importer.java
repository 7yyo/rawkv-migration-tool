package com.pingcap.importer;

import com.pingcap.enums.Model;
import com.pingcap.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Importer {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    public static void run(Map<String, String> properties, TiSession tiSession) {

        // If it is indexType, run another single-threaded importer
        if (Model.INDEX_TYPE.equals(properties.get(Model.SCENES))) {
            IndexTypeImporter.run(properties, tiSession);
            return;
        }

        long importStartTime = System.currentTimeMillis();

        // Traverse all the files that need to be written.
        PropertiesUtil.checkConfig(properties, Model.IMPORT_FILE_PATH);
        List<File> importFileList = FileUtil.showFileList(properties.get(Model.IMPORT_FILE_PATH), false);

        PropertiesUtil.checkConfig(properties, Model.CORE_POOL_SIZE);
        PropertiesUtil.checkConfig(properties, Model.MAX_POOL_SIZE);
        // Start the Main thread for each file.showFileList.
        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(Integer.parseInt(properties.get(Model.CORE_POOL_SIZE)), Integer.parseInt(properties.get(Model.MAX_POOL_SIZE)));

        for (File importFile : importFileList) {
            threadPoolExecutor.execute(new ImporterJob(importFile.getAbsolutePath(), tiSession, properties));
        }
        threadPoolExecutor.shutdown();

        try {
            if (threadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
                long importDuration = System.currentTimeMillis() - importStartTime;
                logger.info("All files import is complete! Duration={}s.", (importDuration / 1000));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}