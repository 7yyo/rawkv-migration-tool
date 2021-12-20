package com.pingcap.controller;

import com.pingcap.enums.Model;
import com.pingcap.task.TaskInterface;
import com.pingcap.util.*;
import org.tikv.common.TiSession;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class FileScanner {

    private String task;
    
    public FileScanner(String task) {
		this.task = task;
    }
	
    
    public void run(TiSession tiSession,TaskInterface cmdInterFace) {
        long importStartTime = System.currentTimeMillis();
        final Map<String, String> properties = cmdInterFace.getProperties();
        final String headLogger = cmdInterFace.getClass().getName();
        List<File> importFileList = FileUtil.showFileList( properties.get(Model.IMPORT_FILE_PATH), false );//
        ////2021-12-09 delete by zhugp, Collections.shuffle(importFileList);

        // Start the Main thread for each file.showFileList.
        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(Integer.parseInt(properties.get(Model.CORE_POOL_SIZE)), Integer.parseInt(properties.get(Model.MAX_POOL_SIZE)));

        for (File importFile : importFileList) {
            threadPoolExecutor.execute(new LineLoadingJob( cmdInterFace, task, importFile.getAbsolutePath(), tiSession));
        }
        threadPoolExecutor.shutdown();

        try {
            if (threadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
                long importDuration = System.currentTimeMillis() - importStartTime;
                cmdInterFace.getLogger().info("All files {} is complete! Duration={}s.", headLogger, (importDuration / 1000));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return;
    }

}