package com.pingcap.controller;

import com.pingcap.enums.Model;
import com.pingcap.task.TaskInterface;
import com.pingcap.timer.SystemMonitorTimer;
import com.pingcap.util.*;
import org.tikv.common.TiSession;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class FileScanner implements ScannerInterface {
    
    public FileScanner() {
    }
	
    @Override
    public void run(TiSession tiSession,TaskInterface cmdInterFace) {
        long importStartTime = System.currentTimeMillis();
        final Map<String, String> properties = cmdInterFace.getProperties();
        final String headLogger = cmdInterFace.getClass().getName();
        List<File> importFileList = FileUtil.showFileList( properties.get(Model.IMPORT_FILE_PATH), false );
        final List<String> ttlSkipTypeList = new ArrayList<>(Arrays.asList(properties.get(Model.TTL_SKIP_TYPE).split(",")));
        final List<String> ttlPutList = new ArrayList<>(Arrays.asList(properties.get(Model.TTL_PUT_TYPE).split(",")));
        Collections.shuffle(importFileList);
        Timer timer = new Timer();
        SystemMonitorTimer importTimer = new SystemMonitorTimer(cmdInterFace);
        timer.schedule(importTimer, 5000, Long.parseLong(properties.get(Model.TIMER_INTERVAL)));
        
        // Start the Main thread for each file.showFileList.
        int corePoolSize = Integer.parseInt(properties.get(Model.CORE_POOL_SIZE));
        final ThreadPoolExecutor threadPoolFileScanner = ThreadPoolUtil.startJob( corePoolSize, Integer.parseInt(properties.get(Model.MAX_POOL_SIZE)));
        for (File importFile : importFileList) {
            if(importTimer.useCPURatio-0.799999 <= 0.000001){
            	threadPoolFileScanner.execute(new LineLoadingJob( cmdInterFace, importFile.getAbsolutePath(), tiSession, ttlSkipTypeList, ttlPutList, 0));
            }
            else{
            	threadPoolFileScanner.execute(new LineLoadingJob( cmdInterFace, importFile.getAbsolutePath(), tiSession, ttlSkipTypeList, ttlPutList, FileUtil.getFileLines(importFile)));
            }
        }
        threadPoolFileScanner.shutdown();

        try {
            if (threadPoolFileScanner.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
                long importDuration = System.currentTimeMillis() - importStartTime;
                cmdInterFace.getLogger().info("All files {} is complete! Duration={}s.", headLogger, (importDuration / 1000));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally{
        	timer.cancel();
        }
        return;
    }

}