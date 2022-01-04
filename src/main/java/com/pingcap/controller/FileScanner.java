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
        List<File> importFileList = FileUtil.showFileList( properties.get(Model.IMPORT_FILE_PATH), false );
        final List<String> ttlSkipTypeList = new ArrayList<>(Arrays.asList(properties.get(Model.TTL_SKIP_TYPE).split(",")));
        final List<String> ttlPutList = new ArrayList<>(Arrays.asList(properties.get(Model.TTL_PUT_TYPE).split(",")));
        Collections.shuffle(importFileList);
        
        // Start the Main thread for each file.showFileList.
        final ThreadPoolExecutor threadPoolFileScanner = ThreadPoolUtil.startJob( Integer.parseInt(properties.get(Model.CORE_POOL_SIZE)), Integer.parseInt(properties.get(Model.MAX_POOL_SIZE)));
        Timer timer = new Timer();
        SystemMonitorTimer taskTimer = new SystemMonitorTimer(cmdInterFace);
        taskTimer.getDateAndUpdate(FileUtil.getFileLastTime(properties.get(Model.SYS_CFG_PATH)), true);
        timer.schedule(taskTimer, 5000, Long.parseLong(properties.get(Model.TIMER_INTERVAL)));
        
        for (File importFile : importFileList) {
            if(taskTimer.useCPURatio < 0.8){
            	threadPoolFileScanner.execute(new LineLoadingJob( cmdInterFace, importFile.getAbsolutePath(), tiSession, ttlSkipTypeList, ttlPutList, 0, threadPoolFileScanner,taskTimer));
            }
            else{
            	threadPoolFileScanner.execute(new LineLoadingJob( cmdInterFace, importFile.getAbsolutePath(), tiSession, ttlSkipTypeList, ttlPutList, FileUtil.getFileLines(importFile), threadPoolFileScanner,taskTimer));
            }
        }
        threadPoolFileScanner.shutdown();

        try {
            if (threadPoolFileScanner.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
                long importDuration = System.currentTimeMillis() - importStartTime;
                cmdInterFace.getLogger().info("All files({}) {} is complete! Duration={}s.",TaskInterface.filesNum.get(), cmdInterFace.getClass().getSimpleName(), (importDuration / 1000));
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