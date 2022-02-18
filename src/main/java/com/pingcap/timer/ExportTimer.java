package com.pingcap.timer;

import com.pingcap.enums.Model;
import com.pingcap.task.TaskInterface;

import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class ExportTimer extends TimerTaskBase {
    private final long startTime = System.currentTimeMillis();

    public ExportTimer(ThreadPoolExecutor threadPoolFileLoading,TaskInterface cmdInterFace,AtomicInteger exportTotalCounter) {
        this.cmdInterFace = cmdInterFace;
        this.processFileLines = exportTotalCounter;
        //this.totalLines = totalLines;
        //this.filePath = filePath;
        this.threadPoolFileLoading = threadPoolFileLoading;
        Map<String, String> properties = cmdInterFace.getProperties();
        this.internalThreadPool = Integer.parseInt(properties.get(Model.INTERNAL_THREAD_POOL));
        this.internalMaxThreadPool = Integer.parseInt(properties.get(Model.INTERNAL_MAXTHREAD_POOL));
    }

    @Override
    public void run() {
        cmpConfingUpdate();
		cmdInterFace.getLogger().info("Total exportNum={} and used time={}s", processFileLines.get(),String.format("%.2f", (float)(System.currentTimeMillis()-startTime)/1000));
    }

}
