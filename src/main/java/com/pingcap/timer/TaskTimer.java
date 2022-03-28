package com.pingcap.timer;

import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.DoubleAdder;

import com.pingcap.enums.Model;
import com.pingcap.task.TaskInterface;
import com.pingcap.util.CountUtil;

public class TaskTimer extends TimerTaskBase {
    private final int totalLines;
    private final String filePath;

    private final long startTime = System.currentTimeMillis();
    
    public TaskTimer(ThreadPoolExecutor threadPoolFileLoading,TaskInterface cmdInterFace,DoubleAdder processFileLines, int totalLines, String filePath) {
        this.cmdInterFace = cmdInterFace;
        this.processFileLines = processFileLines;
        this.totalLines = totalLines;
        this.filePath = filePath;
        this.threadPoolFileLoading = threadPoolFileLoading;
        Map<String, String> properties = cmdInterFace.getProperties();
        this.internalThreadPool = Integer.parseInt(properties.get(Model.INTERNAL_THREAD_POOL));
        this.internalMaxThreadPool = Integer.parseInt(properties.get(Model.INTERNAL_MAXTHREAD_POOL));
    }
    
	@Override
	public void run() {
        cmpConfingUpdate();
        int curProcessLines = processFileLines.intValue();
        float remaining = (float)(totalLines - curProcessLines)/((curProcessLines/(System.currentTimeMillis()-startTime))+1)/1000;
		cmdInterFace.getLogger().info("[{}] [{}/{}],{} ratio {}%,remained {}s", this.filePath, curProcessLines, totalLines,cmdInterFace.getClass().getSimpleName(), CountUtil.getPercentage(curProcessLines, totalLines),String.format("%.2f", remaining));
	}
	

}
