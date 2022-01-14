package com.pingcap.timer;

import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import com.pingcap.enums.Model;
import com.pingcap.task.TaskInterface;
import com.pingcap.util.CountUtil;

public class TaskTimer extends TimerTask {

	private ThreadPoolExecutor threadPoolFileLoading = null;
	private TaskInterface cmdInterFace = null;
    private final AtomicInteger processFileLines;
    private final int totalLines;
    private final String filePath;
    private int internalThreadPool = 0;
    private int internalMaxThreadPool = 0;
    private final long startTime = System.currentTimeMillis();
    
    public TaskTimer(ThreadPoolExecutor threadPoolFileLoading,TaskInterface cmdInterFace,AtomicInteger processFileLines, int totalLines, String filePath) {
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
        int curProcessLines = processFileLines.get();
        float remaining = (float)(totalLines - curProcessLines)/((curProcessLines/(System.currentTimeMillis()-startTime))+1)/1000;
		cmdInterFace.getLogger().info("[{}] [{}/{}],{} ratio {}%,remained {}s", this.filePath, curProcessLines, totalLines,cmdInterFace.getClass().getSimpleName(), CountUtil.getPercentage(curProcessLines, totalLines),String.format("%.2f", remaining));
	}
	
	private synchronized void cmpConfingUpdate(){
        Map<String, String> properties = cmdInterFace.getProperties();
        int newThreadPool = Integer.parseInt(properties.get(Model.INTERNAL_THREAD_POOL));
        int newMaxThreadPool = Integer.parseInt(properties.get(Model.INTERNAL_MAXTHREAD_POOL));
        if(newThreadPool != internalThreadPool){
        	internalThreadPool = newThreadPool;
        	threadPoolFileLoading.setCorePoolSize(newThreadPool);
        }
        if(newMaxThreadPool != internalMaxThreadPool){
        	internalMaxThreadPool = newMaxThreadPool;
        	threadPoolFileLoading.setCorePoolSize(newMaxThreadPool);
        }
	}

}
