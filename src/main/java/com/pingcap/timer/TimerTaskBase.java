package com.pingcap.timer;

import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import com.pingcap.enums.Model;
import com.pingcap.task.TaskInterface;

public class TimerTaskBase extends TimerTask {
	public ThreadPoolExecutor threadPoolFileLoading = null;
	public TaskInterface cmdInterFace = null;
	public int internalThreadPool = 0;
	public int internalMaxThreadPool = 0;
    public AtomicInteger processFileLines;
    
	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	public synchronized void cmpConfingUpdate(){
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
