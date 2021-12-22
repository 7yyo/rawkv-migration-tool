package com.pingcap.timer;

import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import com.pingcap.task.TaskInterface;
import com.pingcap.util.CountUtil;

public class TaskTimer extends TimerTask {

	private TaskInterface cmdInterFace = null;
    private final AtomicInteger totalFileLine;
    private final int totalLines;
    private final String filePath;
	
    public TaskTimer(TaskInterface cmdInterFace,AtomicInteger totalFileLine, int totalLines, String filePath) {
        this.cmdInterFace = cmdInterFace;
        this.totalFileLine = totalFileLine;
        this.totalLines = totalLines;
        this.filePath = filePath;
    }
    
	@Override
	public void run() {
		cmdInterFace.getLogger().info("[{}] [{}/{}], Process ratio {}%", this.filePath, totalFileLine, totalLines, CountUtil.getPercentage(totalFileLine.get(), totalLines));
	}

}
