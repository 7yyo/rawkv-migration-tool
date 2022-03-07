package com.pingcap.timer;

import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

import com.pingcap.controller.BatchJob;
import com.pingcap.controller.ExportExecuterJob;
import com.pingcap.controller.LineLoadingJob;
import com.pingcap.enums.Model;
import com.pingcap.rawkv.LimitSpeedkv;
import com.pingcap.task.Export;
import com.pingcap.task.TaskInterface;
import com.pingcap.util.FileUtil;
import com.pingcap.util.OSUtils;

public class SystemMonitorTimer extends TimerTask {
	public double useCPURatio = 0;
	private long lastModifiedDate = java.lang.Long.MAX_VALUE;
	private static final double prvNumber[]={1024,1024*1024,1024*1024*1024};
	private static final String prvNumberUnit[]={"KB","MB","GB"};
	private TaskInterface cmdInterFace;
	private List<double[]> traffic = new ArrayList<>();

	public SystemMonitorTimer(TaskInterface cmdInterFace){
		this.cmdInterFace = cmdInterFace;
		double curData[] = new double[2];
		curData[0] = System.currentTimeMillis();
		curData[1] = 0;
		traffic.add(curData);
		checkSpeedConfiguration();
	}
	
	@Override
	public void run() {
		List<double[]> buffer = putData();
		ThreadGroup group = Thread.currentThread().getThreadGroup();
        ThreadGroup topGroup = group;
        while (group != null) {
            topGroup = group;
            group = group.getParent();
        }     
        checkSpeedConfiguration();
        useCPURatio = OSUtils.getCPURatio();
        if(cmdInterFace instanceof Export){
			cmdInterFace.getLogger().info("Process ratio, CPU={}%,MEM=({}/{}),TREADS={},BATS={},WRTS={},SPEED={}", 
					String.format("%.2f", useCPURatio),
					byteTo(Runtime.getRuntime().freeMemory()),
					byteTo(Runtime.getRuntime().totalMemory()),
					topGroup.activeCount(),
					ExportExecuterJob.JobtotalUsedCount.get(),
					ExportExecuterJob.WorkertotalUsedCount.get(),
					getSpeedString(buffer));	
        }
        else{
			cmdInterFace.getLogger().info("Process ratio, CPU={}%,MEM=({}/{}),TREADS={},FSCAN={},TJOB={},SPEED={}", 
					String.format("%.2f", useCPURatio),
					byteTo(Runtime.getRuntime().freeMemory()),
					byteTo(Runtime.getRuntime().totalMemory()),
					topGroup.activeCount(),
					LineLoadingJob.totalUsedCount.get(),
					BatchJob.totalUsedCount.get(),
					getSpeedString(buffer));
        }
		 getDateAndUpdate(FileUtil.getFileLastTime(cmdInterFace.getProperties().get(Model.SYS_CFG_PATH)),true);
		 buffer = null;
	}
	
	private synchronized void checkSpeedConfiguration(){
        double speedMax = java.lang.Double.MAX_VALUE;
        String speedMaxStr = cmdInterFace.getProperties().get(Model.TASKSPEEDLIMIT);
        if(null != speedMaxStr){
        	speedMax = Integer.parseInt(speedMaxStr)*1024*1024;
        }
        if(LimitSpeedkv.getRateValue() != speedMax){
        	LimitSpeedkv.setRateValue(speedMax);
        }
	}

	private synchronized List<double[]> putData(){
		double curData[] = new double[2];
		curData[1] = TaskInterface.totalDataBytes.getAndSet(0);	// bytes
		curData[0] = System.currentTimeMillis();				// time
		traffic.add(curData);
		if(5 < traffic.size())
			traffic.remove(0);
		curData = null;
		List<double[]> buffer = new ArrayList<>(traffic.size());
		buffer.addAll(traffic);
		return buffer;
	}
	
	private String getSpeedString(List<double[]> buffer){
		double preData[],curData[];
		double dataValue = 0,timeValue = 0;
		for(int i=1;i<buffer.size();i++){
			preData = buffer.get(i-1);
			curData = buffer.get(i);
			timeValue += (curData[0] - preData[0])/2;
			dataValue += (curData[1] + preData[1])/2;
		}
		if(0 == timeValue)
			return "0,0byte";
		return byteTo(dataValue/timeValue*1000);
	}
	
	public static String byteTo(double value){
		if(value < prvNumber[0])
			return value + "byte";
		for(int i=1;i<prvNumber.length;i++){
			if(value < prvNumber[i]){
				return String.format("%.2f%s", value/prvNumber[i-1],prvNumberUnit[i-1]);
			}
		}
		return String.format("%.2f%s", value/prvNumber[2],prvNumberUnit[2]);
	}
	
	public static String kbTo(double value){
		if(value < prvNumber[0])
			return value + prvNumberUnit[0];
		for(int i=1;i<prvNumber.length;i++){
			if(value < prvNumber[i]){
				return String.format("%.2f%s", value/prvNumber[i-1],prvNumberUnit[i]);
			}
		}
		return String.format("%.2f%s", value/prvNumber[2],"TB");
	}
	
	public synchronized long getDateAndUpdate(long newTime,boolean isUpdate){
		if(isUpdate){
			lastModifiedDate = newTime;
		}
		return lastModifiedDate;
	}
}
