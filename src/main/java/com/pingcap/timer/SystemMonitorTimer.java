package com.pingcap.timer;

import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

import com.pingcap.task.TaskInterface;
import com.pingcap.util.OSUtils;

public class SystemMonitorTimer extends TimerTask {
	public double useCPURatio = 0;
	private static final long prvNumber[]={1024,1024*1024,1024*1024*1024};
	private static final String prvNumberUnit[]={"KB","MB","GB"};
	private TaskInterface cmdInterFace;
	private List<long[]> traffic = new ArrayList<>();
	
	public SystemMonitorTimer(TaskInterface cmdInterFace){
		this.cmdInterFace = cmdInterFace;
		long curData[] = new long[2];
		curData[0] = System.currentTimeMillis();
		curData[1] = 0;
		traffic.add(curData);
	}
	
	@Override
	public void run() {
		List<long[]> buffer = putData();
		ThreadGroup group = Thread.currentThread().getThreadGroup();
        ThreadGroup topGroup = group;
        while (group != null) {
            topGroup = group;
            group = group.getParent();
        }
        useCPURatio = OSUtils.getCPURatio();
		cmdInterFace.getLogger().info("Process status, CPU= {}%,MEM=({}/{}),TREADS= {}, SPEED= {}/s", 
				String.format("%.2f", useCPURatio),
				byteTo(Runtime.getRuntime().freeMemory()),
				byteTo(Runtime.getRuntime().totalMemory()),
				topGroup.activeCount(),
				getIBytesString(buffer));
	}

	private synchronized List<long[]> putData(){
		long curData[] = new long[2];
		curData[0] = System.currentTimeMillis();
		curData[1] = TaskInterface.totalDataBytes.getAndSet(0);
		traffic.add(curData);
		if(5 < traffic.size())
			traffic.remove(0);
		List<long[]> buffer = new ArrayList<>(traffic.size());
		buffer.addAll(traffic);
		return buffer;
	}
	
	private String getIBytesString(List<long[]> buffer){
		long preData[],curData[];
		long dataValue = 0,timeValue = 0;
		for(int i=1;i<buffer.size();i++){
			preData = buffer.get(i-1);
			curData = buffer.get(i);
			timeValue += (curData[0] - preData[0])/2;
			dataValue += (curData[1] + preData[1])/2;
		}
		return byteTo((dataValue*1000)/(timeValue+1));
	}
	
	public static String byteTo(long value){
		if(value < prvNumber[0])
			return value + "byte";
		for(int i=1;i<prvNumber.length;i++){
			if(value < prvNumber[i]){
				return String.format("%.2f%s", (float)value/prvNumber[i-1],prvNumberUnit[i-1]);
			}
		}
		return String.format("%.2f%s", (float)value/prvNumber[2],prvNumberUnit[2]);
	}
	
	public static String kbTo(long value){
		if(value < prvNumber[0])
			return value + prvNumberUnit[0];
		for(int i=1;i<prvNumber.length;i++){
			if(value < prvNumber[i]){
				return String.format("%.2f%s", (float)value/prvNumber[i-1],prvNumberUnit[i]);
			}
		}
		return String.format("%.2f%s", (float)value/prvNumber[2],"TB");
	}	
}
