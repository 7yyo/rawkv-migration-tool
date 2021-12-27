package com.pingcap.timer;

import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

import com.pingcap.task.TaskInterface;
import com.pingcap.util.OSUtils;

public class SystemMonitorTimer extends TimerTask {
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
		putData();
		ThreadGroup group = Thread.currentThread().getThreadGroup();
        ThreadGroup topGroup = group;
        while (group != null) {
            topGroup = group;
            group = group.getParent();
        }

		cmdInterFace.getLogger().info("Process status, CPU= {},MEM=({}/{}),TREADS= {}, SPEED= {}", 
				OSUtils.getCPURatio(),
				Runtime.getRuntime().freeMemory(),
				Runtime.getRuntime().totalMemory(),
				topGroup.activeCount(),
				getIBytesString());
	}

	private synchronized void putData(){
		long curData[] = new long[2];
		curData[0] = System.currentTimeMillis();
		curData[1] = TaskInterface.totalDataBytes.getAndSet(0);
		traffic.add(curData);
		if(5 < traffic.size())
			traffic.remove(0);		
	}
	
	private String getIBytesString(){
		long preData[],curData[];
		long dataValue = 0,timeValue = 0;
		for(int i=1;i<traffic.size();i++){
			preData = traffic.get(i-1);
			curData = traffic.get(i);
			timeValue = (timeValue +(curData[0] - preData[0]))/2;
			dataValue = (dataValue +(curData[1] - preData[1]))/2;
		}
		return String.format("%.2fMB/s", kbToMB((dataValue*1000)/(timeValue+1)));
	}
	
	private static float kbToMB(long value) {
		if (value == 0) {
			return 0;
		}
		if (value <= 5 ) {
			return (float)value/1024;
		}
		value = 1000*value/1024;
		return Float.parseFloat(keepNDecimal(value, 2));
	}
	
	public static String keepNDecimal(long value, int n) {
		int r = (int)(value%10);
		if (r == 0) {
			/*do nothing*/
		} else if (r >= 5) {
			value += (10-r);
		} else {
			value -= r;
		}
		StringBuilder sb = new StringBuilder(Long.toString(value));
		sb.setLength(sb.length()-1);
		int len = sb.length();
		if (len < n) {
			while (++len <= n) {
				sb.insert(0, '0');
			}
			sb.insert(0, "0.");
		} else {
			sb.insert(sb.length()-n, '.');
		}
		return sb.toString();
	}
	
}
