package com.pingcap.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

/**
 * @author yuyang
 */
public class ThreadPoolUtil {

    public static ThreadPoolExecutor startJob(int corePoolSize, int maxPoolSize) {
        return new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                0,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(maxPoolSize),
                new ThreadFactoryBuilder().setDaemon(true).build(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }
    
/**
 * @author zhugp
 */   
    public static void forExecutor(ThreadPoolExecutor executor, int cSize){
        //预防等待队列过载,未完成数量超过设定值则等待。
    	if(executor.getActiveCount()+executor.getQueue().size() > cSize) {
            while (true) {
				try {
					Thread.sleep(5000);				
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
            	if(executor.getActiveCount()+executor.getQueue().size() < cSize)
            		break;
            }
        }//end if
    }

}
