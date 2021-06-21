package com.pingcap.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Properties;
import java.util.concurrent.*;

/**
 * @author yuyang
 */
public class ThreadPoolUtil {

    public static ThreadPoolExecutor startJob(int corePoolSize, int maxPoolSize, String fileName) {
        return new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                0,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<Runnable>(maxPoolSize),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("thread-" + fileName + "-%d").build(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

}
