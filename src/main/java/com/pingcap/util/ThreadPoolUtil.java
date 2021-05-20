package com.pingcap.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.log4j.Logger;

import java.util.concurrent.*;

public class ThreadPoolUtil {

    private static final Logger logger = Logger.getLogger(ThreadPoolUtil.class);

    public static ThreadPoolExecutor startJob(int corePoolSize, int maxPoolSize, String fileName) {
        return new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                0,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<Runnable>(2),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("thread-%d").build(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

}
