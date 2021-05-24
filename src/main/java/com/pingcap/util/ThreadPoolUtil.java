package com.pingcap.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.pingcap.enums.Model;

import java.util.Properties;
import java.util.concurrent.*;

public class ThreadPoolUtil {

    public static ThreadPoolExecutor startJob(int corePoolSize, int maxPoolSize, Properties properties) {
        return new ThreadPoolExecutor(
                corePoolSize,
                maxPoolSize,
                0,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<Runnable>(Integer.parseInt(properties.getProperty(Model.BLOCK_DEQUE_CAPACITY))),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("thread-%d").build(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

}
