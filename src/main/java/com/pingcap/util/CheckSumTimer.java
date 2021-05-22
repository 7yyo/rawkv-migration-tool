package com.pingcap.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class CheckSumTimer extends Thread {

    private static final Logger logger = LoggerFactory.getLogger("checkSumLog");

    private final Properties properties;
    private String logStr = "";

    private final String filePath;
    private final AtomicInteger totalCheckNum;
    private final int totalFileNum;

    public CheckSumTimer(String filePath, AtomicInteger totalCheckNum, int totalFileNum, Properties properties) {
        this.filePath = filePath;
        this.totalCheckNum = totalCheckNum;
        this.totalFileNum = totalFileNum;
        this.properties = properties;
    }

    public void run() {
        long interval = Long.parseLong(properties.getProperty("importer.timer.interval"));
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                logStr = String.format("[%s] [%s/%s], Check sum ratio %s", filePath, totalCheckNum, totalFileNum, CountUtil.getPercentage(totalCheckNum.get(), totalFileNum));
                logger.info(logStr + "%");
            }
        }, 5000, interval);
    }

}
