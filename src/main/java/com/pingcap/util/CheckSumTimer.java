package com.pingcap.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class CheckSumTimer extends TimerTask {

    private static final Logger logger = LoggerFactory.getLogger("checkSumLog");

    private String logStr = "";

    private final String filePath;
    private final AtomicInteger totalCheckNum;
    private final int totalFileNum;

    public CheckSumTimer(String filePath, AtomicInteger totalCheckNum, int totalFileNum) {
        this.filePath = filePath;
        this.totalCheckNum = totalCheckNum;
        this.totalFileNum = totalFileNum;
    }

    public void run() {
        logStr = String.format("[%s] [%s/%s], Check sum ratio %s", filePath, totalCheckNum, totalFileNum, CountUtil.getPercentage(totalCheckNum.get(), totalFileNum));
        logger.info(logStr + "%");
    }

}
