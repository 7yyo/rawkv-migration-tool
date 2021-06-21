package com.pingcap.timer;

import com.pingcap.enums.Model;
import com.pingcap.util.CountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yuyang
 */
public class CheckSumTimer extends TimerTask {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    private final String filePath;
    private final AtomicInteger totalCheckNum;
    private final int totalFileNum;

    public CheckSumTimer(String filePath, AtomicInteger totalCheckNum, int totalFileNum) {
        this.filePath = filePath;
        this.totalCheckNum = totalCheckNum;
        this.totalFileNum = totalFileNum;
    }

    @Override
    public void run() {
        String logStr = String.format("[%s] [%s/%s], Check sum ratio [%s]", filePath, totalCheckNum, totalFileNum, CountUtil.getPercentage(totalCheckNum.get(), totalFileNum));
        logger.info(logStr + "%");
    }

}
