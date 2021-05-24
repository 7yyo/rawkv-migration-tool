package com.pingcap.timer;

import com.pingcap.enums.Model;
import com.pingcap.util.CountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;

public class FileCountTimer extends TimerTask {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    private int totalFileProcessed;
    private int totalFile;

    public FileCountTimer(int totalFileProcessed, int totalFile) {
        this.totalFileProcessed = totalFileProcessed;
        this.totalFile = totalFile;
    }

    @Override
    public void run() {
        logger.info("Total data file processing progress: [%s/%s] %s", totalFileProcessed, totalFile, CountUtil.getPercentage(totalFileProcessed, totalFile));
    }
}
