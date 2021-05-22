package com.pingcap.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Time;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class ImportTimer extends TimerTask {

    private static final Logger logger = LoggerFactory.getLogger("logBackLog");

    private final Properties properties;

    private final AtomicInteger totalFileLine;
    private final int totalLines;
    private final String filePath;
    private String logStr = "";
    private boolean timerCancel;

    private final Timer timer = new Timer();

    public ImportTimer(AtomicInteger totalFileLine, int totalLines, String filePath, Properties properties) {
        this.totalFileLine = totalFileLine;
        this.totalLines = totalLines;
        this.filePath = filePath;
        this.properties = properties;
    }

    @Override
    public void run() {
        logStr = String.format("[%s] [%s/%s], Insert ratio %s", this.filePath, totalFileLine, totalLines, CountUtil.getPercentage(totalFileLine.get(), totalLines));
        logger.info(logStr + "%");
    }
}


