package com.pingcap.timer;

import com.pingcap.enums.Model;
import com.pingcap.util.CountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class ImportTimer extends TimerTask {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    private final AtomicInteger totalFileLine;
    private final int totalLines;
    private final String filePath;

    public ImportTimer(AtomicInteger totalFileLine, int totalLines, String filePath) {
        this.totalFileLine = totalFileLine;
        this.totalLines = totalLines;
        this.filePath = filePath;
    }

    @Override
    public void run() {
        String log = String.format("[%s] [%s/%s], Insert ratio %s", this.filePath, totalFileLine, totalLines, CountUtil.getPercentage(totalFileLine.get(), totalLines));
        logger.info(log + "%");
    }
}


