package com.pingcap.timer;

import com.pingcap.enums.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class ExportTimer extends TimerTask {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private final AtomicInteger totalExportNum;

    public ExportTimer(AtomicInteger totalExportNum) {
        this.totalExportNum = totalExportNum;
    }

    @Override
    public void run() {
        logger.info("Total exportNum={}", totalExportNum.get());
    }

}
