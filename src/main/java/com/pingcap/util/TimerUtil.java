package com.pingcap.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Time;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class TimerUtil extends Thread {

    private static final Logger logger = LoggerFactory.getLogger("logBackLog");

    private final Properties properties;

    private final AtomicInteger totalFileLine;
    private final int totalLines;
    private final String filePath;
    private String logStr = "";

    public TimerUtil(AtomicInteger totalFileLine, int totalLines, String filePath, Properties properties) {
        this.totalFileLine = totalFileLine;
        this.totalLines = totalLines;
        this.filePath = filePath;
        this.properties = properties;
    }

    public static String getResult(int num1, int num2) {
        java.text.NumberFormat numerator = java.text.NumberFormat.getInstance();
        numerator.setMaximumFractionDigits(2);
        return numerator.format((float) num1 / (float) num2 * 100);
    }


    @Override
    public void run() {
        long interval = Long.parseLong(properties.getProperty("importer.timer.interval"));
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                logStr = String.format("[%s] %s lines has been imported, [%s/%s], progress %s", filePath, totalFileLine, totalFileLine, totalLines, getResult(totalFileLine.get(), totalLines));
                logger.info(logStr + "%");
            }
        }, 5000, interval);
    }
}


