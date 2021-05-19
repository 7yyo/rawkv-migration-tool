package com.pingcap.util;

import com.pingcap.importer.IndexInfoS2T;
import org.apache.log4j.Logger;

import java.sql.Time;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class TimerUtil implements Runnable {

    private static final Logger logger = Logger.getLogger(TimerUtil.class);

    private final AtomicInteger totalFileLine;
    private final int totalLines;
    private final String filePath;
    private String logStr = "";
    private static String result = "";

    public TimerUtil(AtomicInteger totalFileLine, int totalLines, String filePath) {
        this.totalFileLine = totalFileLine;
        this.totalLines = totalLines;
        this.filePath = filePath;
    }

    public static void main(String[] args) {
        System.out.println(getResult(123,1245));
    }

    public static String getResult(int num1, int num2) {
        java.text.NumberFormat numerator = java.text.NumberFormat.getInstance();
        numerator.setMaximumFractionDigits(2);
        return numerator.format((float) num1 / (float) num2 * 100);
    }


    @Override
    public void run() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                logStr = String.format("[%s] %s lines has been imported, [%s/%s], progress %s", filePath, totalFileLine, totalFileLine, totalLines, getResult(totalFileLine.get(), totalLines));
                logger.info(logStr + "%");
            }
        }, 5000, 10000);
    }
}


