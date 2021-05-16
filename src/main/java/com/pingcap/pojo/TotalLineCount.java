package com.pingcap.pojo;

import java.util.concurrent.atomic.AtomicInteger;

public class TotalLineCount {

    private static AtomicInteger line = new AtomicInteger(0);

    public AtomicInteger getLine() {
        return line;
    }

    public void setLine(AtomicInteger line) {
        TotalLineCount.line = line;
    }

    private static TotalLineCount totalLineCount;

    private TotalLineCount() {
    }

    public static synchronized TotalLineCount getInstance() {
        if (totalLineCount == null) {
            totalLineCount = new TotalLineCount();
        }
        return totalLineCount;
    }

}
