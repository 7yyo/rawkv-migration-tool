package com.pingcap.job;

import com.pingcap.enums.Model;
import com.pingcap.checksum.CheckSum;
import io.prometheus.client.Counter;
import org.tikv.common.TiSession;

import java.util.Properties;

/**
 * @author yuyang
 */
public class CheckSumJsonJob implements Runnable {

    private final String checkSumFilePath;
    private final String checkSumDelimiter;
    private final TiSession tiSession;
    private final Properties properties;
    private final Counter fileCounter;

    public CheckSumJsonJob(String checkSumFilePath, String checkSumDelimiter, TiSession tiSession, Properties properties, Counter fileCounter) {
        this.checkSumFilePath = checkSumFilePath;
        this.checkSumDelimiter = checkSumDelimiter;
        this.tiSession = tiSession;
        this.properties = properties;
        this.fileCounter = fileCounter;
    }

    @Override
    public void run() {
        fileCounter.labels("check sum").inc();
        String simpleCheckSum = properties.getProperty(Model.SIMPLE_CHECK_SUM);
        if (!Model.ON.equals(simpleCheckSum)) {
            CheckSum.checkSum(checkSumFilePath, checkSumDelimiter, tiSession, properties);
        } else {
            CheckSum.simpleCheckSum(checkSumFilePath, tiSession, properties);
        }
    }
}
