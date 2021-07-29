package com.pingcap.job;

import com.pingcap.checksum.CheckSum;

import static com.pingcap.enums.Model.*;

import org.tikv.common.TiSession;

import java.util.Map;
import java.util.Properties;

public class CheckSumJsonJob implements Runnable {

    private final String checkSumFilePath;
    private final TiSession tiSession;
    private final Map<String, String> properties;

    public CheckSumJsonJob(String checkSumFilePath, TiSession tiSession, Map<String, String> properties) {
        this.checkSumFilePath = checkSumFilePath;
        this.tiSession = tiSession;
        this.properties = properties;
    }

    @Override
    public void run() {
        String simpleCheckSum = properties.get(SIMPLE_CHECK_SUM);
        if (!ON.equals(simpleCheckSum)) {
            CheckSum.checkSum(checkSumFilePath, tiSession, properties);
        } else {
            CheckSum.doRedo(checkSumFilePath, tiSession, properties);
        }
    }

}
