package com.pingcap;

import com.pingcap.checksum.CheckSum;
import com.pingcap.enums.Model;
import com.pingcap.export.Exporter;
import com.pingcap.importer.Importer;
import com.pingcap.metrics.Prometheus;
import com.pingcap.rawkv.RawKv;
import com.pingcap.redo.Redo;
import com.pingcap.util.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;

import java.util.Map;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final String PERSONAL_PROPERTIES_PATH = "src/main/resources/rawkv.properties";

    public static void main(String[] args) throws Exception {

        logger.info("Welcome to Raw KV Migration tool!");

        String propertiesPath = System.getProperty(Model.P) == null ? PERSONAL_PROPERTIES_PATH : System.getProperty(Model.P);
        Map<String, String> properties = PropertiesUtil.getProperties(propertiesPath);
        TiSession tiSession = TiSessionUtil.getTiSession(properties);

        // Some API.
        if (!StringUtils.isEmpty(System.getProperty(Model.M))) {
            switch (System.getProperty(Model.M)) {
                case Model.GET:
                    RawKv.get(tiSession, System.getProperty(Model.K));
                    break;
                case Model.TRUNCATE:
                    RawKv.truncateRawKv(tiSession);
                    break;
                case Model.DELETE:
                    RawKv.deleteByKey(tiSession, System.getProperty(Model.K));
                    break;
                default:
                    throw new IllegalStateException(System.getProperty(Model.M));
            }
        } else {

            String task = properties.get(Model.TASK);
            String prometheusEnable = properties.get(Model.PROMETHEUS_ENABLE);

            if (Model.ON.equals(prometheusEnable)) {
                int prometheusPort = Integer.parseInt(properties.get(Model.PROMETHEUS_PORT));
                Prometheus.initPrometheus(prometheusPort);
            }
            if (!StringUtils.isEmpty(task)) {
                switch (task) {
                    case Model.IMPORT:
                        Importer.run(properties, tiSession);
                        break;
                    case Model.CHECK_SUM:
                        CheckSum.run(properties, tiSession);
                        break;
                    case Model.EXPORT:
                        Exporter.run(properties, tiSession);
                        break;
                    case Model.REDO:
                        Redo.run(properties, tiSession);
                        break;
                    default:
                        throw new IllegalStateException(task);
                }
            } else {
                logger.error("{} cannot be empty.", Model.TASK);
            }
        }

        tiSession.close();

        System.exit(0);

    }

}
