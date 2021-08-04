package com.pingcap.metrics;

import com.pingcap.enums.Model;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author yuyang
 */
public class Prometheus {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    public static void initPrometheus(int prometheusPort) {
        try {
            new HTTPServer(prometheusPort);
            DefaultExports.initialize();
            logger.info("Run prometheus success. Port={}", prometheusPort);
        } catch (IOException e) {
            logger.error("Run prometheus failed. Port={}", prometheusPort, e);
            System.exit(0);
        }
    }

}
