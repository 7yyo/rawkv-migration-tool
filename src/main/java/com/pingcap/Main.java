package com.pingcap;

import com.pingcap.checksum.CheckSum;
import com.pingcap.enums.Model;
import com.pingcap.export.LimitExporter;
import com.pingcap.export.RegionExporter;
import com.pingcap.importer.Importer;
import com.pingcap.metrics.Prometheus;
import com.pingcap.rawkv.RawKv;
import com.pingcap.util.*;
import io.prometheus.client.Counter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;

import java.util.Properties;

/**
 * @author yuyang
 * <p>
 * The entrance of the data migration tool currently implements full data import, full data export, full or sampled data verification, and some other APIs
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final String PERSONAL_PROPERTIES_PATH = "src/main/resources/importer.properties";
    static final Counter FILE_COUNTER = Counter.build().name("file_counter").help("File counter.").labelNames("file_counter").register();

    public static void main(String[] args) throws Exception {

        logger.info("Welcome.");

        String propertiesPath = System.getProperty(Model.P) == null ? PERSONAL_PROPERTIES_PATH : System.getProperty(Model.P);
        Properties properties = PropertiesUtil.getProperties(propertiesPath);
        TiSession tiSession = TiSessionUtil.getTiSession(properties);

        // Some API.
        if (System.getProperty(Model.M) != null) {
            switch (System.getProperty(Model.M)) {
                case Model.GET:
                    RawKv.get(tiSession, System.getProperty(Model.K));
                    break;
                case Model.CHECK:
                    RawKv.batchGetCheck(System.getProperty(Model.F), tiSession, properties);
                    break;
                case Model.TRUNCATE:
                    RawKv.truncateRawKv(tiSession);
                    break;
                default:
            }
        } else {
            // Full import, export, data verification
            String task = properties.getProperty(Model.TASK);
            String prometheusEnable = properties.getProperty(Model.PROMETHEUS_ENABLE);
            int prometheusPort = Integer.parseInt(properties.getProperty(Model.PROMETHEUS_PORT));
            if (Model.ON.equals(prometheusEnable)) {
                Prometheus.initPrometheus(prometheusPort);
            }
            if (StringUtils.isNotBlank(task)) {
                switch (task) {
                    case Model.IMPORT:
                        Importer.runImporter(properties, tiSession, FILE_COUNTER);
                        break;
                    case Model.CHECK_SUM:
                        CheckSum.startCheckSum(properties, tiSession, FILE_COUNTER);
                        break;
                    case Model.EXPORT:
                        String exportMode = properties.getProperty(Model.EXPORT_MODE);
                        switch (exportMode) {
                            case Model.REGION_EXPORT:
                                RegionExporter.runRegionExporter(properties, tiSession);
                                break;
                            case Model.LIMIT_EXPORT:
                                LimitExporter.runLimitExporter(properties, tiSession);
                                break;
                            default:
                        }
                        break;
                    default:
                }
            } else {
                logger.error(String.format("[%s] can not be empty.", Model.TASK));
            }
        }
        tiSession.close();
        System.exit(0);
    }

}
