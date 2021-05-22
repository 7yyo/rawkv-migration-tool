package com.pingcap.enums;

public class Model {

    public static final String JSON_FORMAT = "json";
    public static final String CSV_FORMAT = "csv";

    public static final String INDEX_INFO = "indexInfo";
    public static final String TEMP_INDEX_INFO = "tempIndexInfo";
    public static final String INDEX_TYPE = "indexType";

    public static final String LOG = "logger";
    public static final String CHECK_SUM_LOG = "checkSumLog";
    public static final String AUDIT_LOG = "auditLog";

    public static final String PD = "importer.tikv.pd";
    public static final String INTERNAL_THREAD_NUM = "importer.tikv.internalThreadNum";
    public static final String CORE_POOL_SIZE = "importer.tikv.corePoolSize";
    public static final String MAX_POOL_SIZE = "importer.tikv.maxPoolSize";
    public static final String CHECK_SUM_THREAD_NUM = "importer.checkSum.checkSumThreadNum";
    public static final String BATCH_SIZE = "importer.tikv.batchSize";
    public static final String DELETE_FOR_TEST = "importer.tikv.deleteForTest";
    public static final String ENABLE_CHECK_SUM = "importer.checkSum.enabledCheckSum";
    public static final String CHECK_SUM_FILE_PATH = "importer.checkSum.checkSumFilePath";
    public static final String CHECK_SUM_DELIMITER = "importer.checkSum.checkSumDelimiter";
    public static final String CHECK_SUM_PERCENTAGE = "importer.checkSum.checkSumPercentage";
    public static final String FILE_PATH = "importer.in.filePath";
    public static final String MODE = "importer.in.mode";
    public static final String SCENES = "importer.in.scenes";
    public static final String DELIMITER_1 = "importer.in.delimiter_1";
    public static final String DELIMITER_2 = "importer.in.delimiter_2";
    public static final String ENV_ID = "importer.out.envId";
    public static final String APP_ID = "importer.out.appId";
    public static final String TTL_TYPE = "importer.ttl.type";
    public static final String TTL_DAY = "importer.ttl.day";
    public static final String TIMER_INTERVAL = "importer.timer.interval";

}
