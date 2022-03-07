package com.pingcap.enums;

public class Model {

	public static final String SYS_CFG_PATH = "TIKV.CFGFILE.PATH";
    public static final String READ_TIMEOUT = "tikv.read.timeout";
    public static final String WRITE_TIMEOUT = "tikv.write.timeout";
    public static final String BATCH_READ_TIMEOUT = "tikv.batchRead.timeout";
    public static final String BATCH_WRITE_TIMEOUT = "tikv.batchWrite.timeout";
    public static final String SCAN_TIMEOUT = "tikv.scan.timeout";
    public static final String CLEAN_TIMEOUT = "tikv.clean.timeout";

    public static final String IMPORT = "import";
    public static final String CHECK_SUM = "checksum";
    public static final String UNIMPORT = "unimport";
    public static final String EXPORT = "export";

    public static final String JSON_FORMAT = "json";
    public static final String CSV_FORMAT = "csv";
    public static final String ROWB64_FORMAT = "rowb64";

    public static final String ON = "true";

    public static final String P = "p";
    public static final String M = "m";
    public static final String K = "k";
    public static final String GET = "get";
    public static final String TRUNCATE = "truncate";
    public static final String DELETE_BY_PREFIX = "deleteByPrefix";
    public static final String ADD = "add";
    public static final String UPDATE = "update";
    public static final String DELETE = "delete";

    public static final String INDEX_INFO = "indexInfo";
    public static final String TEMP_INDEX_INFO = "tempIndexInfo";
    public static final String INDEX_TYPE = "indexType";
    public static final String INDEX_TYPE_DELIMITER = "@";

    public static final String LOG = "logger";
    public static final String CHECK_SUM_LOG = "checkSumLog";
    public static final String AUDIT_LOG = "auditLog";
    public static final String REDO_LOG = "redoLog";
    public static final String CS_FAIL_LOG = "checkSumFailLog";
    public static final String BP_FAIL_LOG = "batchPutFailLog";
    public static final String REDO_FAIL_LOG = "redoFailLog";

    public static final String TASK = "tikv.task";
    public static final String TASKSPEEDLIMIT = "tikv.task.speedLimit";
    public static final String PD = "importer.tikv.pd";

    public static final String CHECK_SUM_MOVE_PATH = "importer.checkSum.movePath";

    public static final String IMPORT_FILE_PATH = "importer.in.filePath";
    public static final String MODE = "importer.in.mode";
    public static final String SCENES = "importer.in.scenes";
    public static final String INTERNAL_THREAD_POOL = "importer.tikv.internalPool";
    public static final String INTERNAL_MAXTHREAD_POOL = "importer.tikv.maxInternalPoolSize";
    public static final String INTERNAL_THREAD_NUM = "importer.tikv.internalThreadNum";
    public static final String CORE_POOL_SIZE = "importer.tikv.corePoolSize";
    public static final String MAX_POOL_SIZE = "importer.tikv.maxPoolSize";
    public static final String BATCHS_PACKAGE_SIZE = "importer.in.BatchsPackageSize";
    public static final String BATCH_SIZE = "importer.tikv.batchSize";

    public static final String DELIMITER_1 = "importer.in.delimiter_1";
    public static final String DELIMITER_2 = "importer.in.delimiter_2";
    public static final String KEY_DELIMITER = "importer.in.keyDelimiter";
    public static final String CHECK_EXISTS_KEY = "importer.in.checkExistsKey";
    public static final String ENV_ID = "importer.out.envId";
    public static final String APP_ID = "importer.out.appId";
    public static final String TTL_SKIP_TYPE = "importer.ttl.skip.type";
    public static final String TTL_PUT_TYPE = "importer.ttl.put.type";
    public static final String TTL = "importer.in.ttl";
    public static final String TIMER_INTERVAL = "importer.timer.interval";
    public static final String PROMETHEUS_ENABLE = "importer.prometheus.enable";
    public static final String PROMETHEUS_PORT = "importer.prometheus.port";
    public static final String UPDATE_TIME = "importer.in.updateTime";

    public static final String EXPORT_FILE_PATH = "exporter.out.filePath";
    public static final String EXPORT_SCENES_TYPE = "exporter.out.scenes.type";

    public static final String REDO_MOVE_PATH = "redo.movePath";

}
