package com.pingcap.task;

import static com.pingcap.enums.Model.DELIMITER_1;
import static com.pingcap.enums.Model.DELIMITER_2;
import static com.pingcap.enums.Model.KEY_DELIMITER;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAdder;
import org.slf4j.Logger;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.pingcap.controller.ScannerInterface;
import com.pingcap.enums.Model;
import com.pingcap.pojo.LineDataText;
import com.pingcap.util.PropertiesUtil;
import io.prometheus.client.Histogram;
import io.prometheus.client.Counter;

public interface TaskInterface {
	static final Counter BATCH_PUT_FAIL_COUNTER = Counter.build().name("batch_put_fail_counter").help("Batch put fail counter.").labelNames("batch_put_fail").register();
    static final Histogram REQUEST_LATENCY = Histogram.build().name("requests_latency_seconds").help("Request latency in seconds.").labelNames("request_latency").register();
    //static public final AtomicDouble totalDataBytes = new AtomicDouble(0);  
    static public DoubleAdder totalDataBytes = new DoubleAdder();
    static public final AtomicInteger filesNum = new AtomicInteger(0);
    
	public Logger getLogger();
	public Logger getLoggerAudit();
	public Logger getLoggerFail();

	public Map<String, String> getProperties();
	public void setProperties(Map<String, String> properties);
	public void installPrivateParamters(Map<String, Object> propParameters);
	public int executeTikv(Map<String, Object> propParameters, RawKVClient rawKvClient, AtomicInteger totalParseErrorCount, LinkedHashMap<ByteString, LineDataText> pairs, LinkedHashMap<ByteString, LineDataText> pairs_jmp, boolean hasTtl,String filePath ,int dataSize);
	public void  succeedWriteRowsLogger(String filePath, LinkedHashMap<ByteString, LineDataText> pairs);
	public void  faildWriteRowsLogger(LinkedHashMap<ByteString, LineDataText> pairs);
	public ScannerInterface getInitScanner();
	public void finishedReport(String filePath,
			int importFileLineNum,
			int totalImportCount,
			int totalEmptyCount,
			int totalSkipCount,
			int totalParseErrorCount,
			int totalBatchPutFailCount,
			int totalDuplicateCount,
			long duration,
			LinkedHashMap<String, Long> ttlSkipTypeMap,Map<String, Object> propParameters);
	
	public void checkAllParameters(Map<String, String> properties);
	public Histogram getHistogram();
	
	public static void checkShareParameters(Map<String, String> properties) {
        PropertiesUtil.checkNaturalNumber( properties, Model.CORE_POOL_SIZE, false);
        PropertiesUtil.checkNaturalNumber( properties, Model.MAX_POOL_SIZE, false);
        PropertiesUtil.checkNaturalNumber( properties, Model.INTERNAL_THREAD_NUM, false);
        PropertiesUtil.checkNaturalNumber( properties, Model.INTERNAL_THREAD_POOL, false);
        PropertiesUtil.checkNaturalNumber( properties, Model.INTERNAL_MAXTHREAD_POOL, false);
        PropertiesUtil.checkNaturalNumber( properties, Model.TIMER_INTERVAL, false);
        PropertiesUtil.checkNaturalNumber( properties, Model.BATCH_SIZE, false);
        PropertiesUtil.checkConfig(properties, KEY_DELIMITER);
        // For CSV format, There may be have two delimiter, if CSV has only one delimiter, delimiter2 is invalid.
        PropertiesUtil.checkConfig(properties, DELIMITER_1);
        PropertiesUtil.checkConfig(properties, DELIMITER_2);
        PropertiesUtil.checkNaturalNumber( properties, Model.BATCHS_PACKAGE_SIZE, false);
        PropertiesUtil.checkNumberFromTo( properties, Model.TASKSPEEDLIMIT, false,10,1000);
		PropertiesUtil.checkConfig(properties, Model.CHECK_EXISTS_KEY);
	}
	
}
