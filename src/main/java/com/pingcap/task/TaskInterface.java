package com.pingcap.task;

import static com.pingcap.enums.Model.DELIMITER_1;
import static com.pingcap.enums.Model.DELIMITER_2;
import static com.pingcap.enums.Model.KEY_DELIMITER;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.pingcap.controller.ScannerInterface;
import com.pingcap.enums.Model;
import com.pingcap.util.PropertiesUtil;
import io.prometheus.client.Histogram;
import io.prometheus.client.Counter;

public interface TaskInterface {
	static final Counter BATCH_PUT_FAIL_COUNTER = Counter.build().name("batch_put_fail_counter").help("Batch put fail counter.").labelNames("batch_put_fail").register();
    static final Histogram REQUEST_LATENCY = Histogram.build().name("requests_latency_seconds").help("Request latency in seconds.").labelNames("request_latency").register();
    
	public Logger getLogger();
	public Logger getLoggerAudit();
	public Logger getLoggerFail();

	public Map<String, String> getProperties();
	public void setProperties(Map<String, String> properties);
	public HashMap<ByteString, ByteString> executeTikv(RawKVClient rawKvClient, HashMap<ByteString, ByteString> pairs, HashMap<ByteString, String> pairs_lines, boolean hasTtl,String filePath);
	public void  succeedWriteRowsLogger(String filePath, HashMap<ByteString, ByteString> pairs);
	public void  faildWriteRowsLogger(HashMap<ByteString, ByteString> pairs);
	public ScannerInterface getInitScanner();
	
	public void checkAllParameters(Map<String, String> properties);
	public Histogram getHistogram();
	
	public static void checkShareParameters(Map<String, String> properties) {
        PropertiesUtil.checkNaturalNumber( properties, Model.CORE_POOL_SIZE, false);
        PropertiesUtil.checkNaturalNumber( properties, Model.MAX_POOL_SIZE, false);
        PropertiesUtil.checkNaturalNumber( properties, Model.INTERNAL_THREAD_NUM, false);
        PropertiesUtil.checkNaturalNumber( properties, Model.INTERNAL_THREAD_POOL, false);
        PropertiesUtil.checkNaturalNumber( properties, Model.TIMER_INTERVAL, false);
        PropertiesUtil.checkNaturalNumber( properties, Model.BATCH_SIZE, false);
        PropertiesUtil.checkConfig(properties, KEY_DELIMITER);
        // For CSV format, There may be have two delimiter, if CSV has only one delimiter, delimiter2 is invalid.
        PropertiesUtil.checkConfig(properties, DELIMITER_1);
        PropertiesUtil.checkConfig(properties, DELIMITER_2);
	}
}
