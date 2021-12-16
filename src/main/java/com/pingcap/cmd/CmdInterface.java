package com.pingcap.cmd;

import static com.pingcap.enums.Model.DELIMITER_1;
import static com.pingcap.enums.Model.DELIMITER_2;
import static com.pingcap.enums.Model.KEY_DELIMITER;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.pingcap.enums.Model;
import com.pingcap.util.PropertiesUtil;

public interface CmdInterface {
	public Logger getLogger();
	public Logger getLoggerProcess();
	public Logger getLoggerFail();
	
	public void setTtl(long ttl);
	public boolean executeTikv( RawKVClient rawKvClient, HashMap<ByteString, ByteString> pairs, HashMap<ByteString, String> pairs_lines, boolean hasTtl);
	
	public void checkAllParameters(Map<String, String> properties);
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
