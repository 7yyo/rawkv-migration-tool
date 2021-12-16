package com.pingcap.cmd;

import static com.pingcap.enums.Model.REDO_FAIL_LOG;
import static com.pingcap.enums.Model.REDO_FILE_PATH;
import static com.pingcap.enums.Model.REDO_MOVE_PATH;
import static com.pingcap.enums.Model.REDO_TYPE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.pingcap.enums.Model;
import com.pingcap.util.PropertiesUtil;

public class UnImport implements CmdInterface {
    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private static final Logger redoLog = LoggerFactory.getLogger(Model.REDO_LOG);
    private static final Logger redoFailLog = LoggerFactory.getLogger(REDO_FAIL_LOG);
    @SuppressWarnings("unused")
	private long ttl = 0;
    
	public UnImport() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Logger getLogger() {
		return logger;
	}

	@Override
	public Logger getLoggerProcess() {
		return redoLog;
	}

	@Override
	public Logger getLoggerFail() {
		return redoFailLog;
	}

	@Override
	public void checkAllParameters(Map<String, String> properties) {
		CmdInterface.checkShareParameters(properties);
		
        // Redo file
        PropertiesUtil.checkConfig(properties, REDO_FILE_PATH);
        // Redo.log move path
        PropertiesUtil.checkConfig(properties, REDO_MOVE_PATH);
        // Redo data type, indexInfo or tempIndexInfo
        PropertiesUtil.checkConfig(properties, REDO_TYPE);
	}

	@Override
	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	@Override
	public boolean executeTikv(RawKVClient rawKvClient, HashMap<ByteString, ByteString> pairs,
			HashMap<ByteString, String> pairs_lines, boolean hasTtl) {
		List<ByteString> keyList = new ArrayList<>(pairs.size());
		for(Entry<ByteString, ByteString> op:pairs.entrySet()) {
			keyList.add(op.getKey());
		}
		rawKvClient.batchDelete(keyList);
		return true;
	}

}
