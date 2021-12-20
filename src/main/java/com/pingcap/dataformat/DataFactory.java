package com.pingcap.dataformat;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.tikv.shade.com.google.protobuf.ByteString;

import com.pingcap.enums.Model;

import io.prometheus.client.Histogram;

public class DataFactory implements DataFormatInterface {

	private DataFormatInterface formatInterface = null;
	
	public static DataFactory getInstance(String importMode,Map<String, String> properties) {
		return new DataFactory(importMode,properties);
	}
	
	public DataFactory(String importMode,Map<String, String> properties) {
		if(Model.JSON_FORMAT.equals(importMode)) {
			formatInterface = new DataFormatForJson(properties);
		}
		else if(Model.CSV_FORMAT.equals(importMode)) {
			formatInterface = new DataFormatForCsv(properties);
		}
	}

	@Override
	public boolean formatToKeyValue(Histogram.Timer timer,AtomicInteger totalParseErrorCount, String scenes,String line,DataFormatCallBack dataFormatCallBack) throws Exception {
		return formatInterface.formatToKeyValue( timer, totalParseErrorCount, scenes, line, dataFormatCallBack);
	}
	
	public static String getRowsHeader(HashMap<ByteString, String> kvPairLines){
		StringBuffer rowsHeader = new StringBuffer();
		if(null == kvPairLines)
			return rowsHeader.toString();
		for(Entry<ByteString, String> obj:kvPairLines.entrySet()){
			if(0 == rowsHeader.length())
				rowsHeader.append(obj.getValue());
			else
				rowsHeader.append(",").append(obj.getValue());
		}
		return rowsHeader.toString();
	}

}
