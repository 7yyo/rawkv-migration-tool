package com.pingcap.dataformat;

import java.util.concurrent.atomic.AtomicInteger;
import org.tikv.shade.com.google.protobuf.ByteString;
import io.prometheus.client.Histogram;

public interface DataFormatInterface {
	public static String DATA_LINENO = "LINENO";
	public static String DATA_LINEDATA = "LINEDATA";
	
	public interface DataFormatCallBack {
		public boolean putDataCallBack(String ttlType, ByteString key,ByteString value);
	}
	
	public boolean formatToKeyValue( Histogram.Timer timer, AtomicInteger totalParseErrorCount, String scenes, String line, DataFormatCallBack dataFormatCallBack) throws Exception;
	
}
