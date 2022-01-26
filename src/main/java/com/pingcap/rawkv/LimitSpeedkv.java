package com.pingcap.rawkv;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.google.common.util.concurrent.RateLimiter;
import com.pingcap.pojo.LineDataText;


public class LimitSpeedkv {
	private static final RateLimiter rateLimiter = RateLimiter.create(java.lang.Double.MAX_VALUE);
	
	  public static void batchPut(RawKVClient rawKvClient,Map<ByteString, LineDataText> kvPairs,int size) {
		  rateLimiter.acquire(size+size);
		  Map<ByteString, ByteString> buffer = new HashMap<>(kvPairs.size());
		  for(Entry<ByteString, LineDataText> obj:kvPairs.entrySet()){
			  buffer.put(obj.getKey(), obj.getValue().getValue());
		  }
		  rawKvClient.batchPut(buffer, 0);
		  buffer.clear();
		  buffer = null;
	  }

	  public static void batchPut(RawKVClient rawKvClient,Map<ByteString, LineDataText> kvPairs, long ttl,int size) {
		  rateLimiter.acquire(size+size);
		  Map<ByteString, ByteString> buffer = new HashMap<>(kvPairs.size());
		  for(Entry<ByteString, LineDataText> obj:kvPairs.entrySet()){
			  buffer.put(obj.getKey(), obj.getValue().getValue());
		  }
		  rawKvClient.batchPut(buffer, ttl);
		  buffer.clear();
		  buffer = null;
	  }
	  
	  public static List<KvPair> batchGet(RawKVClient rawKvClient,List<ByteString> keys,int size) {
		  rateLimiter.acquire(size+size);
		  return rawKvClient.batchGet(keys);
	  }
	  
	  public static void batchDelete(RawKVClient rawKvClient,List<ByteString> keys,int size) {
		  rateLimiter.acquire(size+size);
		  rawKvClient.batchDelete(keys);
	  }
	  
	  public static void setRateValue(double permitsPerSecond) {
		  rateLimiter.setRate(permitsPerSecond);
	  }
	  
	  public static double getRateValue() {
		  return rateLimiter.getRate();
	  }	  
}
