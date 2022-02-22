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
		  size = (size + size);
		  if(size > rateLimiter.getRate())
			  batchAcquire(size);
		  else
			  rateLimiter.acquire(size);
		  Map<ByteString, ByteString> buffer = new HashMap<>(kvPairs.size());
		  for(Entry<ByteString, LineDataText> obj:kvPairs.entrySet()){
			  buffer.put(obj.getKey(), obj.getValue().getValue());
		  }
		  rawKvClient.batchPut(buffer, 0);
		  buffer.clear();
		  buffer = null;
	  }

	  public static void batchPut(RawKVClient rawKvClient,Map<ByteString, LineDataText> kvPairs, long ttl,int size) {
		  if(size > rateLimiter.getRate())
			  batchAcquire(size);
		  else
			  rateLimiter.acquire(size);
		  Map<ByteString, ByteString> buffer = new HashMap<>(kvPairs.size());
		  for(Entry<ByteString, LineDataText> obj:kvPairs.entrySet()){
			  buffer.put(obj.getKey(), obj.getValue().getValue());
		  }
		  rawKvClient.batchPut(buffer, ttl);
		  buffer.clear();
		  buffer = null;
	  }
	  
	  public static List<KvPair> batchGet(RawKVClient rawKvClient,List<ByteString> keys,int size) {
		  if(size > rateLimiter.getRate())
			  batchAcquire(size);
		  else
			  rateLimiter.acquire(size);
		  return rawKvClient.batchGet(keys);
	  }
	  
	  public static void batchDelete(RawKVClient rawKvClient,List<ByteString> keys,int size) {
		  if(size > rateLimiter.getRate())
			  batchAcquire(size);
		  else
			  rateLimiter.acquire(size);
		  rawKvClient.batchDelete(keys);
	  }
	  
	  public static void testTraffic(double size) {
		  if(size > rateLimiter.getRate())
			  batchAcquire(size);
		  else
			  rateLimiter.acquire((int)size);
	  }
	  
	  public static void setRateValue(double permitsPerSecond) {
		  rateLimiter.setRate(permitsPerSecond);
	  }
	  
	  public static double getRateValue() {
		  return rateLimiter.getRate();
	  }
	  
	  public static void batchAcquire(double size) {
		  int acuqire;
		  while(0 < size){
			  acuqire = (int) rateLimiter.getRate();
			  rateLimiter.acquire(acuqire);
			  size -= acuqire;
		  }
	  }
}
