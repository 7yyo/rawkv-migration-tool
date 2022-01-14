package com.pingcap.rawkv;

import java.util.List;
import java.util.Map;

import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.google.common.util.concurrent.RateLimiter;


public class LimitSpeedkv {
	private static final RateLimiter rateLimiter = RateLimiter.create(java.lang.Double.MAX_VALUE);
	
	  public static void batchPut(RawKVClient rawKvClient,Map<ByteString, ByteString> kvPairs,int size) {
		  rateLimiter.acquire(size+size);
		  rawKvClient.batchPut(kvPairs, 0);
	  }

	  public static void batchPut(RawKVClient rawKvClient,Map<ByteString, ByteString> kvPairs, long ttl,int size) {
		  rateLimiter.acquire(size+size);
		  rawKvClient.batchPut(kvPairs, ttl);
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
