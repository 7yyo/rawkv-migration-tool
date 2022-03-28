package com.pingcap.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAdder;

import org.tikv.kvproto.Kvrpcpb;

import com.pingcap.task.Export;

public class ExportExecuterJob implements Runnable {
	public static final AtomicInteger JobtotalUsedCount = new AtomicInteger(0);
	public static final AtomicInteger WorkertotalUsedCount = new AtomicInteger(0);
	private Export exporter;
	private String filePath;
	private DoubleAdder counter;
	private List<Kvrpcpb.KvPair> g_kvPairList = null;
	private ArrayList<StringBuilder> g_stringList = null;
	
	public ExportExecuterJob( List<Kvrpcpb.KvPair> kvPairList,Export exporter, String filePath,DoubleAdder counter){
		this.exporter = exporter;
		this.filePath = filePath;
		this.g_kvPairList = new ArrayList<Kvrpcpb.KvPair>(kvPairList.size());
		this.g_kvPairList.addAll(kvPairList);
		this.counter = counter;
		JobtotalUsedCount.incrementAndGet();
	}
	
	public ExportExecuterJob( ArrayList<StringBuilder> stringList, Export exporter, String filePath){
		this.exporter = exporter;
		this.filePath = filePath;
		this.g_stringList = new ArrayList<StringBuilder>(stringList.size());
		this.g_stringList.addAll(stringList);
		WorkertotalUsedCount.incrementAndGet();
	}

	@Override
	public void run() {
		if(null != g_kvPairList){
			exporter.executeSaveTo(g_kvPairList, filePath, counter);
			JobtotalUsedCount.decrementAndGet();
			g_kvPairList.clear();
		}
		else if(null != g_stringList){
			Export.ioWriteFileChannel(g_stringList,filePath);
			g_stringList.clear();
			WorkertotalUsedCount.decrementAndGet();
		}
	}

}
