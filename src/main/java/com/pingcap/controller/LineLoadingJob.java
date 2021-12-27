package com.pingcap.controller;

import com.pingcap.enums.Model;
import com.pingcap.task.TaskInterface;
import com.pingcap.timer.TaskTimer;
import com.pingcap.util.FileUtil;
import com.pingcap.util.ThreadPoolUtil;

import io.prometheus.client.Histogram;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.tikv.common.TiSession;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class LineLoadingJob implements Runnable {
    private final String importFilePath;
    private final TiSession tiSession;

    private final AtomicInteger totalImportCount = new AtomicInteger(0);
    private final AtomicInteger totalEmptyCount = new AtomicInteger(0);
    private final AtomicInteger totalSkipCount = new AtomicInteger(0);
    private final AtomicInteger totalParseErrorCount = new AtomicInteger(0);
    private final AtomicInteger totalBatchPutFailCount = new AtomicInteger(0);
    private final AtomicInteger totalDuplicateCount = new AtomicInteger(0);
    private ThreadPoolExecutor threadPoolFileLoading = null;
    private TaskInterface cmdInterFace = null;
    private LinkedHashMap<String, Long> ttlSkipTypeMap = new LinkedHashMap<>();
    private List<String> ttlSkipTypeList = new ArrayList<>();
    private List<String> ttlPutList = new ArrayList<>();
    private int threadsNumber = 0;
    
    public LineLoadingJob( TaskInterface cmdInterFace, String importFilePath, TiSession tiSession, List<String> ttlSkipTypeList, List<String> ttlPutList) {
        this.importFilePath = importFilePath;
        this.tiSession = tiSession;
        this.cmdInterFace = cmdInterFace;
        Map<String, String> properties = cmdInterFace.getProperties();
        this.threadPoolFileLoading = ThreadPoolUtil.startJob(Integer.parseInt(properties.get(Model.INTERNAL_THREAD_POOL)), Integer.parseInt(properties.get(Model.INTERNAL_MAXTHREAD_POOL)));
        properties = null;
        this.ttlSkipTypeList.addAll(ttlSkipTypeList);
        // Used to count the number of skipped entries for each ttl type.
        if (!ttlSkipTypeList.isEmpty()) {
            FileUtil.cloneToTtlSkipTypeMap(ttlSkipTypeMap,ttlSkipTypeList);
        }
        this.ttlPutList.addAll(ttlPutList);
    }
    
    @Override
    public void run() {

        long startTime = System.currentTimeMillis();
        final Map<String, String> properties = cmdInterFace.getProperties();
        final String headLogger = "["+cmdInterFace.getClass().getSimpleName()+" summary]";

        // Start the file sub-thread, import the data of the file through the sub-thread, and divide the data in advance according to the number of sub-threads.
        File importFile = new File(importFilePath);
        final String absolutePath  = importFile.getAbsolutePath();
        final int importFileLineNum = FileUtil.getFileLines(importFile);

        final int internalThreadNum = Integer.parseInt(properties.get(Model.INTERNAL_THREAD_NUM));
        ////List<String> threadPerLineList = CountUtil.getPerThreadFileLines(importFileLineNum, internalThreadNum, importFile.getAbsolutePath());

        Timer timer = new Timer();
        TaskTimer importTimer = new TaskTimer(cmdInterFace, totalImportCount, importFileLineNum, importFilePath);
        timer.schedule(importTimer, 5000, Long.parseLong(properties.get(Model.TIMER_INTERVAL)));

        // Block until all child threads end.
/*        int avgTmp = importFileLineNum / internalThreadNum;
        if(importFileLineNum > avgTmp*internalThreadNum) {
        	avgTmp += ((importFileLineNum - avgTmp*internalThreadNum + internalThreadNum - 1)/internalThreadNum);
        }
        final int avg = avgTmp;*/
        
        int fileSplitSize = Integer.parseInt(properties.get(Model.BATCHS_PACKAGE_SIZE));
        if(importFileLineNum <= fileSplitSize){
        	fileSplitSize = (importFileLineNum+internalThreadNum-1) / internalThreadNum;
        }
        int splitLimit = fileSplitSize;
        final int countDownNum = (importFileLineNum+fileSplitSize-1)/fileSplitSize;
        CountDownLatch countDownLatch = new CountDownLatch(countDownNum);
        LineIterator lineIterator = null;
 
        final Map<String, String> container = new HashMap<String, String>(fileSplitSize+1);
        Histogram.Timer fileBlockTimer = cmdInterFace.getHistogram().labels("split file").startTimer();
        try {
			lineIterator = FileUtils.lineIterator(importFile, "UTF-8");
            // If the data file has a large number of rows, the block time may be slightly longer
            for (int m = 1; m <= importFileLineNum; m++) {
            	try {
            		container.put(""+ m, lineIterator.nextLine());
                } catch (Exception e) {
                	--splitLimit;
                	cmdInterFace.getLoggerFail().error("LineIterator error, file = {} ,error={}", absolutePath, e);
                    totalParseErrorCount.incrementAndGet();
                    continue;
                }
                if(splitLimit <= container.size()) {
                	splitLimit = fileSplitSize;
                	threadPoolFileLoading.execute(
							new BatchJob(
	                			tiSession,
	                            totalImportCount,
	                            totalEmptyCount,
	                            totalSkipCount,
	                            totalParseErrorCount,
	                            totalBatchPutFailCount,
	                            absolutePath,
	                            ttlSkipTypeList,
	                            ttlSkipTypeMap,
	                            container,
	                            countDownLatch,
	                            totalDuplicateCount,
	                            ttlPutList,
	                            cmdInterFace));
                	++threadsNumber;
                	container.clear();
                }
            }
            if(0 < container.size()) {	
            	threadPoolFileLoading.execute(
						new BatchJob(
	                        tiSession,
	                        totalImportCount,
	                        totalEmptyCount,
	                        totalSkipCount,
	                        totalParseErrorCount,
	                        totalBatchPutFailCount,
	                        absolutePath,
	                        ttlSkipTypeList,
	                        ttlSkipTypeMap,
	                        container,
	                        countDownLatch,
	                        totalDuplicateCount,
	                        ttlPutList,
	                        cmdInterFace));
            	++threadsNumber;
            	container.clear();
            }
        	fileBlockTimer.observeDuration();
		}
        catch (RuntimeException e1) {
        	e1.printStackTrace();
        	System.exit(1);
        } 
        catch(Error e1){
        	e1.printStackTrace();
        	System.exit(1);
        }
        catch (Throwable e1) {
        	e1.printStackTrace();
        	System.exit(1);
        }
        finally {
        	try {
				lineIterator.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
        	fileBlockTimer.close();
        }
        cmdInterFace.getLogger().info("file={}, line={}, each processes={}, countDownNum={},threadsNumber={}", absolutePath, importFileLineNum, fileSplitSize, countDownNum, threadsNumber);
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        threadPoolFileLoading.shutdown();
        long duration = System.currentTimeMillis() - startTime;
        StringBuilder result = new StringBuilder(
        		headLogger +
                        ", Process ratio 100% file=" + importFile.getAbsolutePath() + ", " +
                        "total=" + importFileLineNum + ", " +
                        "imported=" + totalImportCount + ", " +
                        "empty=" + totalEmptyCount + ", " +
                        "skip=" + totalSkipCount + ", " +
                        "parseErr=" + totalParseErrorCount + ", " +
                        "putErr=" + totalBatchPutFailCount + ", " +
                        "duplicate=" + totalDuplicateCount + ", " +
                        "duration=" + duration / 1000 + "s, ");
        result.append("Skip type[");
        for (Map.Entry<String, Long> item : ttlSkipTypeMap.entrySet()) {
            result.append("<").append(item.getKey()).append(">").append("[").append(item.getValue()).append("]").append("]");
        }
        ttlPutList.clear();
        ttlPutList = null;
        ttlSkipTypeMap.clear();
        ttlSkipTypeMap = null;
//        logger.info("Check sum file={} complete. Total={}, totalCheck={}, notExists={}, skip={}, parseErr={}, checkSumFail={}", checkSumFile.getAbsolutePath(), totalCount, totalCheck, notInsert, skip, parseErr, checkSumFail);
        timer.cancel();
        cmdInterFace.getLogger().info(result.toString());

    }

}
