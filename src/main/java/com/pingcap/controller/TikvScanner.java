package com.pingcap.controller;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;

import com.pingcap.enums.Model;
import com.pingcap.rawkv.LimitSpeedkv;
import com.pingcap.task.Export;
import com.pingcap.task.TaskInterface;
import com.pingcap.timer.ExportTimer;
import com.pingcap.timer.SystemMonitorTimer;
import com.pingcap.util.FileUtil;
import com.pingcap.util.PropertiesUtil;
import com.pingcap.util.ThreadPoolUtil;
import org.tikv.shade.com.google.protobuf.ByteString;
import io.prometheus.client.Histogram;

public class TikvScanner implements ScannerInterface {
    private static final AtomicInteger exportTotalCounter = new AtomicInteger(0);
    
    private long lastModifiedDate = 0;
    
	@Override
	public void run(TiSession tiSession, TaskInterface cmdInterFace) {
        long startTime = System.currentTimeMillis();
        final Map<String, String> properties = cmdInterFace.getProperties();
       
        cmdInterFace.checkAllParameters(properties);
		Export exporter = (Export)cmdInterFace;
        int limit = Integer.parseInt(properties.get(Model.BATCH_SIZE))+1;
        String exportFilePath = properties.get(Model.EXPORT_FILE_PATH);

        FileUtil.deleteFolder(exportFilePath);
        FileUtil.createFolder(exportFilePath);

        int interval = Integer.parseInt(properties.get(Model.TIMER_INTERVAL));

        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(Integer.parseInt(properties.get(Model.CORE_POOL_SIZE)), Integer.parseInt(properties.get(Model.MAX_POOL_SIZE)));

        ExportTimer exportTimer = new ExportTimer(threadPoolExecutor,cmdInterFace,exportTotalCounter);
        scannerTimer.schedule(exportTimer, 5000, interval);

        SystemMonitorTimer taskTimer = new SystemMonitorTimer(cmdInterFace);
        taskTimer.getDateAndUpdate(FileUtil.getFileLastTime(properties.get(Model.SYS_CFG_PATH)), true);
        scannerTimer.schedule(taskTimer, 5000, Long.parseLong(properties.get(Model.TIMER_INTERVAL)));
        lastModifiedDate = taskTimer.getDateAndUpdate(0, false);
        
        ByteString startKey = ByteString.EMPTY;
        ByteString lastStartKey = ByteString.EMPTY;
        List<Kvrpcpb.KvPair> kvPairList;
        RawKVClient rawKvClient = tiSession.createRawClient();
        boolean isReadyEnd = false;
        double traffic = 0;

        //Random random = new Random();
        int debug_count = 0;
        while (true) {
        	if(lastModifiedDate != taskTimer.getDateAndUpdate(0, false)){
        		lastModifiedDate = taskTimer.getDateAndUpdate(0, false);
        		PropertiesUtil.reloadConfiguration(threadPoolExecutor,cmdInterFace);
        		limit = Integer.parseInt(properties.get(Model.BATCH_SIZE))+1;
        		exporter.reloadConfig(Integer.parseInt(properties.get(Model.CORE_POOL_SIZE)), Integer.parseInt(properties.get(Model.MAX_POOL_SIZE)), Integer.parseInt(properties.get(Model.BATCHS_PACKAGE_SIZE)));
        	}
            Histogram.Timer scanDuration = cmdInterFace.getHistogram().labels("scan duration").startTimer();
            try{
            	kvPairList = rawKvClient.scan(startKey, limit);
            	debug_count ++;
            	exporter.getLogger().debug("debug_count:{}[startKey={},lastStartKey={}],kvPairList:{}",(++debug_count),startKey,lastStartKey,kvPairList.size());
            }
            catch(Exception e){
            	e.printStackTrace();
            	continue;
            }
            finally{
            	scanDuration.observeDuration();
            }
            if(isReadyEnd){
                if(1 >= kvPairList.size() && startKey.equals(lastStartKey)){
                    threadPoolExecutor.shutdown();
                    cmdInterFace.getLogger().info("Complete data export. Total number of exported rows={}", exportTotalCounter);
                    break;           	
                }
            }
            else if(limit > kvPairList.size()){
            	isReadyEnd = true;
            	if(0 < kvPairList.size())
            		lastStartKey = kvPairList.get(kvPairList.size() - 1).getKey();
            }
            if(0 < kvPairList.size()){
            	//If it is 0, it may cause LimitSpeedkv.testTraffic execution error 
            	traffic = 1;
            	for(int i=0;i<kvPairList.size();i++){
            		traffic += (kvPairList.get(i).getKey().toStringUtf8().length() + kvPairList.get(i).getValue().toStringUtf8().length());
            	}
            	startKey = kvPairList.get(kvPairList.size() - 1).getKey();
	            if(0 != exportTotalCounter.get())
	            	kvPairList.remove(0);
	            exportTotalCounter.addAndGet(kvPairList.size());
	            threadPoolExecutor.execute(new ExportExecuterJob( kvPairList, exporter, exportFilePath));
	            	
	            kvPairList.clear();
            	TaskInterface.totalDataBytes.addAndGet(traffic);
            	//The returned data is compressed. 
            	//The rate cannot be accurately obtained. 
	            LimitSpeedkv.testTraffic(traffic);
            }
        }
        threadPoolExecutor.shutdown();

        try {
            if (threadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
            	exporter.printFinishedInfo(exportTotalCounter.get(),System.currentTimeMillis() - startTime);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        exportTimer.cancel();
        rawKvClient.close();

        exporter.writeAndClose();
        taskTimer.cancel();
        scannerTimer.cancel();
	}

}
