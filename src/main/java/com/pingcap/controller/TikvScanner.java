package com.pingcap.controller;

import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;

import com.pingcap.enums.Model;
import com.pingcap.task.Export;
import com.pingcap.task.TaskInterface;
import com.pingcap.timer.ExportTimer;
import com.pingcap.util.FileUtil;
import com.pingcap.util.ThreadPoolUtil;
import org.tikv.shade.com.google.protobuf.ByteString;
import io.prometheus.client.Histogram;

public class TikvScanner implements ScannerInterface {
    private static final AtomicInteger exportTotalCounter = new AtomicInteger(0);
    
	@Override
	public void run(TiSession tiSession, TaskInterface cmdInterFace) {
        long startTime = System.currentTimeMillis();
        final Map<String, String> properties = cmdInterFace.getProperties();
       
        cmdInterFace.checkAllParameters(properties);
		Export exporter = (Export)cmdInterFace;
        int limit = Integer.parseInt(properties.get(Model.EXPORT_LIMIT))+1;
        int thread = Integer.parseInt(properties.get(Model.EXPORT_THREAD));
        String exportFilePath = properties.get(Model.EXPORT_FILE_PATH);

        FileUtil.deleteFolder(exportFilePath);
        FileUtil.createFolder(exportFilePath);

        int interval = Integer.parseInt(properties.get(Model.TIMER_INTERVAL));

        ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.startJob(thread, thread);

        Timer timer = new Timer();
        ExportTimer exportTimer = new ExportTimer(exportTotalCounter);
        timer.schedule(exportTimer, 5000, interval);

        ByteString startKey = ByteString.EMPTY;
        ByteString lastStartKey = ByteString.EMPTY;
        List<Kvrpcpb.KvPair> kvPairList;
        RawKVClient rawKvClient = tiSession.createRawClient();
        boolean isReadyEnd = false;
        int curLines = 0;
        //Random random = new Random();
        while (true) {
            Histogram.Timer scanDuration = cmdInterFace.getHistogram().labels("scan duration").startTimer();
            try{
            	kvPairList = rawKvClient.scan(startKey, limit);
            }
            catch(Exception e){
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
            if(0 == exportTotalCounter.get()){
            	curLines = kvPairList.size();//TOTAL_EXPORT_NUM.addAndGet(kvPairList.size());
            	exporter.executeSaveTo(kvPairList, 0,curLines, exportFilePath);
            }
            else{
            	curLines = kvPairList.size()-1;//TOTAL_EXPORT_NUM.addAndGet(kvPairList.size()-1);
            	exporter.executeSaveTo(kvPairList, 1,curLines, exportFilePath);
            }
            exportTotalCounter.addAndGet(curLines);
            
            if(0 < kvPairList.size())
            	startKey = kvPairList.get(kvPairList.size() - 1).getKey();
            kvPairList.clear();
        }
        exportTimer.cancel();
        rawKvClient.close();

        exporter.printFinishedInfo(exportTotalCounter.get(),System.currentTimeMillis() - startTime);
	}

}
