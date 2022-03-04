package com.pingcap.task;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.alibaba.fastjson.util.IOUtils;
import com.pingcap.controller.ExportExecuterJob;
import com.pingcap.controller.ScannerInterface;
import com.pingcap.controller.TikvScanner;
import com.pingcap.dataformat.DataFactory;
import com.pingcap.dataformat.DataFormatInterface;
import com.pingcap.enums.Model;
import com.pingcap.pojo.LineDataText;
import com.pingcap.util.FileUtil;
import com.pingcap.util.PropertiesUtil;
import com.pingcap.util.ThreadPoolUtil;

import io.prometheus.client.Histogram;

public class Export implements TaskInterface {
    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private Map<String, String> properties = null;
    //private String pid = JavaUtil.getPid();
    private String exportFilePath;
    private String exportType;
    private int packgeSize = 0;
    private static final int LISTBUFFSIZE = 4096;
    private static final String EXPORT_FILE_PATH = "/export%s_%s_%d.txt";
    private Histogram EXPORT_DURATION = Histogram.build().name("Export_duration_").help("export duration").labelNames("type").register();
    private final AtomicInteger totalExportCount = new AtomicInteger(0);
    private final AtomicInteger totalExportIndexType = new AtomicInteger(0);
    private final AtomicInteger totalExportIndexInfo = new AtomicInteger(0);
    private final AtomicInteger totalExportTempIndex = new AtomicInteger(0);
    private final AtomicInteger totalParserError = new AtomicInteger(0);
	private final AtomicInteger totalSkipCount = new AtomicInteger(0);
    private final AtomicInteger totalIOError = new AtomicInteger(0);
    private static ArrayList<StringBuilder> wrtBufferIndexType = new ArrayList<StringBuilder>(LISTBUFFSIZE);
    private static ArrayList<StringBuilder> wrtBufferIndexInfo = new ArrayList<StringBuilder>(LISTBUFFSIZE);
    private static ArrayList<StringBuilder> wrtBufferIndexTemp = new ArrayList<StringBuilder>(LISTBUFFSIZE);
    private final boolean wrtTypes[] = {false,false,false};
    private ThreadPoolExecutor threadPoolFileWriter = ThreadPoolUtil.startJob(1, 1);
    private DataFactory dataFactory;
    
    public synchronized void fastWriteIndexType(StringBuilder line,int packgeSize){
    	wrtBufferIndexType.add(line);
    	while(wrtBufferIndexType.size() >= packgeSize){
			int filenum = filesNum.incrementAndGet();
    		writeToFile(wrtBufferIndexType,String.format(exportFilePath + EXPORT_FILE_PATH, exportType, Model.INDEX_TYPE,filenum),packgeSize);
    	}
    }
    
    private synchronized void fastWriteIndexInfo(StringBuilder line,int packgeSize){
    	wrtBufferIndexInfo.add(line);
    	while(wrtBufferIndexInfo.size() >= packgeSize){
			int filenum = filesNum.incrementAndGet();
    		writeToFile(wrtBufferIndexInfo,String.format(exportFilePath + EXPORT_FILE_PATH, exportType, Model.INDEX_INFO,filenum),packgeSize);
    	}
    }
    
    private synchronized void fastWriteIndexTemp(StringBuilder line,int packgeSize){
    	wrtBufferIndexTemp.add(line);
    	while(wrtBufferIndexTemp.size() >= packgeSize){
			int filenum = filesNum.incrementAndGet();
    		writeToFile(wrtBufferIndexTemp,String.format(exportFilePath + EXPORT_FILE_PATH, exportType, Model.TEMP_INDEX_INFO,filenum),packgeSize);
    	}
    }
    
    public void writeToFile(ArrayList<StringBuilder> wrtBuffer,String filePath,int size){
		final ArrayList<StringBuilder> wrtTmpBuffer = new ArrayList<StringBuilder>(packgeSize);
		if(-1 == size){
			//Data row order
			wrtTmpBuffer.addAll(wrtBuffer);
			wrtBuffer.clear();
		}
		else{
			//Reverse order of data rows
			for(int i=packgeSize-1;i>=0;i--){
				wrtTmpBuffer.add(wrtBuffer.get(i));
				wrtBuffer.remove(i);
			}
		}
		threadPoolFileWriter.execute(new ExportExecuterJob(wrtTmpBuffer, this, filePath));
    }
    
    public void writeAndClose(){
    	if(wrtBufferIndexType.size() > 0){
    		int filenum = filesNum.incrementAndGet();
    		writeToFile(wrtBufferIndexType,String.format(exportFilePath + EXPORT_FILE_PATH, exportType, Model.INDEX_TYPE,filenum),-1);
    	}
    	if(wrtBufferIndexInfo.size() > 0){
			int filenum = filesNum.incrementAndGet();
    		writeToFile(wrtBufferIndexInfo,String.format(exportFilePath + EXPORT_FILE_PATH, exportType, Model.INDEX_INFO,filenum),-1);
    	}
    	if(wrtBufferIndexTemp.size() > 0){
			int filenum = filesNum.incrementAndGet();
    		writeToFile(wrtBufferIndexTemp,String.format(exportFilePath + EXPORT_FILE_PATH, exportType, Model.TEMP_INDEX_INFO,filenum),-1);
    	}
    	
    	threadPoolFileWriter.shutdown();

        try {
            threadPoolFileWriter.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    public void reloadConfig(int poolSize, int poolSizeMax, int packgeSize){
    	if(0 < poolSize)
    		threadPoolFileWriter.setCorePoolSize(poolSize);
    	if(0 < poolSizeMax)
    		threadPoolFileWriter.setMaximumPoolSize(poolSizeMax);
    	if(0 < packgeSize)
    		this.packgeSize = packgeSize;
    }
    
	public static void ioWriteFileChannel(ArrayList<StringBuilder> wrtBuffer,String filePath){
            File file = FileUtil.createFile(filePath);
            FileOutputStream fileOutputStream = null;
            FileChannel fileChannel = null;
            try {
				fileOutputStream = new FileOutputStream(file);
            	fileChannel = fileOutputStream.getChannel();
            	for(int i=0;i<wrtBuffer.size();i++){
	            	fileChannel.write(StandardCharsets.UTF_8.encode(CharBuffer.wrap(wrtBuffer.get(i))));
            	}
            } catch (FileNotFoundException e) {
            	filesNum.decrementAndGet();
                e.printStackTrace();
            } catch (IOException e) {
				e.printStackTrace();
				filesNum.decrementAndGet();
			}
            finally{
            	if(null != fileChannel){
            		IOUtils.close(fileChannel);
            	}
            	if(null != fileOutputStream){
            		IOUtils.close(fileOutputStream);
            	}
            }
	}
    
	public Export() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Logger getLogger() {
		return logger;
	}

	@Override
	public Logger getLoggerFail() {
		return logger;
	}

	@Override
	public Logger getLoggerAudit() {
		return logger;
	}

	@Override
	public void checkAllParameters(Map<String, String> properties) {
		PropertiesUtil.checkNaturalNumber(properties, Model.BATCH_SIZE, false);
		PropertiesUtil.checkNaturalNumber(properties, Model.CORE_POOL_SIZE, false);
		PropertiesUtil.checkNaturalNumber(properties, Model.MAX_POOL_SIZE, false);
		
        PropertiesUtil.checkNaturalNumber( properties, Model.INTERNAL_THREAD_POOL, false);
        PropertiesUtil.checkNaturalNumber( properties, Model.INTERNAL_MAXTHREAD_POOL, false);
        threadPoolFileWriter.setCorePoolSize(Integer.parseInt(properties.get(Model.INTERNAL_THREAD_POOL)));
        threadPoolFileWriter.setMaximumPoolSize(Integer.parseInt(properties.get(Model.INTERNAL_MAXTHREAD_POOL)));
        
		PropertiesUtil.checkConfig(properties, Model.EXPORT_FILE_PATH);
        exportFilePath = properties.get(Model.EXPORT_FILE_PATH);
        PropertiesUtil.checkNaturalNumber( properties, Model.BATCHS_PACKAGE_SIZE, false);
        packgeSize = Integer.parseInt(properties.get(Model.BATCHS_PACKAGE_SIZE));
        PropertiesUtil.checkConfig(properties, Model.MODE);
        exportType = properties.get(Model.MODE);
        try {
			dataFactory = DataFactory.getInstance(exportType,properties);
		} catch (Exception e) {
            logger.error(e.getMessage());
            System.exit(0);
		}
        PropertiesUtil.checkConfig(properties, Model.EXPORT_SCENES_TYPE);
        List<String> outScenesTypeList = new ArrayList<>(Arrays.asList(properties.get(Model.EXPORT_SCENES_TYPE).split(",")));
		if(outScenesTypeList.contains(Model.INDEX_TYPE)){
			wrtTypes[0] = true;
		}
		if(outScenesTypeList.contains(Model.INDEX_INFO)){
			wrtTypes[1] = true;
		}
		if(outScenesTypeList.contains(Model.TEMP_INDEX_INFO)){
			wrtTypes[2] = true;
		}
        if(!wrtTypes[0] && !wrtTypes[1] && !wrtTypes[2]){
            logger.error("Configuration {} of item illegal",Model.EXPORT_SCENES_TYPE);
            System.exit(0);
        }
	}

	@Override
	public int executeTikv(Map<String, Object> propParameters, RawKVClient rawKvClient, AtomicInteger totalParseErrorCount, LinkedHashMap<ByteString, LineDataText> pairs,
			LinkedHashMap<ByteString, LineDataText> pairs_jmp, boolean hasTtl,String filePath,int dataSize) {
    	return 0;
	}
	
	
	public void executeSaveTo(List<Kvrpcpb.KvPair> kvPairList,String filePath) {
		Histogram.Timer transformDuration;
        boolean ret;
        
        int total = 0;
        String key,value;
        for (int i = 0; i < kvPairList.size(); i++) {
        	key = kvPairList.get(i).getKey().toStringUtf8();
        	value = kvPairList.get(i).getValue().toStringUtf8();
        	if(StringUtils.isEmpty(key)&&StringUtils.isEmpty(value)){
        		totalSkipCount.incrementAndGet();
        		continue;
        	}
            transformDuration = EXPORT_DURATION.labels("transform duration").startTimer();
            try {
				ret = dataFactory.unFormatToKeyValue( properties.get(Model.SCENES), key, value,new DataFormatInterface.UnDataFormatCallBack() {					
					@Override
					public boolean getDataCallBack( String jsonData, String type, int typeInt) {
				        StringBuilder kvPair = new StringBuilder(jsonData);
						kvPair.append("\r\n");
						switch(typeInt){
						case 0:
							if(wrtTypes[0]){
						    	totalExportIndexType.incrementAndGet();
								fastWriteIndexType(kvPair,packgeSize);
							}
							break;
						case 1:
							if(wrtTypes[1]){
								totalExportIndexInfo.incrementAndGet();
								fastWriteIndexInfo(kvPair,packgeSize);
							}
							break;
						default:
							if(wrtTypes[2]){
								totalExportTempIndex.incrementAndGet();
								fastWriteIndexTemp(kvPair,packgeSize);
							}
						}
						return true;
					}
				});
				if(ret)
					++total;
				
			} catch (Exception e) {
				totalParserError.incrementAndGet();
				logger.error("Exception unFormatToKeyValue:key={},value={}",kvPairList.get(i).getKey().toStringUtf8(),kvPairList.get(i).getValue().toStringUtf8());
				e.printStackTrace();
			}
            finally{
            	transformDuration.observeDuration();
            }
        }
        totalExportCount.addAndGet(total);
	}

	@Override
	public Histogram getHistogram() {
		return EXPORT_DURATION;
	}

	@Override
	public void succeedWriteRowsLogger(String filePath, LinkedHashMap<ByteString, LineDataText> pairs) {
	}

	@Override
	public void faildWriteRowsLogger(LinkedHashMap<ByteString, LineDataText> pairs_lines) {

	}
	
	@Override
	public void setProperties(Map<String, String> properties) {
		checkAllParameters(properties);
		this.properties = properties;
	}

	@Override
	public Map<String, String> getProperties() {
		return properties;
	}

	@Override
	public ScannerInterface getInitScanner() {
		return new TikvScanner();
	}
	
	public void printFinishedInfo(int total,long duration){
		final String headLogger = "["+this.getClass().getSimpleName()+" summary]";
		StringBuilder result = new StringBuilder(
        		headLogger +
                        " file=" + properties.get(Model.EXPORT_FILE_PATH) + ", " +
                        "total=" + total + ", " +
                        "exported=" + totalExportCount + ", " +
                        "indexType=" + totalExportIndexType + ", " +
                        "indexInfo=" + totalExportIndexInfo + ", " +
                        "empty=" + totalSkipCount  + ", " +
                        "tempIndex=" + totalExportTempIndex + ", " +
                        "parseErr=" + totalParserError + ", " +
                        "IoErr=" + totalIOError + ", " +
                        "duration=" + duration / 1000 + "s, ");
        logger.info(result.toString());
	}

	@Override
	public void finishedReport(String filePath, int importFileLineNum, int totalImportCount, int totalEmptyCount,
			int totalSkipCount, int totalParseErrorCount, int totalBatchPutFailCount, int totalDuplicateCount,
			long duration, LinkedHashMap<String, Long> ttlSkipTypeMap,Map<String, Object> propParameters) {
        StringBuilder result = new StringBuilder(
                "["+getClass().getSimpleName()+" summary]" +
                        ", Process ratio 100% file=" + filePath + ", " +
                        "total=" + importFileLineNum + ", " +
                        "exported=" + totalImportCount + ", " +
                        "empty=" + totalEmptyCount + ", " +
                        "skip=" + totalSkipCount + ", " +
                        "parseErr=" + totalParseErrorCount + ", " +
                        "duration=" + duration / 1000 + "s, ");
        result.append("Skip type[");
        for (Map.Entry<String, Long> item : ttlSkipTypeMap.entrySet()) {
            result.append("<").append(item.getKey()).append(">").append("[").append(item.getValue()).append("]").append("]");
        }
        logger.info(result.toString());
	}

	@Override
	public void installPrivateParamters(Map<String, Object> propParameters) {
		// TODO Auto-generated method stub
	}
}
