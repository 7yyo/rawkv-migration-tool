package com.pingcap.task;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.pingcap.controller.ScannerInterface;
import com.pingcap.controller.TikvScanner;
import com.pingcap.dataformat.DataFactory;
import com.pingcap.dataformat.DataFormatInterface;
import com.pingcap.enums.Model;
import com.pingcap.util.FileUtil;
import com.pingcap.util.JavaUtil;
import com.pingcap.util.PropertiesUtil;

import io.prometheus.client.Histogram;

public class Export implements TaskInterface {
    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);
    private Map<String, String> properties = null;
    private String pid = JavaUtil.getPid();
    private String exportFilePath;
    private int batchSize = 0;
    private int startLine=0;
    private static final String EXPORT_FILE_PATH = "/export_%s_%d-%d.txt";
    private Histogram EXPORT_DURATION = Histogram.build().name("Export_duration_"+pid).help("export duration").labelNames("type").register();
    private final AtomicInteger totalExportCount = new AtomicInteger(0);
    private final AtomicInteger totalExportIndexType = new AtomicInteger(0);
    private final AtomicInteger totalExportIndexInfo = new AtomicInteger(0);
    private final AtomicInteger totalExportTempIndex = new AtomicInteger(0);
    private final AtomicInteger totalParserError = new AtomicInteger(0);
    private final AtomicInteger totalIOError = new AtomicInteger(0);
    Map<String,Object[]> fileChannelList = new HashMap<>();
    
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
		PropertiesUtil.checkNaturalNumber(properties, Model.EXPORT_LIMIT, false);
		PropertiesUtil.checkNaturalNumber(properties, Model.EXPORT_THREAD, false);
		PropertiesUtil.checkConfig(properties, Model.EXPORT_FILE_PATH);
        exportFilePath = properties.get(Model.EXPORT_FILE_PATH);
        batchSize = Integer.parseInt(properties.get(Model.BATCH_SIZE));
	}

	@Override
	public HashMap<ByteString, ByteString> executeTikv(RawKVClient rawKvClient, HashMap<ByteString, ByteString> pairs,
			HashMap<ByteString, String> pairs_lines, boolean hasTtl,String filePath) {
    	return pairs;
	}
	
	public Object[] getWriteFileChannel(String channelName){
		//FileChannel fileChannel
		//FileOutputStream fileOutputStream;
		Object[] vec = fileChannelList.get(channelName);
		if(null == vec){
			vec = new Object[3];
            File file = FileUtil.createFile(String.format(exportFilePath + EXPORT_FILE_PATH, channelName,startLine,batchSize));
            try {
            	@SuppressWarnings("resource")
				FileOutputStream fileOutputStream = new FileOutputStream(file);
            	FileChannel fileChannel = fileOutputStream.getChannel();
            	vec[0] = fileOutputStream;
            	vec[1] = fileChannel;
            	vec[2] = (int)0;
                fileChannelList.put( channelName, vec);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
		}
		return vec;
	}
	
	public void closeWriteFileChannel(String channelName,Object[] vec){
		//Object[] vec = fileChannelList.get(channelName);
		if(null != vec){
			FileOutputStream fileOutputStream = (FileOutputStream)vec[0];
			FileChannel fileChannel = (FileChannel)vec[1];
			if(null != fileChannel){
				try {
					fileChannel.close();
				} catch (IOException e) {
					fileChannel = null;
				}
			}
			if(null != fileOutputStream){
				try {
					fileOutputStream.close();
				} catch (IOException e) {
					fileOutputStream = null;
				}
			}
			fileChannelList.remove(channelName);
		}
	}
	
	public Object[] putWriteFileChannel(String channelName,Object[] vec,StringBuilder kvPair) throws IOException{
		if(null == vec)
			vec = getWriteFileChannel(channelName);
		int num = (int)vec[2];
		num ++;
		if(num >= batchSize){
			closeWriteFileChannel(channelName,vec);
			vec = getWriteFileChannel(channelName);
		}

		vec[2] = num;
		fileChannelList.put( channelName, vec);
        FileChannel fileChannel = (FileChannel)vec[1];
        Histogram.Timer writeDuration = EXPORT_DURATION.labels("write duration").startTimer();
        try {
            ByteBuffer line = StandardCharsets.UTF_8.encode(CharBuffer.wrap(kvPair));
            fileChannel.write(line);
            kvPair.setLength(0);
        } catch (IOException e) {
            throw e;
        }
        finally{
            writeDuration.observeDuration();	
        }
		return vec;
	}
	
	public void executeSaveTo(List<Kvrpcpb.KvPair> kvPairList, int startLine,int curLines,String filePath) {
		Histogram.Timer transformDuration;
        StringBuilder kvPair = new StringBuilder();
        Object[][] writer = new Object[3][];
        boolean ret;
        DataFactory dataFactory = DataFactory.getInstance(properties.get(Model.MODE),properties);
        for (int i = startLine; i < kvPairList.size(); i++) {

            transformDuration = EXPORT_DURATION.labels("transform duration").startTimer();
            try {
				ret = dataFactory.unFormatToKeyValue( properties.get(Model.SCENES), kvPairList.get(i).getKey().toStringUtf8(),kvPairList.get(i).getValue().toStringUtf8(),new DataFormatInterface.UnDataFormatCallBack() {					
					@Override
					public boolean getDataCallBack( String jsonData, String type, int typeInt) {
						try{
							kvPair.append(jsonData).append("\n");
							switch(typeInt){
							case 0:
								totalExportIndexType.incrementAndGet();
				                writer[0] = putWriteFileChannel(Model.INDEX_TYPE,writer[0],kvPair);
								break;
							case 1:
								totalExportIndexInfo.incrementAndGet();
								writer[1] = putWriteFileChannel(Model.INDEX_INFO,writer[1],kvPair);
								break;
							default:
								totalExportTempIndex.incrementAndGet();
				            	writer[2] = putWriteFileChannel(Model.TEMP_INDEX_INFO,writer[2],kvPair);	
							}
							return true;
						}
						catch(IOException e){
							totalIOError.incrementAndGet();
							logger.error("IOException unFormatToKeyValue:{}",e.getMessage());
				        	e.printStackTrace();
						}
						return false;
					}
				});
				if(ret)
					totalExportCount.incrementAndGet();
				
			} catch (Exception e) {
				totalParserError.incrementAndGet();
				logger.error("Exception unFormatToKeyValue:key={},value={}",kvPairList.get(i).getKey().toStringUtf8(),kvPairList.get(i).getValue().toStringUtf8());
				e.printStackTrace();
			}
            finally{
            	transformDuration.observeDuration();
            }
        }

	}

	@Override
	public Histogram getHistogram() {
		return EXPORT_DURATION;
	}

	@Override
	public void succeedWriteRowsLogger(String filePath, HashMap<ByteString, ByteString> pairs) {
		for(Entry<ByteString, ByteString> obj:pairs.entrySet()){
			getLogger().debug("File={}, key={}, value={}", filePath, obj.getKey().toStringUtf8(), obj.getValue().toStringUtf8());
		}
	}

	@Override
	public void faildWriteRowsLogger(HashMap<ByteString, ByteString> pairs) {
		for(Entry<ByteString, ByteString> obj:pairs.entrySet()){
			logger.info(obj.getValue().toStringUtf8());
		}
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
                        "tempIndex=" + totalExportTempIndex + ", " +
                        "parseErr=" + totalParserError + ", " +
                        "IoErr=" + totalIOError + ", " +
                        "duration=" + duration / 1000 + "s, ");
        logger.info(result.toString());
	}

	@Override
	public void finishedReport(String filePath, int importFileLineNum, int totalImportCount, int totalEmptyCount,
			int totalSkipCount, int totalParseErrorCount, int totalBatchPutFailCount, int totalDuplicateCount,
			long duration, LinkedHashMap<String, Long> ttlSkipTypeMap) {
        StringBuilder result = new StringBuilder(
                "["+getClass().getSimpleName()+" summary]" +
                        ", Process ratio 100% file=" + filePath + ", " +
                        "total=" + importFileLineNum + ", " +
                        "exported=" + totalImportCount + ", " +
                        "empty=" + totalEmptyCount + ", " +
                        "skip=" + totalSkipCount + ", " +
                        "parseErr=" + totalParseErrorCount + ", " +
                        //"putErr=" + totalBatchPutFailCount + ", " +
                        //"duplicate=" + totalDuplicateCount + ", " +
                        "duration=" + duration / 1000 + "s, ");
        result.append("Skip type[");
        for (Map.Entry<String, Long> item : ttlSkipTypeMap.entrySet()) {
            result.append("<").append(item.getKey()).append(">").append("[").append(item.getValue()).append("]").append("]");
        }
        logger.info(result.toString());
	}
}
