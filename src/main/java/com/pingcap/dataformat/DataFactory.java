package com.pingcap.dataformat;

import java.util.Map;
import com.pingcap.enums.Model;
import org.tikv.shade.com.google.protobuf.ByteString;
import com.pingcap.pojo.InfoInterface;

public class DataFactory implements DataFormatInterface {

	private DataFormatInterface formatInterface = null;
	
	public static DataFactory getInstance(String importMode,Map<String, String> properties) throws Exception {
		return new DataFactory(importMode,properties);
	}
	
	public DataFactory(String importMode,Map<String, String> properties) throws Exception {
		if(Model.JSON_FORMAT.equals(importMode)) {
			formatInterface = new DataFormatForJson(properties);
		}
		else if(Model.CSV_FORMAT.equals(importMode)) {
			formatInterface = new DataFormatForCsv(properties);
		}
		else if(Model.ROWB64_FORMAT.equals(importMode)) {
			formatInterface = new DataFormatForRowB64(properties);
		}		
		else{
			throw new Exception("importer.in.mode in configration illegal:["+importMode+"]");
		}
	}

	@Override
	public boolean formatToKeyValue(String scenes,String line,DataFormatCallBack dataFormatCallBack) throws Exception {
		return formatInterface.formatToKeyValue(scenes,line,dataFormatCallBack);
	}

	@Override
	public boolean unFormatToKeyValue(String scenes, String key,
			String value, UnDataFormatCallBack unDataFormatCallBack) throws Exception {
		return formatInterface.unFormatToKeyValue(scenes,key,value,unDataFormatCallBack);
	}

	@Override
	public InfoInterface packageToObject(String scenes, ByteString key, ByteString value, DataFormatCallBack dataFormatCallBack)
			throws Exception {
		return formatInterface.packageToObject(scenes, key, value, dataFormatCallBack);
	}

}
