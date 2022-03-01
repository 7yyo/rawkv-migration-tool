package com.pingcap.dataformat;

import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang.StringUtils;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.pingcap.enums.Model;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.InfoInterface;
import com.pingcap.pojo.RowB64;
import com.pingcap.pojo.TempIndexInfo;

public class DataFormatForRowB64 implements DataFormatInterface {

	public DataFormatForRowB64(Map<String, String> properties) {
	}

	@Override
	public boolean formatToKeyValue(String scenes, String line, DataFormatCallBack dataFormatCallBack)
			throws Exception {
		ByteString key = ByteString.EMPTY, value = ByteString.EMPTY;
		
		String arr[] = line.split(Model.INDEX_TYPE_DELIMITER,-1);
		if(2 != arr.length){
			throw new Exception("rowb64 format error");
		}
		String tmpKey = arr[0];
		if(StringUtils.isBlank(tmpKey))
			throw new Exception("rowb64 source key is empty");	
		tmpKey = new String(DatatypeConverter.parseBase64Binary(tmpKey),"utf8");
		if(StringUtils.isBlank(tmpKey))
			throw new Exception("rowb64 key is empty");
        // Key@Value
        key = ByteString.copyFromUtf8(tmpKey);
        if (StringUtils.isEmpty(key.toStringUtf8())) {
            throw new Exception("rowb64 key is empty");
        }
		if(null != arr[1]){
			value = ByteString.copyFromUtf8(new String(DatatypeConverter.parseBase64Binary(arr[1]),"utf8"));
		}
		return dataFormatCallBack.putDataCallBack( "", key, value);
	}

	@Override
	public InfoInterface packageToObject(String scenes, ByteString key, ByteString value, DataFormatCallBack dataFormatCallBack)
			throws Exception {
		return (InfoInterface)new RowB64(key,value);
	}

	@Override
	public boolean unFormatToKeyValue(String scenes, String key, String value,
			UnDataFormatCallBack unDataFormatCallBack) throws Exception {
		StringBuffer jsonString = new StringBuffer();
		String dataType;
		int dataTypeInt;
	       if (key.startsWith(IndexInfo.HEADFORMAT)) {
	        	if(DataFormatInterface.isJsonString(value)){
		        	dataType = Model.INDEX_INFO;
		            dataTypeInt = 1;
	        	}
	        	else{
	            	dataType = Model.INDEX_TYPE;
	            	dataTypeInt = 0;
	        	}
	        }
	        else if (key.startsWith(TempIndexInfo.HEADFORMAT)) {
	        	if(DataFormatInterface.isJsonString(value)){
		        	dataType = Model.TEMP_INDEX_INFO;
		        	dataTypeInt = 2;
	        	}
	        	else{
	            	dataType = Model.INDEX_TYPE;
	            	dataTypeInt = 0;
	        	}
	        }
	        else {
	        	dataType = Model.INDEX_TYPE;
	        	dataTypeInt = 0;
	        }
		jsonString.append(DatatypeConverter.printBase64Binary(key.getBytes("utf8"))).append(Model.INDEX_TYPE_DELIMITER).append(DatatypeConverter.printBase64Binary(value.getBytes("utf8")));
		return unDataFormatCallBack.getDataCallBack( jsonString.toString(), dataType, dataTypeInt);
	}

}
