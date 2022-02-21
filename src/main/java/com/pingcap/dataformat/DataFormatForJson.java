package com.pingcap.dataformat;

import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.enums.Model;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.IndexType;
import com.pingcap.pojo.InfoInterface;
import com.pingcap.pojo.TempIndexInfo;

public class DataFormatForJson implements DataFormatInterface {
	
	private String updateTime;
	private String envId;
	private String keyDelimiter;
    
    // Cassandra is from original data file, TiKV is will be put raw kv.
    
	public DataFormatForJson(Map<String, String> properties) {
		this.updateTime = properties.get(Model.UPDATE_TIME);
		this.envId = properties.get(Model.ENV_ID);
        this.keyDelimiter = properties.get(Model.KEY_DELIMITER);
	}

	@Override
	public boolean formatToKeyValue(String scenes,String line,DataFormatCallBack dataFormatCallBack) throws Exception {
		JSONObject jsonObject = null;
		try {
            jsonObject = JSONObject.parseObject(line);
        } catch (Exception e) {
        	throw e;
        }
	    
        // The string type of the key.
        String k;
        // the ttl type
        String ttlType = null;
        // The kv pair to be inserted into the raw kv.
        ByteString key = ByteString.EMPTY, value = ByteString.EMPTY;
        // IndexInfo or TempIndexInfo
        switch (scenes) {
            case Model.INDEX_INFO:
                // Cassandra IndexInfo
            	IndexInfo indexInfoTiKV = new IndexInfo();
            	IndexInfo indexInfoCassandra = JSON.toJavaObject(jsonObject, IndexInfo.class);
                if (indexInfoCassandra.getUpdateTime() != null) {
                    indexInfoCassandra.setUpdateTime(indexInfoCassandra.getUpdateTime().replaceAll("T", " ").replaceAll("Z", ""));
                } else {
                    // If updateTime = null, set current time.
                    indexInfoCassandra.setUpdateTime(updateTime);
                }
                // TiKV indexInfo
                IndexInfo.initValueIndexInfoTiKV(indexInfoTiKV, indexInfoCassandra);           
                // If the configuration file is configured with envId, we need to overwrite the corresponding value in the json file
                if (!StringUtils.isEmpty(envId)) {
                    k = String.format(IndexInfo.KET_FORMAT, keyDelimiter, envId, keyDelimiter, indexInfoCassandra.getType(), keyDelimiter, indexInfoCassandra.getId());
                } else {
                    k = String.format(IndexInfo.KET_FORMAT, keyDelimiter, indexInfoCassandra.getEnvId(), keyDelimiter, indexInfoCassandra.getType(), keyDelimiter, indexInfoCassandra.getId());
                }
                
                key = ByteString.copyFromUtf8(k);
                value = ByteString.copyFromUtf8(JSONObject.toJSONString(indexInfoTiKV));
                ttlType = indexInfoCassandra.getType();
                //No ttlType = null
                if(null == ttlType)
                	ttlType = "";
                break;

            case Model.TEMP_INDEX_INFO:
        	    TempIndexInfo tempIndexInfoTiKV = new TempIndexInfo();
        	    TempIndexInfo tempIndexInfoCassandra = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
                // TiKV tempIndexInfo
                TempIndexInfo.initValueTempIndexInfo(tempIndexInfoTiKV, tempIndexInfoCassandra);
                if (!StringUtils.isEmpty(envId)) {
                    k = String.format(TempIndexInfo.KEY_FORMAT, keyDelimiter, envId, keyDelimiter, tempIndexInfoCassandra.getId());
                } else {
                    k = String.format(TempIndexInfo.KEY_FORMAT, keyDelimiter, tempIndexInfoCassandra.getEnvId(), keyDelimiter, tempIndexInfoCassandra.getId());
                }
                key = ByteString.copyFromUtf8(k);
                value = ByteString.copyFromUtf8(JSONObject.toJSONString(tempIndexInfoTiKV));
                break;
        }
        return dataFormatCallBack.putDataCallBack( ttlType, key, value);
	}

	@Override
	public boolean unFormatToKeyValue(String scenes, String key,
			String value, UnDataFormatCallBack unDataFormatCallBack) throws Exception {
        String jsonString = null;
        JSONObject jsonObject = null;
        int dataTypeInt;
        String dataType;
        if (key.startsWith(Model.INDEX_INFO)) {
        	dataType = Model.INDEX_INFO;
        	dataTypeInt = 1;
            jsonObject = JSONObject.parseObject(value);
            IndexInfo indexInfo = JSON.toJavaObject(jsonObject, IndexInfo.class);
            // key = indexInfo_:_{envid}_:_{type}_:_{id}
            String keyArr[] = key.split(keyDelimiter);
            indexInfo.setEnvId(keyArr[1]);
            indexInfo.setType(keyArr[2]);
            indexInfo.setId(keyArr[3]);
            jsonString = JSON.toJSONString(indexInfo);
        } else if (key.startsWith(Model.TEMP_INDEX_INFO)) {
        	dataType = Model.TEMP_INDEX_INFO;
        	dataTypeInt = 2;
            jsonObject = JSONObject.parseObject(value);
            TempIndexInfo tempIndexInfo = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
            // key = tempIndex_:_{envid}_:_{id}
            String keyArr[] = key.split(keyDelimiter);
            tempIndexInfo.setEnvId(keyArr[1]);
            tempIndexInfo.setId(keyArr[2]);
            jsonString = JSON.toJSONString(tempIndexInfo);
        }
        else {
        	dataType = Model.INDEX_TYPE;
        	dataTypeInt = 0;
        	jsonString = key + Model.INDEX_TYPE_DELIMITER + value;
        }
		return unDataFormatCallBack.getDataCallBack( jsonString, dataType, dataTypeInt);
	}

	@Override
	public InfoInterface packageToObject(String scenes, String key, String value, DataFormatCallBack dataFormatCallBack)
			throws Exception {
	        JSONObject jsonObject = null;
	        if (key.startsWith(Model.INDEX_INFO)) {
	            jsonObject = JSONObject.parseObject(value);
	            IndexInfo indexInfo = JSON.toJavaObject(jsonObject, IndexInfo.class);
	            // key = indexInfo_:_{envid}_:_{type}_:_{id}
	            ////String keyArr[] = key.split(keyDelimiter);
	            ////indexInfo.setEnvId(keyArr[1]);
	            ////indexInfo.setType(keyArr[2]);
	            ////indexInfo.setId(keyArr[3]);
	            return indexInfo;
	        } else if (key.startsWith(Model.TEMP_INDEX_INFO)) {
	            jsonObject = JSONObject.parseObject(value);
	            TempIndexInfo tempIndexInfo = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
	            // key = tempIndex_:_{envid}_:_{id}
	            ////String keyArr[] = key.split(keyDelimiter);
	            ////tempIndexInfo.setEnvId(keyArr[1]);
	            ////tempIndexInfo.setId(keyArr[2]);
	            return tempIndexInfo;
	        }
	        else {
	        	return (InfoInterface)new IndexType(key + Model.INDEX_TYPE_DELIMITER + value);
	        }
	}

}
