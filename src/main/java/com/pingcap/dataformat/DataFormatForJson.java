package com.pingcap.dataformat;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.enums.Model;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.TempIndexInfo;

import io.prometheus.client.Histogram;

public class DataFormatForJson implements DataFormatInterface {
	
	private String updateTime;
	private String envId;
	private String keyDelimiter;
    
    private JSONObject jsonObject = null;
    // Cassandra is from original data file, TiKV is will be put raw kv.
    IndexInfo indexInfoCassandra = null;
    IndexInfo indexInfoTiKV = new IndexInfo();
    TempIndexInfo tempIndexInfoCassandra = null;
    TempIndexInfo tempIndexInfoTiKV = new TempIndexInfo();
    
	public DataFormatForJson(Map<String, String> properties) {
		this.updateTime = properties.get(Model.UPDATE_TIME);
		this.envId = properties.get(Model.ENV_ID);
        this.keyDelimiter = properties.get(Model.KEY_DELIMITER);
	}

	@Override
	public boolean formatToKeyValue(Histogram.Timer timer,AtomicInteger totalParseErrorCount, String scenes,String line,DataFormatCallBack dataFormatCallBack) throws Exception {
		try {
            jsonObject = JSONObject.parseObject(line);
            timer.observeDuration();
        } catch (Exception e) {
        	throw e;
        }
        IndexInfo indexInfoCassandra;
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
                indexInfoCassandra = JSON.toJavaObject(jsonObject, IndexInfo.class);
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
                tempIndexInfoCassandra = JSON.toJavaObject(jsonObject, TempIndexInfo.class);
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

}
