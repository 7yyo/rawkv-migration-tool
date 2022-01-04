package com.pingcap.dataformat;

import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.tikv.shade.com.google.protobuf.ByteString;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.PascalNameFilter;
import com.pingcap.enums.Model;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.ServiceTag;

public class DataFormatForCsv implements DataFormatInterface {
	private String keyDelimiter;
	private String delimiter1;
	private String delimiter2;
	private String envId;
	private String appId;
	private String updateTime;
    PascalNameFilter nameFilter = new PascalNameFilter();
	
	public DataFormatForCsv(Map<String, String> properties) {
		this.keyDelimiter = properties.get(Model.KEY_DELIMITER);
		this.delimiter1 = properties.get(Model.DELIMITER_1);
		this.delimiter2 = properties.get(Model.DELIMITER_2);
		this.updateTime = properties.get(Model.UPDATE_TIME);
		this.envId = properties.get(Model.ENV_ID);
		this.appId = properties.get(Model.APP_ID);
	}

    /**
     * 1. id|type|targetId
     * 2. id|type|targetId##BLKMDL_ID
     * 3. id|type|targetId##BLKMDL_ID##PD_SALE_FTA_CD##ACCT_DTL_TYPE##CORPPRVT_FLAG##CMTRST_CST_ACCNO##AR_ID##QCRCRD_IND
     * 4. id,type,targetId
     */
	@Override
	public boolean formatToKeyValue(String scenes, String line,
			DataFormatCallBack dataFormatCallBack) throws Exception {
		String type = null;
		ByteString key = ByteString.EMPTY, value = ByteString.EMPTY;
		if(Model.INDEX_TYPE.equals(scenes)) {
			String arr[] = line.split(Model.INDEX_TYPE_DELIMITER);
			if(2 != arr.length)
				throw new Exception("IndexType format error");
			if(StringUtils.isBlank(arr[0]))
				throw new Exception("IndexType key is empty");
            // Key@Value
            key = ByteString.copyFromUtf8(arr[0]);
            if (StringUtils.isEmpty(key.toStringUtf8())) {
                throw new Exception("IndexType key is empty");
            }
            value = ByteString.copyFromUtf8(arr[1]);
		}
		else {
		    IndexInfo indexInfoTiKV = new IndexInfo();
		    String arr[] = line.split(delimiter1);
		    if(3 != arr.length)
		    	throw new Exception("indexInfo format error");
	        final String id = arr[0];
	        type = arr[1];
	        String k = String.format(IndexInfo.KET_FORMAT, keyDelimiter, envId, keyDelimiter, type, keyDelimiter, id);
	        // CSV has no timestamp, so don't consider.
	        String extArr[] = arr[2].split(delimiter2);
	        indexInfoTiKV.setTargetId(extArr[0]);
	        indexInfoTiKV.setAppId(appId);
	
	        // except <id|type|targetId>
	        if (extArr.length > 1) {
	            ServiceTag serviceTag = new ServiceTag();
	            if (extArr.length == 2) {
	                // id|type|targetId##BLKMDL_ID
	                serviceTag.setBLKMDL_ID(extArr[1]);
	            } else {
	                // id|type|targetId##BLKMDL_ID##PD_SALE_FTA_CD##ACCT_DTL_TYPE##CORPPRVT_FLAG##CMTRST_CST_ACCNO##AR_ID##QCRCRD_IND
	                serviceTag.setBLKMDL_ID(extArr[1]);
	                serviceTag.setPD_SALE_FTA_CD(extArr[2]);
	                serviceTag.setACCT_DTL_TYPE(extArr[3]);
	                serviceTag.setCORPPRVT_FLAG(extArr[4]);
	                serviceTag.setCMTRST_CST_ACCNO(extArr[5]);
	                serviceTag.setAR_ID(extArr[6]);
	                serviceTag.setQCRCRD_IND(extArr[7]);
	            }
	            indexInfoTiKV.setServiceTag(JSON.toJSONString(serviceTag, nameFilter));
	        }
	
	        indexInfoTiKV.setUpdateTime(updateTime);
	
	        key = ByteString.copyFromUtf8(k);
	        value = ByteString.copyFromUtf8(JSONObject.toJSONString(indexInfoTiKV));
		}
        return dataFormatCallBack.putDataCallBack( type, key, value);
	}

	@Override
	public boolean unFormatToKeyValue(String scenes, String key,
			String value, UnDataFormatCallBack unDataFormatCallBack) throws Exception {
		String jsonString = null;
		String dataType;
		int dataTypeInt;
        if (key.startsWith(Model.INDEX_INFO)) {
        	dataType = Model.INDEX_INFO;
        	JSONObject jsonObject = JSONObject.parseObject(value);
        	IndexInfo indexInfoTiKV = JSON.toJavaObject(jsonObject, IndexInfo.class);
        	String keyArr[] = key.split(keyDelimiter);
        	// key = indexInfo_:_{envid}_:_{type}_:_{id}
        	// id|type|targetId##BLKMDL_ID
        	// id|type|targetId##BLKMDL_ID##PD_SALE_FTA_CD##ACCT_DTL_TYPE##CORPPRVT_FLAG##CMTRST_CST_ACCNO##AR_ID##QCRCRD_IND
        	String tag = indexInfoTiKV.getServiceTag();
        	jsonString = keyArr[3]+delimiter2+keyArr[2]+delimiter2+indexInfoTiKV.getTargetId();
        	if(!StringUtils.isBlank(tag)) {
        		jsonObject = JSONObject.parseObject(tag);
        		ServiceTag serviceTag = JSON.toJavaObject(jsonObject, ServiceTag.class);
        		if( StringUtils.isBlank(serviceTag.getPD_SALE_FTA_CD())&&
        				StringUtils.isBlank(serviceTag.getACCT_DTL_TYPE())&&
        				StringUtils.isBlank(serviceTag.getCORPPRVT_FLAG())&&
        				StringUtils.isBlank(serviceTag.getCMTRST_CST_ACCNO())&&
        				StringUtils.isBlank(serviceTag.getAR_ID())&&
        				StringUtils.isBlank(serviceTag.getQCRCRD_IND())
        				)
            		jsonString += (delimiter2+serviceTag.getBLKMDL_ID());
        		else
        			jsonString += (delimiter2+serviceTag.getBLKMDL_ID()+delimiter2+serviceTag.getPD_SALE_FTA_CD()+delimiter2+serviceTag.getACCT_DTL_TYPE()+delimiter2+serviceTag.getCORPPRVT_FLAG()+delimiter2+serviceTag.getCMTRST_CST_ACCNO()+delimiter2+serviceTag.getAR_ID()+delimiter2+serviceTag.getQCRCRD_IND());
        	}
            dataTypeInt = 1;
        }
        else {
        	dataType = Model.INDEX_TYPE;
        	dataTypeInt = 0;
        	jsonString = key + Model.INDEX_TYPE_DELIMITER + value;
        }
		return unDataFormatCallBack.getDataCallBack( jsonString, dataType, dataTypeInt);
	}

}
