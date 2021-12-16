package com.pingcap.dataformat;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.tikv.shade.com.google.protobuf.ByteString;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.PascalNameFilter;
import com.pingcap.dataformat.DataFormatInterface.DataFormatCallBack;
import com.pingcap.enums.Model;
import com.pingcap.pojo.IndexInfo;
import com.pingcap.pojo.ServiceTag;

import io.prometheus.client.Histogram.Timer;

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
	public boolean formatToKeyValue(Timer timer, AtomicInteger totalParseErrorCount, String scenes, String line,
			DataFormatCallBack dataFormatCallBack) throws Exception {
		String type = null;
		ByteString key = ByteString.EMPTY, value = ByteString.EMPTY;
		if(Model.INDEX_TYPE.equals(scenes)) {
			String arr[] = line.split(Model.INDEX_TYPE_DELIMITER);
			if(2 != arr.length)
				throw new Exception("IndexType format error");
            // Key@Value
            key = ByteString.copyFromUtf8(arr[0]);
            if (StringUtils.isEmpty(key.toStringUtf8())) {
                throw new Exception("IndexType key is empty");
            }
            value = ByteString.copyFromUtf8(arr[1]);
		}
		else {
		    IndexInfo indexInfoTiKV = new IndexInfo();	    
	        String id = line.split(delimiter1)[0];
	        type = line.split(delimiter1)[1];
	        String k = String.format(IndexInfo.KET_FORMAT, keyDelimiter, envId, keyDelimiter, type, keyDelimiter, id);
	        // CSV has no timestamp, so don't consider.
	        String targetId = line.split(delimiter1)[2].split(delimiter2)[0];
	        indexInfoTiKV.setTargetId(targetId);
	        indexInfoTiKV.setAppId(appId);
	        String v = line.split(delimiter1)[2];
	
	        // except <id|type|targetId>
	        if (v.split(delimiter2).length > 1) {
	            ServiceTag serviceTag = new ServiceTag();
	            String[] vs = v.split(delimiter2);
	            if (vs.length == 2) {
	                // id|type|targetId##BLKMDL_ID
	                serviceTag.setBLKMDL_ID(vs[1]);
	            } else {
	                // id|type|targetId##BLKMDL_ID##PD_SALE_FTA_CD##ACCT_DTL_TYPE##CORPPRVT_FLAG##CMTRST_CST_ACCNO##AR_ID##QCRCRD_IND
	                serviceTag.setBLKMDL_ID(v.split(delimiter2)[1]);
	                serviceTag.setPD_SALE_FTA_CD(v.split(delimiter2)[2]);
	                serviceTag.setACCT_DTL_TYPE(v.split(delimiter2)[3]);
	                serviceTag.setCORPPRVT_FLAG(v.split(delimiter2)[4]);
	                serviceTag.setCMTRST_CST_ACCNO(v.split(delimiter2)[5]);
	                serviceTag.setAR_ID(v.split(delimiter2)[6]);
	                serviceTag.setQCRCRD_IND(v.split(delimiter2)[7]);
	            }
	            indexInfoTiKV.setServiceTag(JSON.toJSONString(serviceTag, nameFilter));
	        }
	
	        indexInfoTiKV.setUpdateTime(updateTime);
	
	        key = ByteString.copyFromUtf8(k);
	        value = ByteString.copyFromUtf8(JSONObject.toJSONString(indexInfoTiKV));
		}
        return dataFormatCallBack.putDataCallBack( type, key, value);
	}

}
