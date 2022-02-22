package com.pingcap.pojo;

import org.apache.commons.lang.StringUtils;

public class TempIndexInfo implements InfoInterface{

    // tempIndex_:_{envid}_:_{id}
    public static final String KEY_FORMAT = "tempIndex%s%s%s%s";

    private String envId;
    private String id;
    private String appId;
    private String targetId;
    private String updateTime;
    private String opType;
    private String duration;

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getTargetId() {
        return targetId;
    }

    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }

    public String getEnvId() {
        return envId;
    }

    public void setEnvId(String envId) {
        this.envId = envId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
    
	@Override
	public String getUpdateTime() {
		return updateTime;
	}
	
    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }
    
    public String getOpType() {
        return opType;
    }

    public void setOpType(String opType) {
        this.opType = opType;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public static void key2TempIndexInfo(TempIndexInfo tempIndexInfo, String key, String keyDelimiter) {
        tempIndexInfo.setEnvId(key.split(keyDelimiter)[1]);
        tempIndexInfo.setId(key.split(keyDelimiter)[2]);
    }

    public boolean equals(TempIndexInfo tempIndexInfo) {
//        boolean envIdC = this.envId.equals(tmpIndexInfo.getEnvId());
//        boolean appIdC = this.appId.equals(tempIndexInfo.getId());
//        boolean idC = this.id.equals(tempIndexInfo.getId());

        if (("".equals(this.targetId) && "".equals(tempIndexInfo.getTargetId())) || (this.targetId == null && tempIndexInfo.getTargetId() == null)) {
            return true;
        } else if (!StringUtils.isEmpty(this.targetId) && !StringUtils.isEmpty(tempIndexInfo.getTargetId())) {
            return this.targetId.equals(tempIndexInfo.getTargetId());
        } else {
            // This means that one is empty and the other is not empty, return false
            return false;
        }
    }

    public static void initValueTempIndexInfo(TempIndexInfo tempIndexInfoTiKV, TempIndexInfo tempIndexInfoCassandra) {
        // appId
        tempIndexInfoTiKV.setAppId(tempIndexInfoCassandra.getAppId());
        // target id
        tempIndexInfoTiKV.setTargetId(tempIndexInfoCassandra.getTargetId());
        if (tempIndexInfoCassandra.getOpType() != null) {
            tempIndexInfoTiKV.setOpType(tempIndexInfoCassandra.getOpType());
        }
        if (tempIndexInfoCassandra.getDuration() != null) {
            tempIndexInfoTiKV.setDuration(tempIndexInfoCassandra.getDuration());
        }
    }

	@Override
	public boolean equalsValue(Object indexInfo) {
		return equals((TempIndexInfo)indexInfo);
	}
	
}
