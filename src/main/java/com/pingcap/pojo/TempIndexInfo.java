package com.pingcap.pojo;

public class TempIndexInfo {

    public static final String KEY_FORMAT = "tempIndex_:_%s_:_%s";

    private String envId;
    private String id;
    private String appId;
    private String targetId;
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
        boolean targetIdC = this.targetId.equals(tempIndexInfo.getTargetId());
        return targetIdC;
    }

    public static void initValueTempIndexInfo(TempIndexInfo tempIndexInfoTiKV, TempIndexInfo tempIndexInfoCassandra) {
        // appId
        tempIndexInfoTiKV.setAppId(tempIndexInfoCassandra.getAppId());
        // target id
        tempIndexInfoTiKV.setTargetId(tempIndexInfoCassandra.getTargetId());
    }
}
