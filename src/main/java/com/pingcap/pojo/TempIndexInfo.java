package com.pingcap.pojo;

public class TempIndexInfo {

    public static final String TEMP_INDEX_INFO_KEY_FORMAT = "tempIndex_:_%s_:_%s";

    private String envId;
    private String id;
    private String appId;
    private String targetId;
    private int fileLine;

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

    public int getFileLine() {
        return fileLine;
    }

    public void setFileLine(int fileLine) {
        this.fileLine = fileLine;
    }

    public boolean equals(TempIndexInfo tmpIndexInfo) {
//        boolean envIdC = this.envId.equals(tmpIndexInfo.getEnvId());
        boolean idC = this.id.equals(tmpIndexInfo.getId());
//        boolean appIdC = this.appId.equals(tmpIndexInfo.getId());
        boolean targetIdC = this.targetId.equals(tmpIndexInfo.getTargetId());
        return idC && targetIdC;
    }

    public static TempIndexInfo initTempIndexInfo(TempIndexInfo tempIndexInfos) {
        TempIndexInfo tempIndexInfo = new TempIndexInfo();
        tempIndexInfo.setTargetId(tempIndexInfos.getTargetId());
        tempIndexInfo.setAppId(tempIndexInfos.getAppId());
        return tempIndexInfo;
    }
}
