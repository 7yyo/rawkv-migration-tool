package com.pingcap.pojo;

public class TempIndexInfo {

    public static final String TEMP_INDEX_INFO_KEY_FORMAT = "tempIndex_:_%s_:_%s";

    private String envId;
    private String id;
    private String appId;
    private String targetId;

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

    public static void key2TempIndexInfo(TempIndexInfo tempIndexInfo, String key, String keyDelimiter) {
        tempIndexInfo.setEnvId(key.split(keyDelimiter)[1]);
        tempIndexInfo.setId(key.split(keyDelimiter)[2]);
    }

    /**
     * Compare indexInfo
     * Comparison strategy: id, targetId
     *
     * @param tempIndexInfo: tempIndexInfo object
     * @return boolean
     */
    public boolean equals(TempIndexInfo tempIndexInfo) {
//        boolean envIdC = this.envId.equals(tmpIndexInfo.getEnvId());
//        boolean appIdC = this.appId.equals(tmpIndexInfo.getId());
        boolean idC = this.id.equals(tempIndexInfo.getId());
        boolean targetIdC = this.targetId.equals(tempIndexInfo.getTargetId());
        return idC && targetIdC;
    }

    public static void initTempIndexInfo(TempIndexInfo tempIndexInfoT, TempIndexInfo tempIndexInfoS) {
        // value -
        // appId、targetId
        tempIndexInfoT.setAppId(tempIndexInfoS.getAppId());
        tempIndexInfoT.setTargetId(tempIndexInfoS.getTargetId());
    }
}
