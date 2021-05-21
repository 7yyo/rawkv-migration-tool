package com.pingcap.pojo;

public class IndexInfoS {

    private String envId;
    private String type;
    private String id;
    private String appId;
    private String serviceTag;
    private String targetId;
    private String createTime;
    private String updateTime;
    private int fileLine;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getEnvId() {
        return envId;
    }

    public void setEnvId(String envId) {
        this.envId = envId;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getServiceTag() {
        return serviceTag;
    }

    public void setServiceTag(String serviceTag) {
        this.serviceTag = serviceTag;
    }

    public String getTargetId() {
        return targetId;
    }

    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public int getFileLine() {
        return fileLine;
    }

    public void setFileLine(int fileLine) {
        this.fileLine = fileLine;
    }

    public boolean equals(IndexInfoT indexInfoT) {
        boolean idC = this.id.equals(indexInfoT.getId());
        String createTime = indexInfoT.getCreateTime().replaceAll("T", " ").replaceAll("Z", "");
        boolean createTimeC = this.createTime.equals(createTime);
        boolean serviceTagC = this.serviceTag.trim().equals(indexInfoT.getServiceTag().trim());
        boolean targetIdC = this.targetId.equals(indexInfoT.getTargetId());
        boolean typeC = this.type.equals(indexInfoT.getType());
        return idC & createTimeC && serviceTagC && targetIdC && typeC;
    }
}
