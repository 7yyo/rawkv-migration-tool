package com.pingcap.pojo;

import org.apache.commons.lang.StringUtils;

public class IndexInfo {

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

    public boolean equals(IndexInfo indexInfo) {
        boolean idC = this.id.equals(indexInfo.getId());
        boolean serviceTagC = this.serviceTag.trim().equals(indexInfo.getServiceTag().trim());
        boolean targetIdC = this.targetId.equals(indexInfo.getTargetId());
        boolean typeC = this.type.equals(indexInfo.getType());
        return idC && serviceTagC && targetIdC && typeC;
    }

    public static IndexInfo initIndexInfoT(IndexInfo indexInfoS, String time) {
        IndexInfo indexInfo = new IndexInfo();
        indexInfo.setTargetId(indexInfoS.getTargetId());
        indexInfo.setAppId(indexInfo.getAppId());
        if (StringUtils.isNotBlank(indexInfoS.getServiceTag())) {
            indexInfo.setServiceTag(indexInfoS.getServiceTag());
        }
        indexInfo.setUpdateTime(time);
        return indexInfo;
    }
}
