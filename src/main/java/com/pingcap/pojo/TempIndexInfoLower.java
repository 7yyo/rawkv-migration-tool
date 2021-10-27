package com.pingcap.pojo;

public class TempIndexInfoLower {

    private String envid;
    private String id;
    private String appid;
    private String targetid;
    private String optype;
    private String duration;

    public String getEnvid() {
        return envid;
    }

    public void setEnvid(String envid) {
        this.envid = envid;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAppid() {
        return appid;
    }

    public void setAppid(String appid) {
        this.appid = appid;
    }

    public String getTargetid() {
        return targetid;
    }

    public void setTargetid(String targetid) {
        this.targetid = targetid;
    }

    public String getOptype() {
        return optype;
    }

    public void setOptype(String optype) {
        this.optype = optype;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }
}
