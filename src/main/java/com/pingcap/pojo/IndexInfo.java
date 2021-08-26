package com.pingcap.pojo;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;

public class IndexInfo {

    public static final String KET_FORMAT = "indexInfo_:_%s_:_%s_:_%s";

    private String envId;
    private String type;
    private String id;
    private String appId;
    private String serviceTag;
    private String targetId;
    private String updateTime;
    private String opType;
    private String duration;

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

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public boolean equals(IndexInfo indexInfo) {
        boolean appIdC = this.appId.equals(indexInfo.getAppId());
        boolean serviceTagC = true;
        if (!StringUtils.isEmpty(this.serviceTag) && !StringUtils.isEmpty(indexInfo.getServiceTag())) {
            serviceTagC = this.serviceTag.equals(indexInfo.getServiceTag());
        }
        boolean targetIdC = this.targetId.equals(indexInfo.getTargetId());
//        boolean typeC = this.type.equals(indexInfo.getType());
        return appIdC && serviceTagC && targetIdC;
    }

    public static void key2IndexInfo(IndexInfo indexInfo, String key, String keyDelimiter) {
        indexInfo.setEnvId(key.split(keyDelimiter)[1]);
        indexInfo.setType(key.split(keyDelimiter)[2]);
        indexInfo.setId(key.split(keyDelimiter)[3]);
    }

    /**
     * CSV to indexInfo
     *
     * @param originalLine : Original file line
     * @param delimiter1   :   First delimiter
     * @param delimiter2   :   Second separator
     */
    public static void csv2IndexInfo(IndexInfo indexInfo, String originalLine, String delimiter1, String delimiter2) {

        String targetId = originalLine.split(delimiter1)[2].split(delimiter2)[0];
        indexInfo.setTargetId(targetId);
        // Means 1|2|3##4##5##6##7##8
        String v = originalLine.split(delimiter1)[2];
        ServiceTag serviceTag = new ServiceTag();
        serviceTag.setBLKMDL_ID(v.split(delimiter2)[0]);
        serviceTag.setPD_SALE_FTA_CD(v.split(delimiter2)[1]);
        serviceTag.setACCT_DTL_TYPE(v.split(delimiter2)[2]);
        serviceTag.setCORPPRVT_FLAG(v.split(delimiter2)[3]);
        serviceTag.setCMTRST_CST_ACCNO(v.split(delimiter2)[4]);
        serviceTag.setAR_ID(v.split(delimiter2)[5]);
        serviceTag.setQCRCRD_IND(v.split(delimiter2)[6]);
        indexInfo.setServiceTag(JSON.toJSONString(serviceTag));


    }

    /**
     * Value = appId, serviceTag, targetId, updateTime
     *
     * @param indexInfoTiKV      To tikv.
     * @param indexInfoCassandra From cassandra.
     */
    public static void initValueIndexInfoTiKV(IndexInfo indexInfoTiKV, IndexInfo indexInfoCassandra) {
        // appId
        indexInfoTiKV.setAppId(indexInfoCassandra.getAppId());
        // serviceTag
        if (indexInfoCassandra.getServiceTag() == null) {
            indexInfoTiKV.setServiceTag(null);
        } else {
            indexInfoTiKV.setServiceTag(indexInfoCassandra.getServiceTag());
        }
        // targetId
        indexInfoTiKV.setTargetId(indexInfoCassandra.getTargetId());
        // update time
        indexInfoTiKV.setUpdateTime(indexInfoCassandra.getUpdateTime());
    }

}
