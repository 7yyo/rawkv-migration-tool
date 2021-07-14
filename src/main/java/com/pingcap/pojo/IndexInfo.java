package com.pingcap.pojo;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;

/**
 * @author yuyang
 */
public class IndexInfo {

    public static final String INDEX_INFO_KET_FORMAT = "indexInfo_:_%s_:_%s_:_%s";

    private String envId;
    private String type;
    private String id;
    private String appId;
    private String serviceTag;
    private String targetId;
    private String createTime;
    private String updateTime;

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


    /**
     * Compare indexInfo
     * Comparison strategy: id, serviceTag, targetId, type
     *
     * @param indexInfo: Object indexInfo
     * @return boolean
     */
    public boolean equals(IndexInfo indexInfo) {
        boolean idC = this.id.equals(indexInfo.getId());
        boolean serviceTagC = true;
        if (StringUtils.isNotBlank(this.serviceTag) && StringUtils.isNotBlank(indexInfo.getServiceTag())) {
            serviceTagC = this.serviceTag.equals(indexInfo.getServiceTag());
        }
        boolean targetIdC = this.targetId.equals(indexInfo.getTargetId());
        boolean typeC = this.type.equals(indexInfo.getType());
        return idC && serviceTagC && targetIdC && typeC;
    }

    public static void key2IndexInfo(IndexInfo indexInfo, String key, String keyDelimiter) {
        indexInfo.setEnvId(key.split(keyDelimiter)[1]);
        indexInfo.setType(key.split(keyDelimiter)[2]);
        indexInfo.setId(key.split(keyDelimiter)[3]);
    }

    /**
     * CSV to indexInfo
     *
     * @param originalLine: Original file line
     * @param delimiter1:   First delimiter
     * @param delimiter2:   Second separator
     * @return indexInfo
     */
    public static IndexInfo csv2IndexInfo(IndexInfo indexInfo, String originalLine, String delimiter1, String delimiter2) {

        String id = originalLine.split(delimiter1)[0];
        indexInfo.setId(id);
        String type = originalLine.split(delimiter1)[1];
        indexInfo.setType(type);

        String targetId = originalLine.split(delimiter1)[2].split(delimiter2)[0];
        indexInfo.setTargetId(targetId);

        // Means 1|2|3##4##5.....
        if (originalLine.split(delimiter1).length > 3) {
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
        return indexInfo;
    }

    public static void initIndexInfoT(IndexInfo indexInfoT, IndexInfo indexInfoS) {
        // value
        // appId、serviceTag、targetId、updateTime
        indexInfoT.setAppId(indexInfoT.getAppId());
        if (indexInfoS.getServiceTag() == null) {
            indexInfoT.setServiceTag(null);
        } else if ("".equals(indexInfoS.getServiceTag())) {
            indexInfoT.setServiceTag("");
        } else {
            indexInfoT.setServiceTag(indexInfoS.getServiceTag());
        }
        indexInfoT.setTargetId(indexInfoS.getTargetId());
        indexInfoT.setUpdateTime(indexInfoS.getUpdateTime());
    }
}
