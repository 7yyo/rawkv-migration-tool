package com.pingcap.pojo;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;

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
        boolean serviceTagC = true;
        if ((this.serviceTag != null && !"".equals(this.serviceTag)) && (indexInfo.getServiceTag() != null && !"".equals(indexInfo.getServiceTag()))) {
            serviceTagC = this.serviceTag.equals(indexInfo.getServiceTag());
        }
        boolean targetIdC = this.targetId.equals(indexInfo.getTargetId());
        boolean typeC = this.type.equals(indexInfo.getType());
        return idC && serviceTagC && targetIdC && typeC;
    }

    public static IndexInfo initIndexInfo(String originalLine, String delimiter_1, String delimiter_2) {
        IndexInfo indexInfo = new IndexInfo();

        String id = originalLine.split(delimiter_1)[0];
        indexInfo.setId(id);
        String type = originalLine.split(delimiter_1)[1];
        indexInfo.setType(type);

        String targetId = originalLine.split(delimiter_1)[2].split(delimiter_2)[0];

        if (originalLine.split(delimiter_1).length > 3) {
            String v = originalLine.split(delimiter_1)[2];
            ServiceTag serviceTag = new ServiceTag();
            serviceTag.setBLKMDL_ID(v.split(delimiter_2)[0]);
            serviceTag.setPD_SALE_FTA_CD(v.split(delimiter_2)[1]);
            serviceTag.setACCT_DTL_TYPE(v.split(delimiter_2)[2]);
            serviceTag.setCORPPRVT_FLAG(v.split(delimiter_2)[3]);
            serviceTag.setCMTRST_CST_ACCNO(v.split(delimiter_2)[4]);
            serviceTag.setAR_ID(v.split(delimiter_2)[5]);
            serviceTag.setQCRCRD_IND(v.split(delimiter_2)[6]);
            indexInfo.setServiceTag(JSON.toJSONString(serviceTag));

        }

        indexInfo.setTargetId(targetId);

        return indexInfo;
    }

    public static IndexInfo initIndexInfoT(IndexInfo indexInfoS, String time) {
        IndexInfo indexInfo = new IndexInfo();
        // value
        // appId、serviceTag、targetId、updateTime
        indexInfo.setAppId(indexInfo.getAppId());
        if (indexInfoS.getServiceTag() == null) {
            indexInfo.setServiceTag(null);
        } else if ("".equals(indexInfoS.getServiceTag())) {
            indexInfo.setServiceTag("");
        } else {
            indexInfo.setServiceTag(indexInfoS.getServiceTag());
        }
        indexInfo.setTargetId(indexInfoS.getTargetId());
        indexInfo.setUpdateTime(time);
        return indexInfo;
    }
}
