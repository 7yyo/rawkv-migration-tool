package com.pingcap.pojo;

import org.apache.commons.lang.StringUtils;

public class IndexInfo implements InfoInterface{

    // indexInfo_:_{envid}_:_{type}_:_{id}
    public static final String KET_FORMAT = "indexInfo%s%s%s%s%s%s";
    //private static final PascalNameFilter nameFilter = new PascalNameFilter();

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
        boolean serviceTagC;
        boolean targetIdC;
        // "" != null
        if (("".equals(this.serviceTag) && "".equals(indexInfo.getServiceTag())) || (this.serviceTag == null && indexInfo.getServiceTag() == null)) {
            serviceTagC = true;
        } else if (!StringUtils.isEmpty(this.serviceTag) && !StringUtils.isEmpty(indexInfo.getServiceTag())) {
            serviceTagC = this.serviceTag.equals(indexInfo.getServiceTag());
        } else {
            // This means that one is empty and the other is not empty, return false
            return false;
        }
        if (("".equals(this.targetId) && "".equals(indexInfo.getTargetId())) || (this.targetId == null && indexInfo.getTargetId() == null)) {
            targetIdC = true;
        } else if (!StringUtils.isEmpty(this.targetId) && !StringUtils.isEmpty(indexInfo.getTargetId())) {
            targetIdC = this.targetId.equals(indexInfo.getTargetId());
        } else {
            // This means that one is empty and the other is not empty, return false
            return false;
        }
        return serviceTagC && targetIdC;
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
/*    public static void csv2IndexInfo(IndexInfo indexInfo, String originalLine, String delimiter1, String delimiter2) {
        String id = originalLine.split(delimiter1)[0];
        String type = originalLine.split(delimiter1)[1];
        indexInfo.setId(id);
        indexInfo.setType(type);
        String targetId = originalLine.split(delimiter1)[2].split(delimiter2)[0];
        indexInfo.setTargetId(targetId);
        String v = originalLine.split(delimiter1)[2];

        // except <id|type|targetId>
        if (v.split(delimiter2).length > 1) {
            ServiceTag serviceTag = new ServiceTag();
            String[] vs = v.split(delimiter2);
            if (vs.length == 2) {
                // id|type|targetId##BLKMDL_ID
                serviceTag.setBLKMDL_ID(vs[1]);
            } else {
                // id|type|targetId##BLKMDL_ID##PD_SALE_FTA_CD##ACCT_DTL_TYPE##CORPPRVT_FLAG##CMTRST_CST_ACCNO##AR_ID##QCRCRD_IND
                serviceTag.setBLKMDL_ID(v.split(delimiter2)[1]);
                serviceTag.setPD_SALE_FTA_CD(v.split(delimiter2)[2]);
                serviceTag.setACCT_DTL_TYPE(v.split(delimiter2)[3]);
                serviceTag.setCORPPRVT_FLAG(v.split(delimiter2)[4]);
                serviceTag.setCMTRST_CST_ACCNO(v.split(delimiter2)[5]);
                serviceTag.setAR_ID(v.split(delimiter2)[6]);
                serviceTag.setQCRCRD_IND(v.split(delimiter2)[7]);
            }
            indexInfo.setServiceTag(JSON.toJSONString(serviceTag, nameFilter));
        }
    }*/

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
        if (indexInfoCassandra.getOpType() != null) {
            indexInfoTiKV.setOpType(indexInfoCassandra.getOpType());
        }
        if (indexInfoCassandra.getDuration() != null) {
            indexInfoTiKV.setDuration(indexInfoCassandra.getDuration());
        }
    }

	@Override
	public boolean equalsValue(Object indexInfo) {
		return equals((IndexInfo)indexInfo);
	}

}
