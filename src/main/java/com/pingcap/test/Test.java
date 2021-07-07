package com.pingcap.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.pingcap.enums.Model;
import com.pingcap.util.PropertiesUtil;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class Test {



    private static final String JSON_STRING = "{\"id\":\"1000000\",\"type\":\"typeYA!\",\"envid\":\"pf01\",\"appid\":\"ADP012bfe33b08b\",\"createtime\":\"2020-11-04T17:12:04Z\",\"servicetag\":\"{\\\"ACCT_DTL_TYPE\\\":\\\"SP0001\\\",\\\"PD_SALE_FTA_CD\\\":\\\"99\\\",\\\"AR_ID\\\":\\\"\\\",\\\"CMTRST_CST_ACCNO\\\":\\\"\\\",\\\"QCRCRD_IND\\\":\\\" \\\",\\\"BLKMDL_ID\\\":\\\"52\\\",\\\"CORPPRVT_FLAG\\\":\\\"1\\\"}\",\"targetid\":\"0037277\",\"updatetime\":\"2020-11-04T17:12:04Z\"} \n";
    private static final String PERSONAL_PROPERTIES_PATH = "src/main/resources/importer.properties";

    public static void main(String[] args) {

        String propertiesPath = System.getProperty(Model.P) == null ? PERSONAL_PROPERTIES_PATH : System.getProperty(Model.P);
        Properties properties = PropertiesUtil.getProperties(propertiesPath);

        String table = properties.getProperty("importer.table");
        String keyDelimiter = properties.getProperty(Model.KEY_DELIMITER);

        JSONObject jsonObject = JSONObject.parseObject(JSON_STRING);

        String[] bean = properties.getProperty("importer.bean").split(",");
        HashMap<String, String> beanMap = new HashMap<>(16);
        for (String b : bean) {
            beanMap.put(b.toLowerCase(Locale.ROOT), b);
        }

        String[] keys = properties.getProperty("importer.key").split(",");
        StringBuilder k = new StringBuilder(table);
        for (String key : keys) {
            k.append(keyDelimiter).append(jsonObject.get(key).toString());
        }

        String[] values = properties.getProperty("importer.value").split(",");
        Map<String, String> valueMap = new HashMap<>(16);
        for (String value : values) {
            if (value.contains("time") || value.contains("Time")) {
                valueMap.put(beanMap.get(value), jsonObject.get(value).toString().replaceAll("T", " ").replaceAll("Z", ""));
            } else {
                valueMap.put(beanMap.get(value), jsonObject.get(value).toString());
            }
        }

        System.out.println("k == " + k);
        System.out.println("v == " + JSON.toJSONString(valueMap));

    }

}
