package com.pingcap.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author yuyang
 */
public class KvTestData {

    private static final int WRITE_NUM = 1000000;

    public static void main(String[] args) throws IOException {
        String filePath = "/Users/yuyang/IdeaProjects/tikv_importer/src/main/resources/testFile/indexInfo_json/data.txt";
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath));
        for (int i = 1; i < WRITE_NUM + 1; i++) {
            bufferedWriter.write("{\"id\":\"" +
                    ("" + i) +
                    "\"" +
                    ",\"type\":\"" +
                    "typeYA!" +
                    "\"" +
                    ",\"envid\":\"pf01\",\"appid\":\"ADP012bfe33b08b\",\"createtime\":\"" +
                    "2020-11-04T17:12:04Z" +
                    "\",\"servicetag\":\"{\\\"ACCT_DTL_TYPE\\\":\\\"SP0001\\\",\\\"PD_SALE_FTA_CD\\\":\\\"99\\\",\\\"AR_ID\\\":\\\"\\\",\\\"CMTRST_CST_ACCNO\\\":\\\"\\\",\\\"QCRCRD_IND\\\":\\\" \\\",\\\"BLKMDL_ID\\\":\\\"52\\\",\\\"CORPPRVT_FLAG\\\":\\\"1\\\"}\",\"targetid\":\"0037277\",\"updatetime\":\"" +
                    "2020-11-04T17:12:04Z" +
                    "\"} \n");
        }
        bufferedWriter.flush();
        bufferedWriter.close();
    }

}
