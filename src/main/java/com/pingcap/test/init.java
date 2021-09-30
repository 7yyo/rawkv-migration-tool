package com.pingcap.test;

import com.pingcap.util.FileUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class init {

    static String json = "{\"id\":\"%s\",\"type\":\"1234567890\",\"envid\":\"pf01\",\"appid\":\"ADP012bfe33b08b\",\"createtime\":\"2020-11-04T17:12:04Z\",\"servicetag\":\"{\\\"ACCT_DTL_TYPE\\\":\\\"SP0001\\\",\\\"PD_SALE_FTA_CD\\\":\\\"99\\\",\\\"AR_ID\\\":\\\"\\\",\\\"CMTRST_CST_ACCNO\\\":\\\"\\\",\\\"QCRCRD_IND\\\":\\\" \\\",\\\"BLKMDL_ID\\\":\\\"52\\\",\\\"CORPPRVT_FLAG\\\":\\\"1\\\"}\",\"targetid\":\"0037277\",\"updatetime\":\"2020-11-04T17:12:04Z\"}";

    public static void main(String[] args) throws IOException {
        File file = FileUtil.createFile("src/main/resources/testFile/indexInfo_json/indexInfo2.json");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        FileChannel fileChannel = fileOutputStream.getChannel();
        for (int i = 1000; i <= 1989; i++) {
            ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(String.format(json, i) + "\n");
            try {
                fileChannel.write(byteBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        fileChannel.close();
        fileOutputStream.close();
    }

}
