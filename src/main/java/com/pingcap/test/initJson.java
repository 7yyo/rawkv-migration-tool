package com.pingcap.test;

import com.pingcap.util.FileUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class initJson {

    static String json = "{\"id\":\"%s\",\"type\":\"type\",\"envid\":\"envid\",\"appid\":\"appid\",\"createtime\":\"2021-11-11 00:00:00\",\"servicetag\":\"{\\\"ACCT_DTL_TYPE\\\":\\\"ACCT_DTL_TYPE\\\",\\\"PD_SALE_FTA_CD\\\":\\\"PD_SALE_FTA_CD\\\",\\\"AR_ID\\\":\\\"AR_  ID\\\",\\\"CMTRST_CST_ACCNO\\\":\\\"CMTRST_CST_ACCNO\\\",\\\"QCRCRD_IND\\\":\\\" QCRC RD_IND \\\",\\\"BLKMDL_ID\\\":\\\"BLKMDL_ID\\\",\\\"CORPPRVT_FLAG\\\":\\\"CORPPRVT_FLAG\\\"}\",\"targetid\":\"targetid\",\"updatetime\":\"2021-12-12T00:00:00Z\"}";

    public static void main(String[] args) throws IOException {
        File file = FileUtil.createFile("src/main/resources/testFile/indexInfo_json/indexInfo2.json");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        FileChannel fileChannel = fileOutputStream.getChannel();
        for (int i = 1001; i <= 2000; i++) {
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
