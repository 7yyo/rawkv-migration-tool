package com.pingcap.test;

import com.pingcap.util.FileUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class InitCsv {

    static String csv1 = "%s|type|targetId";
    static String csv2 = "%s|type|targetId##BLKMDL_ID";
    static String csv3 = "%s|type|targetId##BLKMDL_ID##PD_SALE_FTA_CD##ACCT_DTL_TYPE##CORPPRVT_FLAG##CMTRST_CST_ACCNO##AR_ID##QCRCRD_IND";

    public static void main(String[] args) throws IOException {
        File file = FileUtil.createFile("src/main/resources/testFile/indexInfo_csv/indexInfo1.json");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        FileChannel fileChannel = fileOutputStream.getChannel();
        int num = 1;
        for (int i = 1; i <= 1000; i++) {
            ByteBuffer byteBuffer1 = StandardCharsets.UTF_8.encode(String.format(csv1, num++) + "\n");
            ByteBuffer byteBuffer2 = StandardCharsets.UTF_8.encode(String.format(csv2, num++) + "\n");
            ByteBuffer byteBuffer3 = StandardCharsets.UTF_8.encode(String.format(csv3, num++) + "\n");
            try {
                fileChannel.write(byteBuffer1);
                fileChannel.write(byteBuffer2);
                fileChannel.write(byteBuffer3);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        fileChannel.close();
        fileOutputStream.close();
    }

}
