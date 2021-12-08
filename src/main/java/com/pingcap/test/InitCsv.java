package com.pingcap.test;

import com.pingcap.util.FileUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class InitCsv {

    /**
     * abc...xyz
     * 123...890
     *
     *  #
     *  ()
     *  \
     *  _
     *  &
     *  @
     */
    static String csv1 = "%s|139Type|targetId";
    static String csv2 = "%s|type|#()##BLKMDL_ID";
    static String csv3 = "%s|type|\\_##BLKMDL_ID##&@##2@@@##CORPPRVT_FLAG##CMTRST_CST_ACCNO##AR_ID##QCRCRD_IND";

    public static void main(String[] args) throws IOException {
        File file = FileUtil.createFile("src/main/resources/testFile/indexInfo_csv/indexInfo10.json");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        FileChannel fileChannel = fileOutputStream.getChannel();
        int num = 90000001;
        for (int i = 1; i <= 10000000; i++) {
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
