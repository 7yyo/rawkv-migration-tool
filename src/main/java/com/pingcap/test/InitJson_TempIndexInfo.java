package com.pingcap.test;

import com.pingcap.util.FileUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class InitJson_TempIndexInfo {

    static String json = "{\"id\":\"%s\",\"envid\":\"envid\",\"appid\":\"appid\",\"targetid\":\"targetid\"}";

    public static void main(String[] args) throws IOException {
        File file = FileUtil.createFile("src/main/resources/testFile/tempIndexInfo_json/tempIndexInfo.json");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        FileChannel fileChannel = fileOutputStream.getChannel();
        for (int i = 1; i <= 2000; i++) {
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
