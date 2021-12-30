package com.pingcap.test;

import com.pingcap.enums.Model;
import com.pingcap.util.FileUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import org.apache.commons.lang.StringUtils;

public class InitJson_TempIndexInfo {

    static String json = "{\"id\":\"%s\",\"envid\":\"envid\",\"appid\":\"appid\",\"targetid\":\"targetid\"}";

    public static void main(String[] args) throws IOException {
    	File file;
    	if (!StringUtils.isEmpty(System.getProperty(Model.P))){
    		file = FileUtil.createFile(System.getProperty(Model.P));
    	}
    	else{
    		file = FileUtil.createFile("src/main/resources/testFile/tempIndexInfo_json/tempIndexInfo.json");
    	}   
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        FileChannel fileChannel = fileOutputStream.getChannel();
        int num = 90000001;
     	if (!StringUtils.isEmpty(System.getProperty(Model.M))){
     		num = Integer.parseInt(System.getProperty(Model.M));
     	}
     	String headStr = "";
     	if (!StringUtils.isEmpty(System.getProperty(Model.K))){
     		headStr = System.getProperty(Model.K);
     	}
        for (int i = 1; i <= num; i++) {
            ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(String.format(json, headStr+i) + "\n");
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
