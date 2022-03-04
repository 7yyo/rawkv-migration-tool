package com.pingcap.test;

import com.pingcap.enums.Model;
import com.pingcap.util.FileUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

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
    	File file;
    	if (!StringUtils.isEmpty(System.getProperty(Model.P))){
    		file = FileUtil.createFile(System.getProperty(Model.P));
    	}
    	else{
    		file = FileUtil.createFile("src/main/resources/testFile/indexInfo_csv/indexInfo10.csv");
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
    	boolean isUUID = false;
		if("uuid".equalsIgnoreCase(headStr))
			isUUID = true;
        for (int i = 1; i <= num; i++) {
    		if(isUUID)
    			headStr = i+getUUID();
            ByteBuffer byteBuffer1 = StandardCharsets.UTF_8.encode(String.format(headStr+csv1, i) + "\n");
            ByteBuffer byteBuffer2 = StandardCharsets.UTF_8.encode(String.format(headStr+csv2, i++) + "\n");
            ByteBuffer byteBuffer3 = StandardCharsets.UTF_8.encode(String.format(headStr+csv3, i++) + "\n");
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


    public static String getUUID() {
	    UUID uuid = UUID.randomUUID();
	    String str = uuid.toString();
	    return str.substring(0, 8) + str.substring(9, 13) + str.substring(14, 18) + str.substring(19, 23) + str.substring(24);
    }
}
