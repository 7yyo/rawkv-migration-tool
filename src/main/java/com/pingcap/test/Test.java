package com.pingcap.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pingcap.pojo.IndexInfoS;
import com.pingcap.util.FileUtil;
import com.pingcap.util.PropertiesUtil;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import shade.com.google.protobuf.ByteString;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

public class Test {

    private static final String PD_ADDRESS = "172.16.4.33:5555,172.16.4.34:5555,172.16.4.35:5555";
    private static final Properties properties = PropertiesUtil.getProperties();
    private static final String filePath = properties.getProperty("importer.in.importFilesPath_indexInfo");
    private static final String INDEX_INFO_KET_FORMAT = "indexInfo_:_%s_:_%s_:_%s";

    public static void main(String[] args) throws IOException {

        TiConfiguration conf = TiConfiguration.createRawDefault(PD_ADDRESS);
        TiSession session = TiSession.create(conf);
        RawKVClient rawKVClient = session.createRawClient();

        BufferedInputStream bufferedInputStream = null;
        BufferedReader bufferedReader = null;
        String line;
        JSONObject jsonObject;
        IndexInfoS indexInfoS;
        String indexInfoKey;


        List<File> fileList = FileUtil.showFileList(filePath);

        for (File file : fileList) {
            bufferedInputStream = new BufferedInputStream(new FileInputStream(file));
            bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream, StandardCharsets.UTF_8));
            while ((line = bufferedReader.readLine()) != null) {
                jsonObject = JSONObject.parseObject(line);
                indexInfoS = JSON.toJavaObject(jsonObject, IndexInfoS.class);
                indexInfoKey = String.format(INDEX_INFO_KET_FORMAT, "1", indexInfoS.getType(), indexInfoS.getId());
                String result = rawKVClient.get(ByteString.copyFromUtf8(indexInfoKey)).toStringUtf8();
                System.out.println(result);
            }
        }

    }

}
