package com.pingcap.util;

import com.pingcap.importer.IndexInfoS2T_bak;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class FileUtil {

    private static final Logger logger = Logger.getLogger(FileUtil.class);

    private static final List<File> list = new ArrayList<>();

    public static List<File> loadDirectory(File fileList) {
        File[] files = fileList.listFiles();
        if (files == null) {
            logger.error("Files is not exists!");
            return null;
        }
        List<File> insideFiles = new ArrayList<>();
        for (File file : files) {
            if (file.isDirectory()) {
                insideFiles.add(file);
            } else {
                list.add(file);
            }
        }
        for (File file : insideFiles) {
            loadDirectory(file);
        }
        return list;
    }

    public static int getFileLines(File file) {
        FileReader in = null;
        int lines = 0;
        try {
            in = new FileReader(file);
            LineNumberReader reader = new LineNumberReader(in);
            reader.skip(Long.MAX_VALUE);
            lines = reader.getLineNumber() + 1;
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }

    public static HashMap<String, Long> getTtlTypeMap(List<String> list) {
        HashMap<String, Long> ttlTypeCountMap = new HashMap<>();
        for (String ttlType : list) {
            ttlTypeCountMap.put(ttlType, 0L);
        }
        return ttlTypeCountMap;
    }

}
