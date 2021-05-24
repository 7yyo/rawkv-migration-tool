package com.pingcap.util;

import com.pingcap.enums.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class FileUtil {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    private static final List<File> list = new ArrayList<>();
    private static final List<File> checkSumList = new ArrayList<>();

    public static List<File> showFileList(String filePath, boolean isCheckSum, Properties properties) {
        if (!isCheckSum) {
            logger.info("Welcome to To_TiKV.");
            logger.info(String.format("Properties=%s", properties));
        }
        List<File> fileList = FileUtil.loadDirectory(new File(filePath), isCheckSum);
        if (fileList.isEmpty()) {
            logger.warn(String.format("There are no files in this path [%s]", filePath));
            return null;
        } else {
            for (int i = 0; i < fileList.size(); i++) {
                logger.info(String.format("No.%s-[%s]", i + 1, fileList.get(i).getAbsolutePath()));
            }
        }
        logger.info(String.format("Need to process the above files, total=[%s]", fileList.size()));
        return fileList;
    }

    public static List<File> loadDirectory(File fileList, boolean isCheckSum) {
        File[] files = fileList.listFiles();
        if (files == null) {
            logger.error("There is no file in this file path!");
            return null;
        }
        List<File> insideFiles = new ArrayList<>();
        for (File file : files) {
            if (file.isDirectory()) {
                insideFiles.add(file);
            } else {
                if (!file.getAbsolutePath().contains("DS") && !file.getAbsolutePath().contains("import.txt.swp")) {
                    if (isCheckSum) {
                        checkSumList.add(file);
                    } else {
                        list.add(file);
                    }
                }
            }
        }
        for (File file : insideFiles) {
            loadDirectory(file, isCheckSum);
        }
        if (isCheckSum) {
            return checkSumList;
        } else {
            return list;
        }
    }

    public static int getFileLines(File file) {
        FileReader in;
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

    public static ConcurrentHashMap<String, Long> getTtlTypeMap(List<String> list) {
        ConcurrentHashMap<String, Long> ttlTypeCountMap = new ConcurrentHashMap<>();
        for (String ttlType : list) {
            ttlTypeCountMap.put(ttlType, 0L);
        }
        return ttlTypeCountMap;
    }

    public static void deleteFolder(String checkSumFilePath) {
        File deleteFilePath = new File(checkSumFilePath);
        File[] files = deleteFilePath.listFiles();
        if (files == null) {
            logger.error("Files is not exists!");
            return;
        }
        for (File file : files) {
            if (!file.isDirectory()) {
                file.delete();
            } else {
                deleteFolder(file.getAbsolutePath());
            }
        }
    }

    public static void deleteFolders(String checkSumFilePath) {
        File deleteFilePath = new File(checkSumFilePath);
        File[] files = deleteFilePath.listFiles();
        if (files == null) {
            logger.error("Files is not exists!");
            return;
        }
        for (File file : files) {
            file.delete();
        }
    }

}
