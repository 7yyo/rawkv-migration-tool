package com.pingcap.util;

import com.pingcap.enums.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * @author yuyang
 */
public class FileUtil {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    private static final List<File> LIST = new ArrayList<>();
    private static final List<File> CHECK_SUM_LIST = new ArrayList<>();

    public static List<File> showFileList(String filePath, boolean isCheckSum) {
        List<File> fileList = FileUtil.loadDirectory(new File(filePath), isCheckSum);
        if (fileList == null) {
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
                if (isCheckSum) {
                    CHECK_SUM_LIST.add(file);
                } else {
                    LIST.add(file);
                }
            }
        }
        for (File file : insideFiles) {
            loadDirectory(file, isCheckSum);
        }
        if (isCheckSum) {
            return CHECK_SUM_LIST;
        } else {
            return LIST;
        }
    }

    public static int getFileLines(File file) {
        int lines = 0;
        try {
            FileReader in = new FileReader(file);
            LineNumberReader reader = new LineNumberReader(in);
            long n = reader.skip(Long.MAX_VALUE);
            logger.debug(String.format("Skip line = [%s]", n));
            lines = reader.getLineNumber();
            if (!isLinux()) {
                lines++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }

    public static HashMap<String, Long> getTtlTypeMap(List<String> list) {
        HashMap<String, Long> ttlTypeCountMap = new HashMap<>(16);
        for (String ttlType : list) {
            ttlTypeCountMap.put(ttlType, 0L);
        }
        return ttlTypeCountMap;
    }

    public static void deleteFolder(String filePath) {
        File deleteFilePath = new File(filePath);
        File[] files = deleteFilePath.listFiles();
        if (files == null) {
            logger.warn("This folder [checkSum/batchPutErr] no need to delete!");
            return;
        }
        for (File file : files) {
            if (!file.isDirectory()) {
                if (!file.delete()) {
                    logger.error(String.format("Delete folder [%s] failed.", file.getAbsolutePath()));
                }
            } else {
                deleteFolder(file.getAbsolutePath());
            }
        }
        if (!deleteFilePath.delete()) {
            logger.error(String.format("Delete folder [%s] failed.", deleteFilePath.getAbsolutePath()));
        }
    }

    public static void createFolder(String filePath) {
        File checkSumFolder = new File(filePath);
        if (!checkSumFolder.mkdir()) {
            logger.error(String.format("Failed to mkdir check sum file, folder path = [%s]", filePath));
        }
    }

    public static boolean isLinux() {
        return System.getProperty("os.name").toLowerCase().contains("linux");
    }

}
