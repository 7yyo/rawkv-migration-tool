package com.pingcap.util;

import com.pingcap.enums.Model;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class FileUtil {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    public static List<File> showFileList(String filePath) {
        List<File> totalFileList = new ArrayList<>();
        List<File> fileList = loadDirectory(new File(filePath), totalFileList);
        if (fileList == null) {
            logger.warn("Path={} has no file.", filePath);
            System.exit(0);
        } else {
            for (int i = 0; i < fileList.size(); i++) {
                logger.info("No.{}={}", i + 1, fileList.get(i).getAbsolutePath());
            }
        }
        logger.info("Total={}", fileList.size());
        return fileList;
    }

    public static List<File> loadDirectory(File fileList, List<File> totalFileList) {
        File[] files = fileList.listFiles();
        if (files == null) {
            logger.error("There is no file in this path {}", fileList);
            return null;
        }
        Arrays.sort(files, new ComparerByLastModified());
        List<File> insideFilesList = new ArrayList<>();
        for (File file : files) {
            if (file.isDirectory()) {
                insideFilesList.add(file);
            } else {
                totalFileList.add(file);
            }
        }
        for (File file : insideFilesList) {
            loadDirectory(file, totalFileList);
        }
        return totalFileList;
    }

    public static int getFileLines(File file) {
        FileReader fileReader;
        int lines = 0;
        try {
            fileReader = new FileReader(file);
            LineNumberReader lineNumberReader = new LineNumberReader(fileReader);
            long characters = lineNumberReader.skip(Long.MAX_VALUE);
            logger.debug("Skip characters={}, file={}", characters, file);
            lines = lineNumberReader.getLineNumber();
            if (!isLinux()) {
                lines++;
            }
            lineNumberReader.close();
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

    public static synchronized void createFolder(String folderPath) {
        File checkSumFolder = new File(folderPath);
        if (checkSumFolder.exists()) {
            return;
        }
        if (!checkSumFolder.mkdir()) {
            logger.error("Failed to mkdir folder={}", folderPath);
        }
    }

    public static void deleteFolder(String folderPath) {
        File deleteFolder = new File(folderPath);
        File[] fileList = deleteFolder.listFiles();
        if (fileList == null) {
            return;
        }
        for (File file : fileList) {
            if (!file.isDirectory()) {
                if (!file.delete()) {
                    logger.error("Failed to delete file={}", file);
                }
            } else {
                deleteFolder(file.getAbsolutePath());
            }
        }
        if (!deleteFolder.delete()) {
            logger.error("Failed to delete folder={}", folderPath);
        }
    }

    public static File createFile(String filePath) {
        File file = new File(filePath);
        try {
            boolean result = file.createNewFile();
            logger.debug("Result=" + result);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return file;
    }

    public static boolean isLinux() {
        return System.getProperty("os.name").toLowerCase().contains("linux");
    }

    public static LineIterator createLineIterator(File file) {
        LineIterator lineIterator = null;
        try {
            lineIterator = FileUtils.lineIterator(file, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lineIterator;
    }

}

class ComparerByLastModified implements Comparator<File> {
    public int compare(File f1, File f2) {
        long diff = f1.lastModified() - f2.lastModified();
        if (diff > 0)
            return 1;
        else if (diff == 0)
            return 0;
        else
            return -1;
    }
}