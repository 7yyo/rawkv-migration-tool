package com.pingcap.util;

import com.pingcap.importer.IndexInfoS2T;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class CountUtil {

    private static final Logger logger = Logger.getLogger(IndexInfoS2T.class);
    private static final String totalLog = " The total number of lines in the '%s' is %s, and each thread processes %s, and the remainder is %s";
    private static final String threadLog = " Thread-%s Processing '%s' from line %s, need to process %s.";

    public static List<String> getPerThreadFileLines(long line, int threadNum, String fileName) {
        // Average number of items processed by each thread
        long avg = line / threadNum;
        long remainder = line % threadNum;
        logger.info(String.format(totalLog, fileName, line, avg, remainder));
        List<String> list = new ArrayList<>();
        long startIndex; // The index at which each thread started processing.
        long todo; // How many.
        for (int i = 0; i < threadNum; i++) {
            startIndex = i * avg;
            todo = avg;
            logger.info(String.format(threadLog, i, fileName, startIndex, todo));
            list.add(startIndex + "," + todo);
        }
        Integer dangStart = Integer.parseInt(list.get(list.size() - 1).split(",")[0]);
        Integer dangTodo = Integer.parseInt(list.get(list.size() - 1).split(",")[1]);
        list.add((dangStart + dangTodo) + "," + remainder);
        logger.debug(String.format(threadLog, list.size(), fileName, dangStart + dangTodo, remainder));
        return list;
    }

}
