package com.pingcap.util;

import com.pingcap.importer.IndexInfo2T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CountUtil {

    private static final Logger logger = LoggerFactory.getLogger("logBackLog");

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
            if (i == threadNum - 1) {
                todo = todo + remainder;
                list.add(startIndex + "," + todo);
            }else{
                list.add(startIndex + "," + todo);
            }
            logger.info(String.format(threadLog, i, fileName, startIndex, todo));
        }
//        list.add((dangStart + dangTodo) + "," + remainder);
//        logger.info(String.format(threadLog, list.size(), fileName, dangStart + dangTodo, remainder));
        return list;
    }

}
