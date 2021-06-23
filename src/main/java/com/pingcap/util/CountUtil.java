package com.pingcap.util;

import com.pingcap.enums.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yuyang
 */
public class CountUtil {

    private static final Logger logger = LoggerFactory.getLogger(Model.LOG);

    /**
     * @param line:      The total number of rows in the data file
     * @param threadNum: Total number of threads
     * @param fileName:  File name
     * @return For a single data file, the number of rows to be processed by each thread list
     */
    public static List<String> getPerThreadFileLines(long line, int threadNum, String fileName) {
        // Average number of items processed by each thread
        long avg = line / threadNum;
        long remainder = line % threadNum;
        logger.info(String.format("The total number of lines in the '%s'=[%s], and each thread processes=[%s], and the remainder=[%s]", fileName, line, avg, remainder));
        List<String> list = new ArrayList<>();
        long startIndex; // The index at which each thread started processing.
        long todo; // How many.
        for (int i = 0; i < threadNum; i++) {
            startIndex = i * avg;
            todo = avg;
            if (i == threadNum - 1) {
                todo = todo + remainder;
            }
            list.add(startIndex + "," + todo);
        }
        return list;
    }

    /**
     * Returns the percentage of two numbers
     *
     * @param n1: Molecular
     * @param n2: Denominator
     * @return percentage
     */
    public static String getPercentage(int n1, int n2) {
        java.text.NumberFormat numerator = java.text.NumberFormat.getInstance();
        numerator.setMaximumFractionDigits(2);
        return numerator.format(((float) n1 / (float) n2) * 100);
    }

}
