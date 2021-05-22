package com.pingcap.importer;

public class Demo {

    static int[] fileLines = {2, 7, 13, 16, 19};
    static int[] file = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};


    static int lastFileLine = 0;
    static int fileLine = 0;

    public static void main(String[] args) {
        int sum = 0;
        for (int i = 0; i < fileLines.length; i++) {
            fileLine = fileLines[i];
            int n = fileLine - lastFileLine;
            for (int j = 0; j < n; j++) {
                System.out.println(file[sum++]);
            }
            lastFileLine = fileLine;
        }
    }
}
