package com.pingcap.test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Demo {

    public static void main(String[] args) throws IOException, InterruptedException {

        Thread thread1 = new Thread(new job(0, 3));
        Thread thread2 = new Thread(new job(3, 11));
        thread1.start();
        Thread.sleep(2000);
        thread2.start();

    }

}

class job implements Runnable {

    private int start;
    private int todo;

    public job(int start, int todo) {
        this.start = start;
        this.todo = todo;
    }

    @Override
    public void run() {
        File file = new File("/Users/yuyang/import/import.txt");
        BufferedInputStream bufferedInputStream = null;
        BufferedReader bufferedReader = null;
        String line = null;
        try {
            bufferedInputStream = new BufferedInputStream(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        if (bufferedInputStream != null) {
            bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream, StandardCharsets.UTF_8));
        }

        int count = 0;
        int totalCount = 0;
        for (int m = 0; m < start; m++) {
            try {
                bufferedReader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        for (int n = 0; n < todo; n++) {
            try {
                System.out.println(Thread.currentThread().getId() + "===" + bufferedReader.readLine());
                count++;
                totalCount++;
                if (totalCount == todo || count == 2) {
                    System.out.println("batch put");
                    count = 0;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

