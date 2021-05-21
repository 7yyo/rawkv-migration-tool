package com.pingcap.util.dataUtil;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TempIndexInfoJsonUtil {

    private static long nums;
    private static long eachSlices;
    private static int wrongData;
    private static String filePathWithOutSuffix;

    static {
        try {
            Properties properties = new Properties();
            InputStream input = TempIndexInfoJsonUtil.class.getClassLoader().getResourceAsStream("Test.properties");
            properties.load(input);
            nums = Long.valueOf(properties.getProperty("TempIndexInfoTest.nums", "1000000"));
            eachSlices = Long.valueOf(properties.getProperty("TempIndexInfoTest.eachSlices", "100000"));
            wrongData = Integer.parseInt(properties.getProperty("TempIndexInfoTest.wrongData", "10000"));
            filePathWithOutSuffix = properties.getProperty("TempIndexInfoTest.filePathWithOutSuffix", "/tempIndex");

            System.out.printf("nums: %d \n eachSilices: %d \n wrongData: %d \n filePathWithOutSuffix: %s \n ",
                    nums, eachSlices, wrongData, filePathWithOutSuffix);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static StringBuffer sb = new StringBuffer();

    private static AtomicInteger fileId = new AtomicInteger();

    public static void main(String[] args) {
        System.out.println("Create Test Data begin ");
        long now = System.currentTimeMillis();
        try {
            ForkJoinPool pool = new ForkJoinPool();
            ForkJoinTask<Long> task = pool.submit(new ParallelExecuteCreateDataTask(1, nums));
            task.get();
            pool.shutdown();
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (Exception e) {
            System.out.println("create test data error {} " + e);
        }
        System.out.println(" create test data end  size {" + nums + "} cost time {" + Long.valueOf(System.currentTimeMillis() - now) + "} ");
        System.out.println(sb.toString());
    }


    private static class ParallelExecuteCreateDataTask extends RecursiveTask<Long> {

        private static final long serialVersionUID = 1L;
        private static Random r = new Random();

        private long startValue;

        private long endValue;

        public ParallelExecuteCreateDataTask(long startValue, long endValue) {
            this.startValue = startValue;
            this.endValue = endValue;
        }

        protected Long compute() {
            if (endValue - startValue < TempIndexInfoJsonUtil.eachSlices) {
                System.out.println(Thread.currentThread().getName() + " startValue {" + startValue + "} endValue {" + endValue + "} ");
                AtomicInteger WRONG = new AtomicInteger();
                try {
                    String fileFullPath = filePathWithOutSuffix + fileId.getAndAdd(1) + ".txt";
                    File file = getByName(fileFullPath);
                    StringBuilder s = new StringBuilder();
                    long i = startValue;
                    for (; i <= endValue; i++) {
                        int w_d = r.nextInt(wrongData);
                        if (w_d == 1) {
                            s.append("tempIndexWrongData \n");
                            WRONG.addAndGet(1);
                        } else {
                            s.append("{\"id\":\"" +
                                    i +
                                    "\",\"envid\":\"pf01\",\"appid\":\"APD01\",\"targetid\":\"0037033%%ROOM1_APD01_pkax37!!dataSource0\"} \n");
                        }
                        if (i % 10000 == 0) {
                            writeToFile(file, s.toString());
                            s = new StringBuilder();
                        }
                    }
                    writeToFile(file, s.toString());

                    sb.append("(" + fileFullPath + ") create test data end WRONG{" + WRONG + "}  \n");

                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(" create test data parallelExecuteCreateDataTask exception {} " + e);
                    throw e;
                }
                return startValue;
            }
            ParallelExecuteCreateDataTask subTask1 = new ParallelExecuteCreateDataTask(startValue, (startValue + TempIndexInfoJsonUtil.eachSlices - 1));
            subTask1.fork();
            ParallelExecuteCreateDataTask subTask2 = new ParallelExecuteCreateDataTask((startValue + TempIndexInfoJsonUtil.eachSlices), endValue);
            subTask2.fork();
            return startValue;
        }
    }


    private synchronized static void writeToFile(File file, String str) {
        RandomAccessFile fout = null;
        FileChannel fcout = null;
        try {
            fout = new RandomAccessFile(file, "rw");
            long filelength = fout.length();//获取文件的长度
            fout.seek(filelength);//将文件的读写指针定位到文件的末尾
            fcout = fout.getChannel();//打开文件通道
            FileLock flout = null;
            while (true) {
                try {
                    flout = fcout.tryLock();//不断的请求锁，如果请求不到，等一秒再请求
                    break;
                } catch (Exception e) {
                    Thread.sleep(1000);
                }
            }
            fout.write(str.getBytes());//将需要写入的内容写入文件

            flout.release();
            fcout.close();
            fout.close();

        } catch (IOException e1) {
            e1.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {

            if (fcout != null) {

                try {
                    fcout.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    fcout = null;
                }
            }
            if (fout != null) {
                try {
                    fout.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    fout = null;
                }
            }
        }
    }

    private static File getByName(String path) {
        File file = new File(path);

        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new IllegalArgumentException("create file failed", e);
            }
        }

        if (file.isDirectory()) {
            throw new IllegalArgumentException("not a file");
        }

        return file;
    }

}
