package com.pingcap.test.testutil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * IndexInfoJson.nums= 100000000
 * IndexInfoJson.eachSlices= 10000000
 * IndexInfoJson.wrongData= 5000000
 * IndexInfoJson.jumpType= 5000000
 * IndexInfoJson.filePathWithOutSuffix= "/Users/weiwei/tmp/jianhang/out";
 */
public class IndexInfoJson {

    private static long nums;
    private static long eachSlices;
    private static int wrongData;
    private static int jumpType;
    private static String filePathWithOutSuffix;

    static {
        try {
            Properties properties = new Properties();
            InputStream input = IndexInfoJson.class.getClassLoader().getResourceAsStream("Test.properties");
            properties.load(input);
            nums = Long.valueOf(properties.getProperty("IndexInfoJson.nums", "1000000"));
            eachSlices = Long.valueOf(properties.getProperty("IndexInfoJson.eachSlices", "100000"));
            wrongData = Integer.parseInt(properties.getProperty("IndexInfoJson.wrongData", "10000"));
            jumpType = Integer.parseInt(properties.getProperty("IndexInfoJson.jumpType", "10000"));
            filePathWithOutSuffix = properties.getProperty("IndexInfoJson.filePathWithOutSuffix", "/import");

            System.out.printf("nums: %d \n eachSilices: %d \n wrongData: %d \n jumpType: %d \n filePathWithOutSuffix: %s \n ",
                    nums,eachSlices,wrongData,jumpType,filePathWithOutSuffix);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final String TYPE_A001 = "A001";
    private static final String TYPE_B001 = "B001";
    private static final String TYPE_C001 = "C001";

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

        private static Random r2 = new Random();

        private long startValue;

        private long endValue;

        public ParallelExecuteCreateDataTask(long startValue, long endValue) {
            this.startValue = startValue;
            this.endValue = endValue;
        }

        protected Long compute() {
            if (endValue - startValue < IndexInfoJson.eachSlices) {
                System.out.println(Thread.currentThread().getName() + " startValue {" + startValue + "} endValue {" + endValue + "} ");
                AtomicInteger atc_a = new AtomicInteger();
                AtomicInteger atc_b = new AtomicInteger();
                AtomicInteger atc_c = new AtomicInteger();
                AtomicInteger WRONG = new AtomicInteger();
                try {
                    String fileFullPath = filePathWithOutSuffix + fileId.getAndAdd(1) + ".txt";
                    File file = getByName(fileFullPath);
                    StringBuilder s = new StringBuilder();
                    long i = startValue;
                    for (; i <= endValue; i++) {
                        int w_d = r2.nextInt(wrongData);
                        if (w_d == 1) {
                            s.append("123 \n");
                            WRONG.addAndGet(1);
                        } else {
                            int v = r.nextInt(jumpType);
                            String type = "";
                            if (v == 1) {
                                atc_a.addAndGet(1);
                                type = TYPE_A001;
                            } else if (v == 2) {
                                atc_b.addAndGet(1);
                                type = TYPE_B001;
                            } else if (v == 3) {
                                atc_c.addAndGet(1);
                                type = TYPE_C001;
                            } else if (v < jumpType * (1 / 3)) {
                                type = TYPE_A001 + r2.nextInt(1000000);
                            } else if (v < jumpType * (2 / 3)) {
                                type = TYPE_B001 + r2.nextInt(1000000);
                            } else {
                                type = TYPE_C001 + r2.nextInt(1000000);
                            }

                            s.append("{\"id\":\"" +
                                    ("" + i) +
                                    "\"" +
                                    ",\"type\":\"" +
                                    type +
                                    "\"" +
                                    ",\"envid\":\"pf01\",\"appid\":\"ADP012bfe33b08b\",\"createtime\":\"" +
                                    "2020-11-04T17:12:04Z" +
                                    "\",\"servicetag\":\"{\\\"ACCT_DTL_TYPE\\\":\\\"SP0001\\\",\\\"PD_SALE_FTA_CD\\\":\\\"99\\\",\\\"AR_ID\\\":\\\"\\\",\\\"CMTRST_CST_ACCNO\\\":\\\"\\\",\\\"QCRCRD_IND\\\":\\\" \\\",\\\"BLKMDL_ID\\\":\\\"52\\\",\\\"CORPPRVT_FLAG\\\":\\\"1\\\"}\",\"targetid\":\"0037277\",\"updatetime\":\"" +
                                    "2020-11-04T17:12:04Z" +
                                    "\"} \n");
                        }
                        if (i % 10000 == 0) {
                            writeToFile(file, s.toString());
                            s = new StringBuilder();
                        }
                    }
                    writeToFile(file, s.toString());

                    sb.append("(" + fileFullPath + ") create test data end WRONG{" + WRONG + "}  A001{" + atc_a.get() + "} B001{" + atc_b.get() + "}  C001{" + atc_c.get() + "} \n");

                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(" create test data parallelExecuteCreateDataTask exception {} " + e);
                    throw e;
                }
                return startValue;
            }
            ParallelExecuteCreateDataTask subTask1 = new ParallelExecuteCreateDataTask(startValue, (startValue + IndexInfoJson.eachSlices - 1));
            subTask1.fork();
            ParallelExecuteCreateDataTask subTask2 = new ParallelExecuteCreateDataTask((startValue + IndexInfoJson.eachSlices), endValue);
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
