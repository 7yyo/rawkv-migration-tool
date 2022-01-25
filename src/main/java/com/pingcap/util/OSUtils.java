package com.pingcap.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Locale;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

public class OSUtils {
	private static final int CPUTIME = 5000;  
	private static final int PERCENT = 100;
	private static final int FAULTLENGTH = 10;

	public static String linuxVersion = null;
	
	static class Bytes { 
	    public static String substring(String src, int start_idx, int end_idx){ 
	        byte[] b = src.getBytes(); 
	        String tgt = ""; 
	        for(int i=start_idx; i<=end_idx; i++){ 
	            tgt +=(char)b[i]; 
	        }
	        b = null;
	        return tgt; 
	    } 
	}
	
	@SuppressWarnings("deprecation")
	private static synchronized void IintVersionInfo(){
	     FileInputStream is = null; 
	     InputStreamReader isr = null; 
	     BufferedReader brStat = null; 
		 try {
            is = new FileInputStream("/proc/version");
            isr = new InputStreamReader(is);
            brStat = new BufferedReader(isr);
			String line = brStat.readLine();
			if(StringUtils.isBlank(line)){
				return;
			}
			line = line.toLowerCase(Locale.ENGLISH);
			line = line.replace("linux version ", "");
			String [] arr = line.split("\\.");
			if(arr.length > 1)
				linuxVersion = arr[0]+"."+arr[1];
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally{
			IOUtils.closeQuietly(is);
			IOUtils.closeQuietly(isr);
			IOUtils.closeQuietly(brStat);
		}
	}

	public static double getCPURatio(){
        double cpuRatio = 0;
        String osName = System.getProperty("os.name");
        if(null == osName)
        	osName = "";
        else
        	osName = osName.toLowerCase(Locale.ENGLISH);
        if (osName.startsWith("windows")) { 
            cpuRatio = getCpuRatioForWindows(); 
        } 
        else if(osName.contains("linux")) { 
        	cpuRatio = getCpuRateForLinux(); 
        }
        else if(osName.contains("mac")) {
        	cpuRatio = getCpuRateForMac();
        }
        return cpuRatio;
	}
	private static double getCpuRateForLinux(){ 
        InputStream is = null; 
        InputStreamReader isr = null; 
        BufferedReader brStat = null; 
        StringTokenizer tokenStat = null;
        Process process = null;

        try{
        	if(null == linuxVersion)
        		IintVersionInfo();
            process = Runtime.getRuntime().exec("top -b -n 1"); 
            is = process.getInputStream();
            isr = new InputStreamReader(is);
            brStat = new BufferedReader(isr);
            
            if(linuxVersion.equals("2.4")){ 
            	//did not test
                brStat.readLine(); 
                brStat.readLine(); 
                brStat.readLine(); 
                brStat.readLine(); 
                
                tokenStat = new StringTokenizer(brStat.readLine()); 
                tokenStat.nextToken(); 
                tokenStat.nextToken(); 
                String user = tokenStat.nextToken(); 
                tokenStat.nextToken(); 
                String system = tokenStat.nextToken(); 
                tokenStat.nextToken(); 
                String nice = tokenStat.nextToken(); 
                
                user = user.substring(0,user.indexOf("%")); 
                system = system.substring(0,system.indexOf("%")); 
                nice = nice.substring(0,nice.indexOf("%")); 
                
                float userUsage = new Float(user).floatValue(); 
                float systemUsage = new Float(system).floatValue(); 
                float niceUsage = new Float(nice).floatValue(); 
                
                return (userUsage+systemUsage+niceUsage)/100; 
            }else{ 
                brStat.readLine(); 
                brStat.readLine(); 
                    
                tokenStat = new StringTokenizer(brStat.readLine()); 
                tokenStat.nextToken(); 
                tokenStat.nextToken(); 
                tokenStat.nextToken();
                tokenStat.nextToken();
                //get %id,redhat 
                String cpuUsage = abnormalTextToInt(tokenStat.nextToken(), "0");
                if(StringUtils.isBlank(cpuUsage)){
                	//centos
                	tokenStat.nextToken(); 
                	tokenStat.nextToken(); 
                	cpuUsage = abnormalTextToInt(tokenStat.nextToken(), "0");
                }

                Float usage = new Float(cpuUsage);
                return (1-usage.floatValue()/100);
            } 
        } catch(IOException ioe){
        	ioe.printStackTrace();
            freeResource(is, isr, brStat); 
            return 1; 
        } finally{
        	try {
				cleanInputStream(is);
			} catch (IOException e) {
			}
            freeResource(is, isr, brStat); 
            if(null != process){
            	try {
            		is = process.getErrorStream();
    				cleanInputStream(is);
    			} catch (IOException e1) {
    			}
            	finally{
            		IOUtils.closeQuietly(is);
            	}
            	process.destroy();
            }
        } 
    }
	
	private static double getCpuRateForMac(){ 
        InputStream is = null; 
        InputStreamReader isr = null; 
        BufferedReader brStat = null; 
        StringTokenizer tokenStat = null;
        Process process = null;
        final int checkCount = 3;

        try{
            process = Runtime.getRuntime().exec("top -l "+checkCount); 
            is = process.getInputStream();
            isr = new InputStreamReader(is);
            brStat = new BufferedReader(isr);

            //Skip the previous n times of information
            String cpuLine;
            int num = 1;
            while(true) {
            	cpuLine = brStat.readLine();
            	if(null == cpuLine)
            		break;
            	if(cpuLine.startsWith("CPU usage")) {
            		if(checkCount == num)
            			break;
            		else
            			++num;
            	}
            }
            if(StringUtils.isEmpty(cpuLine))
            	return 0;
            tokenStat = new StringTokenizer(cpuLine); 
            tokenStat.nextToken(); 
            tokenStat.nextToken(); 
            tokenStat.nextToken(); //user
            tokenStat.nextToken(); 
            tokenStat.nextToken(); //system
            tokenStat.nextToken(); 
            String idle = tokenStat.nextToken(); 
            idle = idle.substring(0,idle.indexOf("%")); 
                
            float idleUsage = new Float(idle).floatValue();
            return (1-idleUsage/100);
        } catch(IOException ioe){
        	ioe.printStackTrace();
            freeResource(is, isr, brStat); 
            return 1; 
        } finally{
        	try {
				cleanInputStream(is);
			} catch (IOException e) {
			}
            freeResource(is, isr, brStat); 
            if(null != process){
            	try {
            		is = process.getErrorStream();
    				cleanInputStream(is);
    			} catch (IOException e1) {
    			}
            	finally{
            		IOUtils.closeQuietly(is);
            	}
            	process.destroy();
            }
        } 
    }
	
	public static void cleanInputStream(InputStream is) throws IOException{
		if(null == is)
			return;
        byte buff[] = new byte[1024];
		while (-1 != is.read(buff)) {
		}
		buff = null;
	}
	
    //clean all not number characters
    public static String abnormalTextToInt(String text, String defaultValue) {
        try {
            return Pattern.compile("[^\\d.]").matcher(text).replaceAll("");
        } catch (Exception e) {
            return defaultValue;
        }
    }

    @SuppressWarnings("deprecation")
	private static void freeResource(InputStream is, InputStreamReader isr, BufferedReader br){
    	IOUtils.closeQuietly(is);
    	IOUtils.closeQuietly(isr);
    	IOUtils.closeQuietly(br);
    } 
    
    private static double getCpuRatioForWindows() {  
		try {  
			String procCmd = System.getenv("windir")  
			+ "\\system32\\wbem\\wmic.exe process get Caption,CommandLine,"  
			+ "KernelModeTime,ReadOperationCount,ThreadCount,UserModeTime,WriteOperationCount";  
			//get process information
			long[] c0 = readCpu(Runtime.getRuntime().exec(procCmd));  
			Thread.sleep(CPUTIME);  
			long[] c1 = readCpu(Runtime.getRuntime().exec(procCmd));  
			if (c0 != null && c1 != null) {  
				long idletime = c1[0] - c0[0];  
				long busytime = c1[1] - c0[1];  
				return Double.valueOf(  
				PERCENT * (busytime) / (busytime + idletime))  
					.doubleValue();  
			} else {  
				return 0.0;  
			}  
		} catch (Exception ex) {  
			ex.printStackTrace();  
			return 0.0;  
		}  
	} 
    
	@SuppressWarnings("deprecation")
	private static long[] readCpu(final Process proc) {  
		long[] retn = new long[2];
		InputStream is = null;
		InputStreamReader ir = null;
		LineNumberReader input = null;
		try {
			is = proc.getInputStream();
			proc.getOutputStream().close();  
			ir = new InputStreamReader(is);  
			input = new LineNumberReader(ir);  
			String line = input.readLine();  
			if (line == null || line.length() < FAULTLENGTH) {
				return null;  
			}  
			int capidx = line.indexOf("Caption");  
			int cmdidx = line.indexOf("CommandLine");  
			int rocidx = line.indexOf("ReadOperationCount");  
			int umtidx = line.indexOf("UserModeTime");  
			int kmtidx = line.indexOf("KernelModeTime");  
			int wocidx = line.indexOf("WriteOperationCount");  
			long idletime = 0;  
			long kneltime = 0;  
			long usertime = 0;  
			while ((line = input.readLine()) != null) {  
			if (line.length() < wocidx) {  
				continue;  
			}  
			// order：Caption,CommandLine,KernelModeTime,ReadOperationCount,  
			// ThreadCount,UserModeTime,WriteOperation  
			String caption = Bytes.substring(line, capidx, cmdidx - 1).trim();  
			String cmd = Bytes.substring(line, cmdidx, kmtidx - 1).trim();  
			if (cmd.indexOf("wmic.exe") >= 0) {  
				continue;  
			}  
			if (caption.equals("System Idle Process")  
				|| caption.equals("System")) {  
				idletime += Long.valueOf(  
				Bytes.substring(line, kmtidx, rocidx - 1).trim())  
				.longValue();  
				idletime += Long.valueOf(  
				Bytes.substring(line, umtidx, wocidx - 1).trim())  
				.longValue();  
				continue;  
			}  
			kneltime += Long.valueOf(  
				Bytes.substring(line, kmtidx, rocidx - 1).trim())  
					.longValue();  
			usertime += Long.valueOf(  
				Bytes.substring(line, umtidx, wocidx - 1).trim())  
					.longValue();
			}  
			retn[0] = idletime;  
			retn[1] = kneltime + usertime;  
			return retn;  
		} catch (Exception ex) {  
			ex.printStackTrace();  
		} finally {
        	try {
				cleanInputStream(is);
			} catch (IOException e) {
			}
            freeResource(is, ir, input); 
            if(null != proc){
	        	try {
	        		is = proc.getErrorStream();
					cleanInputStream(is);
				} catch (IOException e1) {
				}
	        	finally{
	        		IOUtils.closeQuietly(is);
	        	}
	        	proc.destroy();
            }
		}  
		return null;  
	}

}
