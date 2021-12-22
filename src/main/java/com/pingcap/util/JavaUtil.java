package com.pingcap.util;

import java.lang.management.ManagementFactory;

public class JavaUtil {

	public JavaUtil() {
	}

	public static boolean hasClass(String className) {
		boolean hasAvailable = false;
		try {
			hasAvailable = null != Class.forName(className);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (Throwable e){
			e.printStackTrace();
		}
		return hasAvailable;
	}
	
	/**
	*
	* @param fullClassName：注意：这里的fullClassName，必须是包含类所在包名的全限定名，
	* @param args：这里是传入的类的构造函数的参数
	* @return
	*/
	@SuppressWarnings("rawtypes")
	public static Object newClazz(String fullClassName, Object... args) {
		Class[] classes = new Class[args.length];
		for (int i = 0; i < classes.length; i++) {
			classes[i] = args[i].getClass();
		}
		Object clazz = null;
		try {
			clazz = Class.forName(fullClassName).getConstructor(classes).newInstance(args);
		}
		catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		return clazz;
	}
	
	public static String getPid(){
		// get name representing the running Java virtual machine.  
		String name = ManagementFactory.getRuntimeMXBean().getName();  
		if(null == name)
			return "";
		// get pid  
		return name.split("@")[0]; 
	}
}
