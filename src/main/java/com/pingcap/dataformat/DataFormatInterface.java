package com.pingcap.dataformat;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.tikv.shade.com.google.protobuf.ByteString;

import com.pingcap.pojo.InfoInterface;

public interface DataFormatInterface {
	public static final String DATA_LINENO = "LINENO";
	public static final String DATA_LINEDATA = "LINEDATA";
	public static final String SPLITERSET = "|_:@#$%^&*-+=<>;[]?";
	
	public interface DataFormatCallBack {
		public boolean putDataCallBack(String ttlType, ByteString key,ByteString value);
	}
	
	public interface UnDataFormatCallBack {
		public boolean getDataCallBack(String strJson, String type, int typeInt);
	}

	public boolean formatToKeyValue( String scenes, String line, DataFormatCallBack dataFormatCallBack) throws Exception;
	
	public InfoInterface packageToObject(String scenes, String key, String value, DataFormatCallBack dataFormatCallBack) throws Exception;
	
	public boolean unFormatToKeyValue(
			String scenes,
			String key,
			String value,
			UnDataFormatCallBack unDataFormatCallBack
			) throws Exception;
	
	// filter characters \\ in configuration
	public static String delimiterMatcher(String source) {
		if(source.startsWith("\\"))
			return source.substring(1);
		return source;
	}
	
	public static int findMatcher(String source,String find) {
		Pattern pattern = Pattern.compile(find);
		Matcher matcher = pattern.matcher(source);
		int count=0;
		while(matcher.find()){
			count++;
		}
		return count;
	}
	
	public static String getGuessKeySpliter(String source) {
		String strSpliter="";
		boolean isFind;
		char ch;
		for(int i=0;i<source.length();i++){
			ch = source.charAt(i);
			isFind = false;
			for(int j=0;j<SPLITERSET.length();j++){
				if(ch == SPLITERSET.charAt(j)){
					isFind = true;
					break;
				}
			}
			if(!isFind)
				break;
			else
				strSpliter += ch;
		}
		return strSpliter;
	}
}
