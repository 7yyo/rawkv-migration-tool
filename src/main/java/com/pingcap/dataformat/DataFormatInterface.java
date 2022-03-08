package com.pingcap.dataformat;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.tikv.shade.com.google.protobuf.ByteString;

import com.pingcap.pojo.InfoInterface;

public interface DataFormatInterface {
	public static final int DATATYPE_INDEXTYPE = 0;
	public static final int DATATYPE_INDEXINFO = 1;
	public static final int DATATYPE_TEMPINDEX = 2;
	public static final String DATA_LINENO = "LINENO";
	public static final String DATA_LINEDATA = "LINEDATA";
	public static final String SPLITERSET = "|_:@#$%^&*-+=<>;[]?";
	public static final char JSON_BODY_PRE= '{';
	public static final char JSON_BODY_TAL= '}';
	
	public interface DataFormatCallBack {
		public boolean putDataCallBack(String ttlType,int dataType,ByteString key,ByteString value);
	}
	
	public interface UnDataFormatCallBack {
		public boolean getDataCallBack(String strJson, String type, int typeInt);
	}

	public boolean formatToKeyValue( String scenes, String line, DataFormatCallBack dataFormatCallBack) throws Exception;
	
	public InfoInterface packageToObject(String scenes, ByteString key, ByteString value, DataFormatCallBack dataFormatCallBack) throws Exception;
	
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
	
	//Simply determine whether it is a JSON string 
/*	public static boolean isJsonString(String source){
		if(StringUtils.isEmpty(source)|| 2 >= source.length())
			return false;
		if(JSON_BODY_PRE == source.charAt(0)&& JSON_BODY_TAL == source.charAt(source.length()-1))
			return true;
		return false;
	}*/
	
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
