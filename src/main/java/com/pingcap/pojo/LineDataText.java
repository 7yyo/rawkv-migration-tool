package com.pingcap.pojo;

import org.tikv.shade.com.google.protobuf.ByteString;

public class LineDataText {
	private int lineNo = 0;
	private String lineData;
	private ByteString value;
	
	public LineDataText(int lineNo,String lineData){
		this.lineNo = lineNo;
		this.lineData = lineData;
	}
	
	public LineDataText(int lineNo,String lineData,ByteString value){
		this.lineNo = lineNo;
		this.lineData = lineData;
		this.value = value;
	}

	public ByteString getValue() {
		return value;
	}

	public void setValue(ByteString value) {
		this.value = value;
	}

	public int getLineNo() {
		return lineNo;
	}

	public void setLineNo(int lineNo) {
		this.lineNo = lineNo;
	}

	public String getLineData() {
		return lineData;
	}

	public void setLineData(String lineData) {
		this.lineData = lineData;
	}

}
