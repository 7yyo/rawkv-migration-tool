package com.pingcap.pojo;

public class IndexType implements InfoInterface{
	private String source;

	public IndexType(){
	}
	
	public IndexType(String source){
		this.source = source;
	}
	
	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	@Override
	public boolean equalsValue(Object indexInfo) {
		if(null != source){
			if(source.equals(((IndexType) indexInfo).getSource()))
				return true;
			else
				return false;
		}
		else if (null == ((IndexType) indexInfo).getSource())
				return true;
		return false;
	}

	@Override
	public String getOpType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getUpdateTime() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDuration() {
		// TODO Auto-generated method stub
		return null;
	}

}
