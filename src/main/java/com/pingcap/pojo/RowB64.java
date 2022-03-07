package com.pingcap.pojo;

import org.tikv.shade.com.google.protobuf.ByteString;

public class RowB64 implements InfoInterface {

	public ByteString key;
	public ByteString value;
	
	public RowB64(ByteString key2, ByteString value2) {
		key = key2;
		value = value2;
	}

	@Override
	public boolean equalsValue(Object indexInfo) {
		return equals((RowB64)indexInfo);
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
		return null;
	}
	
    public boolean equals(RowB64 indexInfo) {
        boolean keyEqual = false;
        boolean valueEqual = false;
        if(null == key && null == indexInfo.key)
        	keyEqual = true;
        else if(null != key)
        	keyEqual = key.equals(indexInfo.key);
        else
        	keyEqual = indexInfo.key.equals(key);
        if(null == value && null == indexInfo.value)
        	valueEqual = true;
        else if(null != value)
        	valueEqual = value.equals(indexInfo.value);
        else
        	valueEqual = indexInfo.value.equals(value);
        return (keyEqual && valueEqual);
    }
}
