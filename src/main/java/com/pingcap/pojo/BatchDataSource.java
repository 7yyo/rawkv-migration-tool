package com.pingcap.pojo;

import java.util.ArrayList;
import java.util.List;

public class BatchDataSource {
    private List<LineDataText> lineDataSourceList;
    
    public BatchDataSource(List<LineDataText> dataSource){
        lineDataSourceList = new ArrayList<>(dataSource.size());
        lineDataSourceList.addAll(dataSource);
    }
    
    public List<LineDataText> getLineDataSourceList(){
    	return lineDataSourceList;
    }
    
    public void reduceLineDataSourceList(int rows){
        for(int kk=rows;0<kk;kk--){
        	lineDataSourceList.remove(kk);
        }
    }
    
    public void destory(){
    	lineDataSourceList.clear();
    	lineDataSourceList = null;
    }
}
