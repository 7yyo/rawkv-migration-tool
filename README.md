# Cassandra -> TiKV 数据迁移工具

## 格式需求

### indexInfo
#### 导出格式
```json
{"appId":"123","serviceTag":"{\"ACCT_DTL_TYPE\":\"SP0002\",\"PD_SALE_FTA_CD\":\"99\",\"AR_ID\":\"\",\"CMTRST_CST_ACCNO\":\"\",\"QCRCRD_IND\":\" \",\"BLKMDL_ID\":\"52\",\"CORPPRVT_FLAG\":\"1\"}","targetId":"0037277","updateTime":"2020-11-04 17:12:04"}
```
#### 导入格式
```json
{indexInfo_:_1_:_A00007_:_1111135}

{"appId":"123","serviceTag":"{\"ACCT_DTL_TYPE\":\"SP0006\",\"PD_SALE_FTA_CD\":\"99\",\"AR_ID\":\"\",\"CMTRST_CST_ACCNO\":\"\",\"QCRCRD_IND\":\" \",\"BLKMDL_ID\":\"52\",\"CORPPRVT_FLAG\":\"1\"}","targetId":"0037277","updateTime":"2020-11-04 17:12:04"}
```

## 配置文件
### importer.properties

`importer.in.filePath`           导入文件夹路径  
`importer.out.envId`             如果有配置则覆盖数据文件的 envId  
`importer.out.appId`             赋值给导入 json  
`importer.ttl.type`              包含的 ttl type 不会被导入  
`importer.tikv.corePoolSize`     主线程数量  
`importer.tikv.maxPoolSize`      主线程数量  
`importer.tikv.batchSize`        批量插入的数据量  
`importer.tikv.insideThread`     单个文件内部线程数量  
`importer.tikv.pd`               pd 的 IP 和端口   
#### 示例
```properties
importer.in.filePath=src/main/resources/testFile
importer.out.envId=1
importer.out.appId=123
importer.ttl.type=A001,B001,C001
importer.tikv.batchSize=100
importer.tikv.insideThread=4
importer.tikv.corePoolSize=10
importer.tikv.maxPoolSize=10
importer.tikv.pd=172.16.4.33:5555,172.16.4.34:5555,172.16.4.35:5555
```

## 功能点
1. TTL Type 跳过导入
```log
[2021-05-17 12:12:02.275] [WARN] [BatchPutIndexInfoJob.java:207] [Skip key - ttl: indexInfo_:_1_:_B001_:_1111129 in '/Users/yuyang/IdeaProjects/tikv_importer/src/main/resources/testFile/import.txt']
```
2. 已存在数据跳过导入
```log
[2021-05-17 12:15:03.434] [WARN] [BatchPutIndexInfoJob.java:250] [Skip key - exists: [ indexInfo_:_1_:_A00006_:_1111148 ], file is [ /Users/yuyang/IdeaProjects/tikv_importer/src/main/resources/testFile/import.txt ]]
```
3. JSON 解析失败，打印错误日志，跳过该行继续导入
```log
[2021-05-17 12:15:39.023] [ERROR] [BatchPutIndexInfoJob.java:191] [Failed to parse json, file='/Users/yuyang/IdeaProjects/tikv_importer/src/main/resources/testFile/import2.txt', line=1, json='2weq']
```
4. 导入完成统计信息，文件总行数，导入行数，跳过行数，导入时间，跳过 ttl 数量
```log
[2021-05-17 12:15:40.083] [INFO] [IndexInfoS2TJob.java:112] [Import Report] File->[/Users/yuyang/IdeaProjects/tikv_importer/src/main/resources/testFile/import.txt], Total rows->[50], Imported rows->[43], Skip rows->[7], Duration->[1s], Skip ttl: B001=4,C001=3,A001=0,]
```
