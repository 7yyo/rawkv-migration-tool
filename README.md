# To Raw KV

`importer.in.filePath`导入文件的文件夹路径  
`importer.in.mode`导入文件的格式，有 json 和 original 两种  
`importer.in.scenes`导入文件的业务，目前有 indexInfo 和 tmpIndexInfo  
`importer.in.delimiter_1` original 文件的第一类分隔符，不需要改动  
`importer.in.delimiter_2` original 文件的第二类分隔符，不需要改动  
`importer.out.envId` 环境 id  
`importer.out.appId` appId  
`importer.ttl.type` 包含的类型数据不插入 TiKV  
`importer.ttl.day` TiKV 数据过期时间，单位为 ms   
`importer.tikv.batchSize` 一次 batch put 的数据量  
`importer.tikv.insideThread` 子线程数量  
`importer.tikv.corePoolSize` 主线程核心线程数量    
`importer.tikv.maxPoolSize` 主线程最大线程数量    
`importer.tikv.checkSumFilePath` checkSum 文件存在的路径  
`importer.tikv.checkSumDelimiter` checkSum 文件的分隔符，不需要改动  
`importer.tikv.pd` pd ip 和 port  
`importer.tikv.checkSumPercentage` checkSum 抽样数据比例，计算公式：数据量/checkSumPercentage。 取余
`importer.tikv.enabledCheckSum` 是否 checksum，非 0 为 checkSum  
`importer.timer.interval` 导入进度打印频率，单位为 ms  

# 示例

```properties
importer.in.filePath=/Users/yuyang/IdeaProjects/tikv_importer/src/Main/resources/testFile/tempIndexInfoJson
importer.in.mode=json
importer.in.scenes=tempIndexInfo
importer.in.delimiter_1=|
importer.in.delimiter_2=##
importer.out.envId=00998877
importer.out.appId=123456789
importer.ttl.type=A001,B001,C001
importer.ttl.day=604800000
importer.tikv.batchSize=200
importer.tikv.insideThread=10
importer.tikv.corePoolSize=10
importer.tikv.maxPoolSize=10
importer.tikv.checkSumFilePath=/Users/yuyang/checkSum
importer.tikv.checkSumDelimiter=@#@#@
importer.tikv.pd=172.16.4.32:5555,172.16.4.33:5555,172.16.4.34:5555,172.16.4.35:5555
importer.tikv.checkSumPercentage=1
importer.tikv.enabledCheckSum=0
importer.timer.interval=20000
```
