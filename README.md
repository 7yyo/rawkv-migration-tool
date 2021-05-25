```properties
## pd 
importer.tikv.pd=172.16.4.33:5555,172.16.4.34:5555,172.16.4.35:5555
## 主线程核心线程数
importer.tikv.corePoolSize=1
## 主线程最大线程数
importer.tikv.maxPoolSize=1
## 内部子线程数量
importer.tikv.internalThreadNum=1
## 阻塞队列，不需要调整
importer.tikv.blockDequeCapacity=2
## 每次 batch put 大小
importer.tikv.batchSize=4
## 是否在每次批量插入前删除数据，为了不报错已存在 key 而设置，测试用，0 为不删除
importer.tikv.deleteForTest=1
## 是否做 check sum，0 为不执行
importer.checkSum.enabledCheckSum=1
## 是否做 key 已存在检查， 0 为不检查
importer.tikv.skipExistsKey=1
## check sum 的线程数
importer.checkSum.checkSumThreadNum=5
## check sum 文件存放的路径
importer.checkSum.checkSumFilePath=/Users/yuyang/checkSum
## check sum 文件行的分隔符，不需要调整
importer.checkSum.checkSumDelimiter=@#@#@
## check sum 抽数百分比
importer.checkSum.checkSumPercentage=1
## 导入数据文件路径
importer.in.filePath=/Users/yuyang/IdeaProjects/tikv_importer/src/Main/resources/testFile/indexInfoJson
## 模式，分为导入和check
importer.tikv.task=import
## 导入文件格式，有 json 和 csv
importer.in.mode=json
## 导入的数据文件格式
importer.in.scenes=indexInfo
## 导入的 csv 文件第一个分隔符
importer.in.delimiter_1=\\|
## 导入的 csv 文件第二个分隔符
importer.in.delimiter_2=##
## key 的分隔符
importer.in.keyDelimiter=_:_
## envId
importer.out.envId=00998877
## appId
importer.out.appId=123456789
## 跳过的 type
importer.ttl.type=A001,B001,C001
## ttl 时间 单位 ms
importer.ttl.day=604800000
## 统计线程打印周期 单位 ms
importer.timer.interval=30000
```
