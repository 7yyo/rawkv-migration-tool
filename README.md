```properties
// pd 地址
importer.tikv.pd=172.16.4.33:5555,172.16.4.34:5555,172.16.4.35:5555
// 主线程池核心线程数
importer.tikv.corePoolSize=10
// 主线程池最大线程数
importer.tikv.maxPoolSize=10
// 内部线程池线程数
importer.tikv.internalThreadNum=5
// 阻塞队列大小
importer.tikv.blockDequeCapacity=2
// 批量插入数据量大小
importer.tikv.batchSize=1000
// 测试使用：插入前删除已经存在的数据，0 关闭，非 0 开启，不需要改动
importer.tikv.deleteForTest=0
// 是否开启 check sum，0 关闭，非 0 开启
importer.checkSum.enabledCheckSum=1
// 是否在插入前检查已存在 key， 0 关闭，非 0 开启
importer.tikv.checkExistsKey=1
// check sum 的线程数
importer.checkSum.checkSumThreadNum=50
// check sum 文件夹路径
importer.checkSum.checkSumFilePath=/Users/yuyang/checkSum
// check sum 文件的分隔符，一般不需要改动
importer.checkSum.checkSumDelimiter=@#@#@
// 抽样数据百分比
importer.checkSum.checkSumPercentage=100
// 导入文件路径
importer.in.filePath=src/Main/resources/testFile/indexInfoJson
// 执行的任务，import - 导入，checkSum - 数据校验
importer.tikv.task=import
// 导入文件格式，有 json 和 csv 两种
importer.in.mode=json
// 导入数据格式，有 indexInfo，tempIndexInfo，indexType 三种
importer.in.scenes=indexInfo
// csv 文件的第一类分隔符，如果是 123，123，123 格式的 csv 文件，则该配置代表“，”，delimiter_2 会被忽略
importer.in.delimiter_1=|
// csv 文件的第二个分隔符
importer.in.delimiter_2=##
// key 的分隔符
importer.in.keyDelimiter=_:_
importer.out.envId=00998877
importer.out.appId=123456789
// type 过滤项，为空则不过滤
importer.ttl.type=A001,B001,C001
// ttl 时间，单位是 ms
importer.ttl.day=604800000
// 进度打印频率，单位是 ms
importer.timer.interval=30000
```
