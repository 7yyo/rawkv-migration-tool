![image](https://github.com/7yyo/to_tikv/blob/master/src/main/resources/img/total.png)

```properties
# pd address
importer.tikv.pd=172.16.4.33:5555,172.16.4.34:5555,172.16.4.35:5555
# The number of core threads in the main thread pool
importer.tikv.corePoolSize=10
# The maximum number of threads in the main thread pool
importer.tikv.maxPoolSize=10
# Number of threads in the internal thread pool
importer.tikv.internalThreadNum=5
# Blocking queue size
importer.tikv.blockDequeCapacity=2
# Batch insert data size
importer.tikv.batchSize=1000
# Test use: delete the existing data before inserting, 0 is closed, non-zero is open, no need to change
importer.tikv.deleteForTest=0
# Whether to open check sum, 0 is off, non-zero is on
importer.checkSum.enabledCheckSum=1
# Whether to check the existing key before inserting, 0 is closed, non-zero is open
importer.tikv.checkExistsKey=1
# check sum the number of threads
importer.checkSum.checkSumThreadNum=50
# check sum folder path
importer.checkSum.checkSumFilePath=/Users/yuyang/checkSum
# check sum file separator, generally do not need to be changed
importer.checkSum.checkSumDelimiter=@#@#@
# Percentage of sampled data
importer.checkSum.checkSumPercentage=100
# Import file path
importer.in.filePath=src/Main/resources/testFile/indexInfoJson
# Tasks performed, import-import, checkSum-data verification
importer.tikv.task=import
# Import file format, there are two kinds of json and csv
importer.in.mode=json
# Import data format, there are indexInfo, tempIndexInfo, indexType three
importer.in.scenes=indexInfo
# The first type of delimiter of csv file, if it is a csv file in 123, 123, 123 format, this configuration represents "," and delimiter_2 will be ignored
importer.in.delimiter_1=|
# The second separator of the csv file
importer.in.delimiter_2=##
# key separator
importer.in.keyDelimiter=_:_
importer.out.envId=00998877
importer.out.appId=123456789
# type filter item, no filter if it is empty
importer.ttl.type=A001,B001,C001
# ttl time, the unit is ms
importer.ttl.day=604800000
# Progress printing frequency, the unit is ms
importer.timer.interval=30000
```
