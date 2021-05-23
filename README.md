```properties
# pd address
importer.tikv.pd=172.16.4.32:5555,172.16.4.33:5555,172.16.4.34:5555,172.16.4.35:5555
# File processing main thread's internal thread pool size
importer.tikv.internalThreadNum=10
# File processing main thread core thread pool size
importer.tikv.corePoolSize=5
# File processing main thread maximum thread pool size
importer.tikv.maxPoolSize=5
# Batch size for each insertion
importer.tikv.batchSize=10000
# Whether to delete the existing data before inserting, for testing purposes. 1 means to delete, 0 means not to delete.
importer.tikv.deleteForTest=1
# check sum switch, 0 is off check sum
importer.checkSum.enabledCheckSum=1
# check sum the number of threads
importer.checkSum.checkSumThreadNum=5
# check sum file storage path, it is recommended to put it on the best disk
importer.checkSum.checkSumFilePath=/Users/yuyang/checkSum
# check sum file separator, if not necessary, no need to modify
importer.checkSum.checkSumDelimiter=@#@#@
# check sum Sampling percentage, 100 is 100%
importer.checkSum.checkSumPercentage=100
# The storage path of the imported data file
importer.in.filePath=/resources/testFile/tempIndexInfoJson
# Import data file format, there are two kinds of json and csv
importer.in.mode=json
# The format of the imported data, currently there are indexInfo, tempIndexInfo, indexType three
importer.in.scenes=tempIndexInfo
# The first delimiter of csv file
importer.in.delimiter_1=|
# The second delimiter of csv file
importer.in.delimiter_2=##
# key separator
importer.in.keyDelimiter=_:_
importer.out.envId=00998877
importer.out.appId=123456789
importer.ttl.type=A001,B001,C001
# TTL expiration time, the unit is ms
importer.ttl.day=604800000
# The statistical interval of import and checkSum progress, the unit is ms
importer.timer.interval=30000
```
