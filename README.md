# Overview
![image](https://github.com/7yyo/to_tikv/blob/master/src/main/resources/img/total.png)
# Data validation
![image](https://github.com/7yyo/to_tikv/blob/master/src/main/resources/img/checksum.png)

# Configuration file parameters
```properties
# PD's IP and port
importer.tikv.pd=172.16.4.33:5555,172.16.4.34:5555,172.16.4.35:5555
# The number of core threads in the main thread pool
importer.tikv.corePoolSize=10
# The maximum number of threads in the main thread pool
importer.tikv.maxPoolSize=10
# The number of threads in the child thread pool
importer.tikv.internalThreadNum=5
# Insert the number of data in batches
importer.tikv.batchSize=1000
# Test the parameters, because it is possible to check whether the key already exists in the raw kv before inserting. In order to prevent the skip key caused by the same data in the database, delete the data to be inserted before each insertion. 1 is on, 0 is off. Please turn off this function in the formal test.
importer.tikv.deleteForTest=1
# After the full import is over, whether to check sum immediately. 1 is on, 0 is off.
importer.checkSum.enabledCheckSum=1
# Whether to check whether the data to be inserted exists in the raw kv before inserting, if it exists, skip the insert. 1 is on, 0 is off.
importer.tikv.checkExistsKey=1
# check sum thread count
importer.checkSum.checkSumThreadNum=50
# The path where the check sum file is stored is recommended to be configured on a better disk
importer.checkSum.checkSumFilePath=/Users/checkSum
# Data that fails to import will be recorded in this path
importer.in.batchPutErrFilePath=/Users/yuyang/batchPutErr
# The separator of the check sum file content, if there is no conflict, generally do not need to be modified.
importer.checkSum.checkSumDelimiter=@#@#@
# check sum sampling percentage, 100 is full data verification.
importer.checkSum.checkSumPercentage=100
# For the full check sum, directly compare with the original data file after the import, and no longer write the sampling check sum file. The problem here is that if there is data that fails ttl filtering or parsing in the original file, it will also appear in the check sum log. Need to manually confirm whether it is duplicate data.
# Note that when this configuration is 1, no matter how much checkSumPercentage is, the full check sum will be performed
importer.checkSum.simpleCheckSum=1
# Import data file path
importer.in.filePath=src/Main/resources/testFile/indexInfoJson
# The tasks performed by the tool currently support import and checkSum. Import is to perform import tasks, and checkSum is to perform data verification tasks. The check sum file executed here is the check sum file generated during import. If it is a full verification, it is to verify the original file.
importer.tikv.task=import
# Imported data format, currently supports json and csv
importer.in.mode=json
# Business data format, currently supports indexInfo, tempIndexInfo, indexType
importer.in.scenes=indexInfo
# For the separator in csv format, this refers to the first separator. Note: If it is data in xxx, xxx, xxx format, set it to "," here, and importer.in.delimiter_2 will be ignored in this case.
importer.in.delimiter_1=,
# For the separator in csv format, this refers to the second separator.
importer.in.delimiter_2=##
# The key separator of the original file, generally does not need to be modified
importer.in.keyDelimiter=_:_
importer.out.envId=00998877
importer.out.appId=123456789
importer.ttl.type=A001,B001,C001
# ttl time in ms
importer.ttl.day=604800000
# Statistics log printing interval, unit ms
importer.timer.interval=30000

# The path of full data export
exporter.out.filePath=/Users/yuyang/export
# There are two export methods, limit / region
exporter.tikv.mode=limit
# In limit export mode, n items are exported as one
exporter.tikv.exportLimit=1
```
# Usage
```shell
java -Dp='importer.properties_file_path' -jar tikv_importer.jar
```

