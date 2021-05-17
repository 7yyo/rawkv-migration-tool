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

