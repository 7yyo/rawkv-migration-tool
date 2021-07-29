java -Dp='/nvme2/data/rawkv.properties' -Dtikv.metrics.enable=true -jar tikv_importer_0612.jar
java -Dp='/nvme2/data/rawkv.properties' -Dm='truncate' -jar tikv_importer_0612.jar
java -Dp='/nvme2/data/rawkv.properties' -Dm='get' -Dk='indexInfo_:_pf01_:_1234567890_:_5900472169' -jar tikv_importer_0612.jar