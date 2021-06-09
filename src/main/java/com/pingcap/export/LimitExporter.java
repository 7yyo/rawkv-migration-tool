package com.pingcap.export;

import com.pingcap.enums.Model;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.List;
import java.util.Properties;

public class LimitExporter {

    public static void runLimitExporter(String exportFilePath, Properties properties, TiSession tiSession) {
        int exportLimit = Integer.parseInt(properties.getProperty(Model.EXPORT_LIMIT));
        RawKVClient rawKVClient = tiSession.createRawClient();
        ByteString startKey = ByteString.EMPTY;
        ByteString endKey = null;
        boolean isStart = true;
        List<Kvrpcpb.KvPair> kvPairList;
        while (isStart || endKey != null) {
            if (isStart) {
                kvPairList = rawKVClient.scan(startKey, exportLimit);
                isStart = false;
            } else {
                kvPairList = rawKVClient.scan(endKey, exportLimit);
            }
            endKey = kvPairList.get(kvPairList.size() - 1).getKey();
        }

    }

}
