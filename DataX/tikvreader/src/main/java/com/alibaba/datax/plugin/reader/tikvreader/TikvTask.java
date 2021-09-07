package com.alibaba.datax.plugin.reader.tikvreader;

import com.alibaba.datax.common.exception.DataXException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;


import java.util.List;

public class TikvTask {
    private static Logger LOG = LoggerFactory.getLogger(TikvTask.class);
    private RawKVClient client;
    private TiSession session;
    protected String startRowKey;
    protected String endRowKey;
    private final String userTable;
    private final String tikvConfig;
    protected boolean isFullScan;

    public TikvTask(com.alibaba.datax.common.util.Configuration originalConfig) {
        this.tikvConfig = originalConfig.getString(Key.TIKV_CONFIG);
        this.userTable = originalConfig.getString(Key.TABLE);
        this.startRowKey = originalConfig.getString(Key.START_ROWKEY);
        this.endRowKey = originalConfig.getString(Key.END_ROWKEY);
        this.isFullScan = false;
        this.client = tikvConnection();
    }

    protected void CheckTableScan() {
        if (this.startRowKey.equals("")) this.startRowKey = this.userTable;
        if (this.endRowKey.equals("")) this.isFullScan = true;
    }

    private RawKVClient tikvConnection() {
        try {
            this.session = TiSession.create(TiConfiguration.createRawDefault(this.tikvConfig));
            this.client = this.session.createRawClient();

        } catch (Exception e) {
            closeConnection();
            throw DataXException.asDataXException(TikvReaderErrorCode.GET_CONNECTION_ERROR, e);
        }
        return this.client;
    }

    protected void closeConnection() {
        if (this.client != null) {
            this.client.close();
        }
        try {
            if (this.session != null) {
                this.session.close();
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(TikvReaderErrorCode.CLOSE_SESSION_ERROR, e);
        }
    }

    protected List<Kvrpcpb.KvPair> scanStartRowKey(ByteString startRowKey) {
        // limit param never equal 1
        return this.client.scan(startRowKey, Key.MAX_KEYSCAN_LIMIT);
    }

    protected List<Kvrpcpb.KvPair> scanRangeRowKey(ByteString startRowKey, ByteString endRowKey) {
        // limit param never equal 1
        return this.client.scan(startRowKey, endRowKey, Key.MAX_KEYSCAN_LIMIT);
    }

}
