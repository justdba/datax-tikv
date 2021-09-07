package com.alibaba.datax.plugin.reader.tikvreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.shade.com.google.protobuf.ByteString;

public class TikvHelper {

    private static final Logger LOG = LoggerFactory.getLogger(TikvReader.Job.class);

    public static ByteString bstr(String s) {
        return ByteString.copyFromUtf8(s);
    }

    public static String str(ByteString bs) {
        return bs.toStringUtf8();
    }

    public static void validateParameter(Configuration originalConfig) {
        String userTable = originalConfig.getString(Key.TABLE);
        String startRowKey = originalConfig.getString(Key.START_ROWKEY);
        String endRowKey = originalConfig.getString(Key.END_ROWKEY);
        if (userTable.equals("") && startRowKey.equals("")) {
            throw DataXException.asDataXException(TikvReaderErrorCode.MISSING_PARAMS_ERROR, "参数缺失");
        }

        if (startRowKey.equals(endRowKey) && !startRowKey.equals("")) {
            throw DataXException.asDataXException(TikvReaderErrorCode.RANGEKEY_EQUAL_ERROR, "参数输入出错");
        }

    }

}
