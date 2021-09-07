package com.alibaba.datax.plugin.reader.tikvreader;

import com.alibaba.datax.common.spi.ErrorCode;


public enum TikvReaderErrorCode implements ErrorCode {
    MISSING_PARAMS_ERROR("TikvReader-00", "table和startRowkey必须填入至少一个参数."),
    GET_CONNECTION_ERROR("TikvReader-01", "Tikv连接失败."),
    SCAN_DATA_ERROR("TikvReader-02", "Tikv scan异常."),
    RANGEKEY_EQUAL_ERROR("TikvReader-03", "startRowKey与endRowKey必须不同."),
    CLOSE_SESSION_ERROR("TikvReader-04", "startRowKey与endRowKey必须不同.");

    private final String code;
    private final String description;

    private TikvReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code, this.description);
    }
}

