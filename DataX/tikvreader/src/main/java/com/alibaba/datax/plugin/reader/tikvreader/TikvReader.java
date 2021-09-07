package com.alibaba.datax.plugin.reader.tikvreader;


import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;

import org.tikv.shade.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;


import java.util.ArrayList;
import java.util.List;

public class TikvReader extends Reader {
    public static class Job extends Reader.Job {
        private static Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration originalConfig = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            TikvHelper.validateParameter(this.originalConfig);
        }


        @Override
        public void prepare() {
        }


        @Override
        public List<Configuration> split(int adviceNumber) {
            //Currently only single task work.
            if (adviceNumber != 1) {
                adviceNumber = 1;
                LOG.warn("Tikv Currently only single task work.");
            }
            List<Configuration> configurations = new ArrayList<Configuration>(adviceNumber);
            for (int i = 0; i < adviceNumber; i++) {
                configurations.add(originalConfig);
            }
            return configurations;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

    }

    public static class Task extends Reader.Task {
        private static Logger LOG = LoggerFactory.getLogger(Task.class);
        private TikvTask tikvTaskproxy;

        @Override
        public void init() {
            Configuration originalConfig = super.getPluginJobConf();
            this.tikvTaskproxy = new TikvTask(originalConfig);
            tikvTaskproxy.CheckTableScan();
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startRead(RecordSender recordSender) {
            int i;
            int flag = 0;
            List<Kvrpcpb.KvPair> kvList;

            try {
                Record record = recordSender.createRecord();
                ByteString loopStartKey = TikvHelper.bstr(tikvTaskproxy.startRowKey);
                ByteString endRowKey = TikvHelper.bstr(tikvTaskproxy.endRowKey);

                while (true) {
                    i = flag;
                    if (tikvTaskproxy.isFullScan) {
                        kvList = tikvTaskproxy.scanStartRowKey(loopStartKey);
                    } else {
                        kvList = tikvTaskproxy.scanRangeRowKey(loopStartKey, endRowKey);
                    }

                    if (kvList.size() == 1 && flag == 1) {
                        break;
                    }

                    for (; i < kvList.size(); i++) {
                        record.addColumn(new StringColumn(TikvHelper.str(kvList.get(i).getKey())));
                        record.addColumn(new StringColumn(TikvHelper.str(kvList.get(i).getValue())));
                        recordSender.sendToWriter(record);
                        record = recordSender.createRecord();
                    }

                    loopStartKey = kvList.get(--i).getKey();
                    flag = 1;

                }
                recordSender.flush();

            } catch (Exception e) {
                tikvTaskproxy.closeConnection();
                throw DataXException.asDataXException(TikvReaderErrorCode.SCAN_DATA_ERROR, e);
            }
            tikvTaskproxy.closeConnection();

        }


        @Override
        public void post() {
            super.post();
        }


        @Override
        public void destroy() {
            tikvTaskproxy.closeConnection();
        }
    }
}