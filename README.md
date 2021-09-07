

#### TikvReader插件说明

- 输入PDServer的ip列表
- 输入表名
- 输入startRowkey / endRowkey


- 按批次取数，默认每次读取10240条记录
- 整表同步时，起止范围填空串，"startRowkey":"","endRowkey": ""


#### TikvReader插件运行
python datax.py tikv.json


kvReader性能测试
------------


2021-06-01 15:44:03.142 [job-0] INFO  JobContainer - 
任务启动时刻                    : 2021-06-01 15:43:33
任务结束时刻                    : 2021-06-01 15:44:03
任务总计耗时                    :                 30s
任务平均流量                    :           48.96MB/s
记录写入速度                    :         325185rec/s
读出记录总数                    :             9755560
读写失败总数                    :                   0

------------





#### tikv写入hive模板
```html
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            }
        },
        "content": [{
                "reader": {
                    "name": "tikvreader",
                    "parameter": {
                        "tikvConfig": "ip1:2379,ip2:2379,ip3:2379",
                        "table": "table1",
                        "range": {
                            "startRowkey": "table1key1",
                            "endRowkey": "table1key6"
                        }
                    }
                },

                "writer": {
                    "name": "hdfswriter",
                    "parameter": {
                        "defaultFS": "hdfs://ip4:9000",
                        "fileType": "orc",
                        "path": "/user/hive/warehouse/hivetestdb.db/hive4",
                        "fileName": "hive4",
                        "column": [{
                                "name": "id",
                                "type": "string"
                            }, {
                                "name": "col1",
                                "type": "string"
                            }
                        ],
                        "writeMode": "append",
                        "fieldDelimiter": "\t",
                        "compress": "NONE"
                    }
                }

            }
        ]
    }
}

```
