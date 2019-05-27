package com.louis.flink.hbase;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Louis
 */
public class HbaseStreamingSource implements SourceFunction<Orders>{
    private static Logger loggerFactory = LoggerFactory.getLogger(HbaseStreamingSource.class);

    private static final long serialVersionUID = 1;

    private volatile boolean isRunning = true;

    /**
     * 开始的时间戳
     */
    private long startTime;

    /**
     * 每次查询多长时间的数据
     */
    private long interval;

    /**
     * 需要查询的列名
     */
    private ArrayList<String> columns;

    /**
     * 需要查询的表名
     */
    private String tableName;

    public HbaseStreamingSource(long startTime, long interval, ArrayList<String> columns, String tableName) {
        this.startTime = startTime;
        this.interval = interval;
        this.columns = columns;
        this.tableName = tableName;
    }

    public HbaseStreamingSource() {
    }

    @Override
    public void run(SourceContext<Orders> out) {
        if (isRunning) {
            long endTime = DateTime.now().getMillis() - interval;
            ResultScanner rs = getHbaseData(tableName, startTime, endTime - startTime, columns);
            transmitData(rs, out);
            startTime = endTime;
        }

        while (isRunning) {
            ResultScanner rs = getHbaseData(tableName, startTime, interval, columns);
            transmitData(rs, out);
            startTime += interval;
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                throw new RuntimeException("休眠异常", e);
            }
        }
    }

    @Override

    public void cancel() {
    }

    /**
     * 获取数据集
     * @param startTime 时间戳开始的时间
     * @param interval  间隔时间
     * @return 对应的结果集
     */

    private ResultScanner getHbaseData(String tableName, long startTime, long interval, List<String> columns) {
        Configuration conf = HBaseConfiguration.create();
        try {
            HTable table = new HTable(conf, tableName);
            Scan scan = new Scan();
            scan.setTimeRange(startTime, startTime + interval);
            for (String column : columns) {
                String[] columnName = column.split(":");
                scan.addColumn(Bytes.toBytes(columnName[0]), Bytes.toBytes(columnName[1]));
            }
            return table.getScanner(scan);
        } catch (IOException e) {
            throw new RuntimeException("读取数据异常", e);
        }
    }

    private void transmitData(ResultScanner rs, SourceContext<Orders> out) {
        Result result;
        try {
            while ((result = rs.next()) != null && isRunning) {
                KeyValue[] kvs = result.raw();
                for (KeyValue kv : kvs) {
                    String value = new String(kv.getValue());
                    String[] tokens = value.split(",");
                    out.collect(new Orders(tokens[0], Double.parseDouble(tokens[2]), Integer.parseInt(tokens[3])));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("结果集遍历异常", e);
        }
    }
}
