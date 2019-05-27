package com.louis.flink.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.io.Serializable;

public class FactoryConnect implements Serializable {

    private static volatile Connection connection;

    private FactoryConnect() {
    }

    public static Connection getConnection() throws IOException {
        if (null == connection) {
            synchronized (FactoryConnect.class) {
                try {
                    if (null == connection) {
                        Configuration conf = HBaseConfiguration.create();
                        connection = ConnectionFactory.createConnection(conf);
                    }
                } catch (Exception e) {
                    System.err.println("读取配置文件异常");
                }
            }
        }
        return connection;
    }
}
