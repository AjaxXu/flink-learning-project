package com.louis.flink.hbase;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

public class HbaseToHbase {

    public static Logger logger = LoggerFactory.getLogger(HbaseToHbase.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(sEnv);
        sEnv.getConfig().disableSysoutLogging();
        ArrayList<String> getColumns = new ArrayList<String>(3);
        getColumns.add("cf1_name");
        getColumns.add("cf2_amount");
        getColumns.add("cf3_groupId");
        List<String> columnFamily = new ArrayList<>(3);
        columnFamily.add("cf1");
        columnFamily.add("cf2");
        columnFamily.add("cf3");

        List<String> setColumns = new ArrayList<>(3);
        setColumns.add("cf2:result");

        DataStreamSource<Orders> orderDataStream = sEnv.addSource(new HbaseStreamingSource(0L, 2000L, getColumns, "Orders"));

        DataStream<Tuple3<String, Double, Integer>> dataStream = orderDataStream.flatMap(
                new FlatMapFunction<Orders, Tuple3<String, Double, Integer>>() {

            @Override
            public void flatMap(Orders value, Collector<Tuple3<String, Double, Integer>> out) throws Exception {
                out.collect(new Tuple3<String, Double, Integer>(value.getCf1_name(),
                        value.getCf2_amount(), value.getCf3_groupId()));
            }
        });

        dataStream.keyBy(2).sum(1).addSink(
                new SinkHbase<Tuple3<String, Double, Integer>>(
                        "OrderResult", columnFamily, setColumns, "result"));

        sEnv.execute("test Hbase");
    }
}