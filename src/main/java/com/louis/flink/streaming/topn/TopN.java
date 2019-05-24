package com.louis.flink.streaming.topn;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Properties;
import java.util.TreeMap;

/**
 * @author Louis
 */
public class TopN {
    public static void main(String[] args) throws Exception {
        //每隔5秒钟 计算过去1小时 的 Top 3 商品
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        FlinkKafkaConsumer011<String> input = new FlinkKafkaConsumer011<>("topn", new SimpleStringSchema(), properties);

        //从最早开始消费 位点
        input.setStartFromEarliest();

        DataStreamSource<String> source = env.addSource(input);

        SingleOutputStreamOperator<Tuple2<String, Integer>> count = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                collector.collect(new Tuple2<>(s, 1));
            }
        }).keyBy(0).window(SlidingProcessingTimeWindows.of(Time.seconds(300), Time.seconds(5))).sum(1);

        count
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new TopNProcessFunction(3))
                .print();

        env.execute();
    }

    private static class TopNProcessFunction extends ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow> {

        private int topSize;

        public TopNProcessFunction(int topSize) {

            this.topSize = topSize;
        }

        public void process(
                ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow>.Context arg0,
                Iterable<Tuple2<String, Integer>> input,
                Collector<String> out) throws Exception {

            TreeMap<Integer, Tuple2<String, Integer>> treemap = new TreeMap<>(
                    new Comparator<Integer>() {

                        @Override
                        public int compare(Integer y, Integer x) {
                            return (x < y) ? -1 : 1;
                        }

                    }); //treemap按照key降序排列，相同count值不覆盖

            for (Tuple2<String, Integer> element : input) {
                treemap.put(element.f1, element);
                if (treemap.size() > topSize) { //只保留前面TopN个元素
                    treemap.pollLastEntry();
                }
            }

            out.collect("=======Start==========\n热销图书列表:\n"+ new Timestamp(System.currentTimeMillis()) +  "\n" +  treemap.toString() + "\n========End=======\n");
        }

    }
}
