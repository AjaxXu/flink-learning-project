package com.louis.flink.ml.taxi.execise;

import com.louis.flink.ml.taxi.entity.TaxiRide;
import com.louis.flink.ml.taxi.source.TaxiRideSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author Louis
 */
public class CountExecise {
    public static void main(String[] args) throws Exception {
        // get an ExecutionEnvironment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // configure event-time processing
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // get the taxi ride data stream
        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource("src/main/resources/nycTaxi/nycTaxiRides.gz", 100, 60));
        rides.flatMap(new FlatMapFunction<TaxiRide, Long>() {
            @Override
            public void flatMap(TaxiRide taxiRide, Collector<Long> collector) throws Exception {
                collector.collect(1L);
            }
        }).timeWindowAll(Time.seconds(10)).sum(0).print();
        env.execute("counting");
    }
}
