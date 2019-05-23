package com.louis.flink.ml.taxi.source;

import com.louis.flink.ml.taxi.entity.TaxiRide;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.zip.GZIPInputStream;

/**
 * @author Louis
 */
public class TaxiRideSource implements SourceFunction<TaxiRide> {

    private final int maxDelayMsecs;
    private final int watermarkDelayMSecs;

    private final String dataFilePath;
    private final int servingSpeed;

    private transient BufferedReader reader;
    private transient InputStream gzipStream;

    public TaxiRideSource(String dataFilePath) {
        this(dataFilePath, 0, 1);
    }

    public TaxiRideSource(String dataFilePath, int servingSpeedFactor) {
        this(dataFilePath, 0, servingSpeedFactor);
    }

    /**
     * Serves the TaxiRide records from the specified and ordered gzipped input file.
     * Rides are served out-of time stamp order with specified maximum random delay
     * in a serving speed which is proportional to the specified serving speed factor.
     *
     * @param dataFilePath The gzipped input file from which the TaxiRide records are read.
     * @param maxEventDelaySecs The max time in seconds by which events are delayed.
     * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
     */
    public TaxiRideSource(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
        if(maxEventDelaySecs < 0) {
            throw new IllegalArgumentException("Max event delay must be positive");
        }
        this.dataFilePath = dataFilePath;
        this.maxDelayMsecs = maxEventDelaySecs * 1000;
        this.watermarkDelayMSecs = maxDelayMsecs < 10000 ? 10000 : maxDelayMsecs;
        this.servingSpeed = servingSpeedFactor;
    }

    @Override
    public void run(SourceContext<TaxiRide> sourceContext) throws Exception {
        gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
        reader = new BufferedReader(new InputStreamReader(gzipStream, StandardCharsets.UTF_8));

        generateUnorderedStream(sourceContext);

        this.reader.close();
        this.reader = null;
        this.gzipStream.close();
        this.gzipStream = null;
    }

    private void generateUnorderedStream(SourceContext<TaxiRide> sourceContext) throws Exception {
        // 设置服务开始时间servingStartTime
        long servingStartTime = Calendar.getInstance().getTimeInMillis();

        // 数据开始时间dataStartTime，即第一个ride的timestamp
        long dataStartTime;

        Random rand = new Random(7452);

        // 使用优先队列进行emit，其比较方式为他们的等待时间
        PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<>(
                32,
                new Comparator<Tuple2<Long, Object>>() {
                    @Override
                    public int compare(Tuple2<Long, Object> o1, Tuple2<Long, Object> o2) {
                        return o1.f0.compareTo(o2.f0); }
                });

        // 读取第一个ride，并将第一个ride插入到schedule里
        String line;
        TaxiRide ride;
        if (reader.ready() && (line = reader.readLine()) != null) {
            // read first ride
            ride = TaxiRide.fromString(line);
            // extract starting timestamp
            dataStartTime = getEventTime(ride);
            // get delayed time，这个delayedtime是dataStartTime加一个随机数，随机数有最大范围，用来模拟真实世界情况
            long delayedEventTime = dataStartTime + getNormalDelayMsecs(rand);

            // 将ride插入到schedule里
            emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, ride));
            // 设置水印时间
            long watermarkTime = dataStartTime + watermarkDelayMSecs;
            // 下一个水印时间是时间戳是 watermarkTime - maxDelayMsecs - 1
            // 只能证明，这个时间一定是小于dataStartTime的
             Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
            // 将该水印放入Schedule，且这个水印被优先队列移到了ride之前
            emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));

        } else {
            return;
        }

        // 从文件里读取下一个ride（peek）
        if (reader.ready() && (line = reader.readLine()) != null) {
            ride = TaxiRide.fromString(line);
        }

        // read rides one-by-one and emit a random ride from the buffer each time
        while (emitSchedule.size() > 0 || reader.ready()) {

            // insert all events into schedule that might be emitted next
            // 在Schedule里的下一个事件的延时后时间
             long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().f0 : -1;
            // 当前从文件读取的ride的事件时间
            long rideEventTime = ride != null ? getEventTime(ride) : -1;
            // 这个while循环用来进行当前Schedule为空的情况
            while(
                    ride != null && ( // while there is a ride AND
                            emitSchedule.isEmpty() || // and no ride in schedule OR
                                    rideEventTime < curNextDelayedEventTime + maxDelayMsecs) // not enough rides in schedule
            )
            {
                // insert event into emit schedule
                long delayedEventTime = rideEventTime + getNormalDelayMsecs(rand);
                emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, ride));

                // read next ride
                if (reader.ready() && (line = reader.readLine()) != null) {
                    ride = TaxiRide.fromString(line);
                    rideEventTime = getEventTime(ride);
                }
                else {
                    ride = null;
                    rideEventTime = -1;
                }
            }

            // 提取Schedule里的第一个ride，叫做head
            Tuple2<Long, Object> head = emitSchedule.poll();
            // head应该要到达的时间
            long delayedEventTime = head.f0;
            long now = Calendar.getInstance().getTimeInMillis();

            // servingTime = servingStartTime + (delayedEventTime - dataStartTime)/ this.servingSpeed
            long servingTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime);
            // 应该再等多久，才让这个ride发生呢？
            long waitTime = servingTime - now;
            // 既然要等，那就睡着等吧
            Thread.sleep( (waitTime > 0) ? waitTime : 0);
            // 如果这个head是一个TaxiRide
            if(head.f1 instanceof TaxiRide) {
                TaxiRide emitRide = (TaxiRide)head.f1;
                // emit ride
                sourceContext.collectWithTimestamp(emitRide, getEventTime(emitRide));
            }
            // 如果这个head是一个水印标志
            else if(head.f1 instanceof Watermark) {
                Watermark emitWatermark = (Watermark)head.f1;
                // emit watermark
                sourceContext.emitWatermark(emitWatermark);
                // 并设置下一个水印标志到Schedule中
                long watermarkTime = delayedEventTime + watermarkDelayMSecs;
                // 同样，保证这个水印的时间戳在下一个ride的timestamp之前
                Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
                emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
            }
        }
    }

    public long toServingTime(long servingStartTime, long dataStartTime, long eventTime) {
        long dataDiff = eventTime - dataStartTime;
        return servingStartTime + (dataDiff / this.servingSpeed);
    }

    public long getEventTime(TaxiRide ride) {
        return ride.getEventTime();
    }

    public long getNormalDelayMsecs(Random rand) {
        long delay = -1;
        long x = maxDelayMsecs / 2;
        while(delay < 0 || delay > maxDelayMsecs) {
            delay = (long)(rand.nextGaussian() * x) + x;
        }
        return delay;
    }

    @Override
    public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
            if (this.gzipStream != null) {
                this.gzipStream.close();
            }
        } catch(IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
            this.gzipStream = null;
        }
    }
}
