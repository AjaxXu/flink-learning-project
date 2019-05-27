package com.louis.flink.ml.taxi.entity;

import com.louis.flink.ml.taxi.utils.GeoUtils;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.table.shaded.org.joda.time.format.DateTimeFormat;
import org.apache.flink.table.shaded.org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Locale;

/**
 * @author Louis
 */
public class TaxiRide implements Comparable<TaxiRide>, Serializable {

    private static transient DateTimeFormatter timeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

    private long rideId;
    private boolean isStart;
    private DateTime startTime;
    private DateTime endTime;
    private float startLon;
    private float startLat;
    private float endLon;
    private float endLat;
    private short passengerCnt;
    private long taxiId;
    private long driverId;

    public TaxiRide() {
        this.startTime = new DateTime();
        this.endTime = new DateTime();
    }

    public TaxiRide(long rideId, boolean isStart, DateTime startTime, DateTime endTime, float startLon, float startLat, float endLon, float endLat, short passengerCnt, long taxiId, long driverId) {
        this.rideId = rideId;
        this.isStart = isStart;
        this.startTime = startTime;
        this.endTime = endTime;
        this.startLon = startLon;
        this.startLat = startLat;
        this.endLon = endLon;
        this.endLat = endLat;
        this.passengerCnt = passengerCnt;
        this.taxiId = taxiId;
        this.driverId = driverId;
    }

    public long getRideId() {
        return rideId;
    }

    public void setRideId(long rideId) {
        this.rideId = rideId;
    }

    public boolean isStart() {
        return isStart;
    }

    public void setIsStart(boolean start) {
        isStart = start;
    }

    public DateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(DateTime startTime) {
        this.startTime = startTime;
    }

    public DateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(DateTime endTime) {
        this.endTime = endTime;
    }

    public float getStartLon() {
        return startLon;
    }

    public void setStartLon(float startLon) {
        this.startLon = startLon;
    }

    public float getStartLat() {
        return startLat;
    }

    public void setStartLat(float startLat) {
        this.startLat = startLat;
    }

    public float getEndLon() {
        return endLon;
    }

    public void setEndLon(float endLon) {
        this.endLon = endLon;
    }

    public float getEndLat() {
        return endLat;
    }

    public void setEndLat(float endLat) {
        this.endLat = endLat;
    }

    public short getPassengerCnt() {
        return passengerCnt;
    }

    public void setPassengerCnt(short passengerCnt) {
        this.passengerCnt = passengerCnt;
    }

    public long getTaxiId() {
        return taxiId;
    }

    public void setTaxiId(long taxiId) {
        this.taxiId = taxiId;
    }

    public long getDriverId() {
        return driverId;
    }

    public void setDriverId(long driverId) {
        this.driverId = driverId;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(rideId).append(",");
        sb.append(isStart ? "START" : "END").append(",");
        sb.append(startTime.toString(timeFormatter)).append(",");
        sb.append(endTime.toString(timeFormatter)).append(",");
        sb.append(startLon).append(",");
        sb.append(startLat).append(",");
        sb.append(endLon).append(",");
        sb.append(endLat).append(",");
        sb.append(passengerCnt).append(",");
        sb.append(taxiId).append(",");
        sb.append(driverId);

        return sb.toString();
    }

    public static TaxiRide fromString(String line) {
        String[] tokens = line.split(",");
        if (tokens.length != 11) {
            throw new IllegalArgumentException("Invalid record: " + line);
        }

        TaxiRide ride = new TaxiRide();

        try {
            ride.rideId = Long.parseLong(tokens[0]);

            switch (tokens[1]) {
                case "START":
                    ride.isStart = true;
                    ride.startTime = DateTime.parse(tokens[2], timeFormatter);
                    ride.endTime = DateTime.parse(tokens[3], timeFormatter);
                    break;
                case "END":
                    ride.isStart = false;
                    ride.endTime = DateTime.parse(tokens[2], timeFormatter);
                    ride.startTime = DateTime.parse(tokens[3], timeFormatter);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid record: " + line);
            }

            ride.startLon = tokens[4].length() > 0 ? Float.parseFloat(tokens[4]) : 0.0f;
            ride.startLat = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
            ride.endLon = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
            ride.endLat = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;
            ride.passengerCnt = Short.parseShort(tokens[8]);
            ride.taxiId = Long.parseLong(tokens[9]);
            ride.driverId = Long.parseLong(tokens[10]);
        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }

        return ride;
    }

    // sort by timestamp,
    // putting START events before END events if they have the same timestamp
    @Override
    public int compareTo(TaxiRide other) {
        if (other == null) {
            return 1;
        }

        int compareTimes = Long.compare(this.getEventTime(), other.getEventTime());
        if (compareTimes == 0) {
            if (this.isStart == other.isStart) {
                return 0;
            } else {
                if (this.isStart) {
                    return -1;
                } else {
                    return 1;
                }
            }
        } else {
            return compareTimes;
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TaxiRide &&
                this.rideId == ((TaxiRide) other).rideId;
    }

    @Override
    public int hashCode() {
        return (int)this.rideId;
    }

    public long getEventTime() {
        if (isStart) {
            return startTime.getMillis();
        } else {
            return endTime.getMillis();
        }
    }

    public double getEuclideanDistance(double longitude, double latitude) {
        if (this.isStart) {
            return GeoUtils.getEuclideanDistance((float) longitude, (float) latitude, this.startLon, this.startLat);
        } else {
            return GeoUtils.getEuclideanDistance((float) longitude, (float) latitude, this.endLon, this.endLat);
        }
    }
}
