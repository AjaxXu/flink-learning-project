package com.louis.flink.ml.taxi.entity;


import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.table.shaded.org.joda.time.format.DateTimeFormat;
import org.apache.flink.table.shaded.org.joda.time.format.DateTimeFormatter;

import java.util.Locale;

/**
 * @author Louis
 * rideId         : Long      // a unique id for each ride
 * taxiId         : Long      // a unique id for each taxi
 * driverId       : Long      // a unique id for each driver
 * startTime      : DateTime  // the start time of a ride
 * paymentType    : String    // CSH or CRD
 * tip            : Float     // tip for this ride
 * tolls          : Float     // tolls for this ride
 * totalFare      : Float     // total fare collected
 */
public class TaxiFare {

    private static transient DateTimeFormatter timeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

    private long rideId;
    private long taxiId;
    private long driverId;
    private DateTime startTime;
    private String paymentType;
    private float tip;
    private float tolls;
    private float totalFare;

    public TaxiFare() {
        this.startTime = new DateTime();
    }

    public TaxiFare(long rideId, long taxiId, long driverId, DateTime startTime, String paymentType, float tip, float tolls, float totalFare) {
        this.rideId = rideId;
        this.taxiId = taxiId;
        this.driverId = driverId;
        this.startTime = startTime;
        this.paymentType = paymentType;
        this.tip = tip;
        this.tolls = tolls;
        this.totalFare = totalFare;
    }

    public long getRideId() {
        return rideId;
    }

    public void setRideId(long rideId) {
        this.rideId = rideId;
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

    public DateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(DateTime startTime) {
        this.startTime = startTime;
    }

    public String getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(String paymentType) {
        this.paymentType = paymentType;
    }

    public float getTip() {
        return tip;
    }

    public void setTip(float tip) {
        this.tip = tip;
    }

    public float getTolls() {
        return tolls;
    }

    public void setTolls(float tolls) {
        this.tolls = tolls;
    }

    public float getTotalFare() {
        return totalFare;
    }

    public void setTotalFare(float totalFare) {
        this.totalFare = totalFare;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(rideId).append(",");
        sb.append(taxiId).append(",");
        sb.append(driverId).append(",");
        sb.append(startTime.toString(timeFormatter)).append(",");
        sb.append(paymentType).append(",");
        sb.append(tip).append(",");
        sb.append(tolls).append(",");
        sb.append(totalFare);

        return sb.toString();
    }

    public static TaxiFare fromString(String line) {
        String[] tokens = line.split(",");
        if (tokens.length != 8) {
            throw new IllegalArgumentException("Invalid record: " + line);
        }

        TaxiFare fare = new TaxiFare();

        try {
            fare.rideId = Long.parseLong(tokens[0]);
            fare.taxiId = Long.parseLong(tokens[1]);
            fare.driverId = Long.parseLong(tokens[2]);
            fare.startTime = DateTime.parse(tokens[3], timeFormatter);
            fare.paymentType = tokens[4];
            fare.tip = tokens[5].length() > 0 ? Float.parseFloat(tokens[5]) : 0.0f;
            fare.tolls = tokens[6].length() > 0 ? Float.parseFloat(tokens[6]) : 0.0f;
            fare.totalFare = tokens[7].length() > 0 ? Float.parseFloat(tokens[7]) : 0.0f;
        } catch (NumberFormatException nfe) {
            throw new RuntimeException("Invalid record: " + line, nfe);
        }
        return fare;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TaxiFare &&
                this.rideId == ((TaxiFare) other).rideId;
    }

    @Override
    public int hashCode() {
        return (int)this.rideId;
    }

    public long getEventTime() {
        return startTime.getMillis();
    }
}
