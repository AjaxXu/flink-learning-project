package com.louis.flink.hbase;

/**
 * @author Louis
 */
public class Orders {
    private String cf1_name;
    private double cf2_amount;
    private int cf3_groupId;

    public Orders() {
    }

    public Orders(String cf1_name, double cf2_amount, int cf3_groupId) {
        this.cf1_name = cf1_name;
        this.cf2_amount = cf2_amount;
        this.cf3_groupId = cf3_groupId;
    }

    public String getCf1_name() {
        return cf1_name;
    }

    public void setCf1_name(String cf1_name) {
        this.cf1_name = cf1_name;
    }

    public double getCf2_amount() {
        return cf2_amount;
    }

    public void setCf2_amount(double cf2_amount) {
        this.cf2_amount = cf2_amount;
    }

    public int getCf3_groupId() {
        return cf3_groupId;
    }

    public void setCf3_groupId(int cf3_groupId) {
        this.cf3_groupId = cf3_groupId;
    }
}
