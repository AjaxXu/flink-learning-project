package com.louis.flink.cache;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Louis
 */
public class CacheFileDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1：注册一个文件,可以使用hdfs或者s3上的文件
        env.registerCachedFile("src/main/resources/log4j.properties","property");

        DataSource<String> data = env.fromElements("org", "apache", "log4j", "not contained");
        DataSet<String> result =data.map(new RichMapFunction<String, String>() {
            private ArrayList<String> dataList = new ArrayList<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //2：使用文件
                File file = getRuntimeContext().getDistributedCache().getFile("property");
                List<String> lines = FileUtils.readLines(file);
                for (String line : lines) {
                    this.dataList.add(line);
                    System.out.println("discache:" + line);
                }
            }

            @Override
            public String map(String s) throws Exception {
                for (String data: dataList) {
                    if (data.contains(s)) {
                        return s;
                    }
                }
                return "\"" + s + "\"  not in cache file";
            }
        });
        result.print();
    }
}
