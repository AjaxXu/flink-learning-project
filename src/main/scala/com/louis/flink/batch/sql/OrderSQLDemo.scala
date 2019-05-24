package com.louis.flink.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sinks.CsvTableSink

/**
  * @author Louis
  *
  */
object OrderSQLDemo {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val tableEnv = BatchTableEnvironment.create(env)
        env.setParallelism(1)
    
        val input = env.readTextFile("src/main/resources/order.txt")
        val orderInput = input.map(line => {
            val tokens = line.split(" ")
            Orders(tokens(0).toInt, tokens(1), tokens(2), tokens(3).toDouble)
        })
        
        val order = tableEnv.fromDataSet(orderInput)
        
        tableEnv.registerTable("Orders", order)
        
        tableEnv.scan("Orders").select("name").printSchema()
        
        val sqlQuery = tableEnv.sqlQuery("select name, sum(price) as total from Orders group by name order by total desc")
        
        val result = tableEnv.toDataSet[Result](sqlQuery)
        
        result.print()
        
        result.map(r => Tuple2.apply(r.name, r.total)).print()
        
        result.writeAsCsv("src/main/resources/orders.csv", "\n", "\t", writeMode = WriteMode.OVERWRITE)
    
        val sink = new CsvTableSink("src/main/resources/orders1.csv", "\t",1, WriteMode.OVERWRITE)
        tableEnv.registerTableSink("SQLTEST", Array("name", "total"), Array(Types.STRING, Types.DOUBLE), sink)
        sqlQuery.insertInto("SQLTEST")
        env.execute()
    }
    
    case class Orders(id: Integer, name: String, book: String, price: Double)
    case class Result(name: String, total: Double)
}
