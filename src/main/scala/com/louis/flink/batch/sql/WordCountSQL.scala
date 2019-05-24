package com.louis.flink.batch.sql


import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

import scala.collection.mutable.ListBuffer

/**
  * @author Louis
  *
  */
object WordCountSQL {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val tEnv = BatchTableEnvironment.create(env)
        
        var list = ListBuffer[WC]()
        
        val line = "Hello Flink Hello TOM"
        
        line.split(" ").foreach(word => list+=WC(word, 1))
        
        val input = env.fromCollection(list)
        
        tEnv.registerDataSet("WordCount", input, 'word, 'frequency)
        
        val table = tEnv.sqlQuery("select word, sum(frequency) from WordCount group by word")
        table.toDataSet[WC].print()
    }
    
    case class WC(word: String, frequency: Long)
}
