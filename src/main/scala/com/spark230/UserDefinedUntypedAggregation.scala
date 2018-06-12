/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.spark230

// $example on:untyped_custom_aggregation$
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
// $example off:untyped_custom_aggregation$

object UserDefinedUntypedAggregation {

  // $example on:untyped_custom_aggregation$
  object MyAverage extends UserDefinedAggregateFunction {
    // 输入参数类型
    def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)
    // aggregation buffer中值的数据类型
    def bufferSchema: StructType = {
      StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
    }
    // 返回值的数据类型
    def dataType: DataType = DoubleType
    // Whether this function always returns the same output on the identical input
    def deterministic: Boolean = true
    //初始化aggregation buffer。这个buffer 是Row，这个Row增加了一些根据index获取值的标准方法，还提供一些更改其值的方法。注意数组和map中的buffer仍然不可改变
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L  //数据值
      buffer(1) = 0L  //数据条数
    }
    // 从输入中获取数据更新buffer
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
      }
    }
    // 合并两个 aggregation buffers 然后存储更新后的值到保存到buffer1中
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }
    // 计算最终的结果
    def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
  }
  // $example off:untyped_custom_aggregation$

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("Spark SQL user-defined DataFrames aggregation example")
      .getOrCreate()

    // $example on:untyped_custom_aggregation$
    // Register the function to access it
    spark.udf.register("myAverage", MyAverage)

    val df = spark.read.json("data/employees.json")
    df.createOrReplaceTempView("employees")
    df.show()
    // +-------+------+
    // |   name|salary|
    // +-------+------+
    // |Michael|  3000|
    // |   Andy|  4500|
    // | Justin|  3500|
    // |  Berta|  4000|
    // +-------+------+

    val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()
    // +--------------+
    // |average_salary|
    // +--------------+
    // |        3750.0|
    // +--------------+
    // $example off:untyped_custom_aggregation$

    spark.stop()
  }

}
