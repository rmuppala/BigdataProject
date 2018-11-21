/////////////////////////////////////////////////////////////////////////////

//  Scala File Name   : EmptyDataFrame

//  Author            : Laxminarayana Cheruku

//  Creation Date     : 21/APR/2016

//  Usage             : Create Empty DataFrame 

/////////////////////////////////////////////////////////////////////////////

package src.main.scala



import org.apache.spark.SparkConf

import org.apache.spark.SparkContext



object EmptyDataFrame {



  def main(args: Array[String]){



    //Create Spark Conf

    val sparkConf = new SparkConf().setAppName("Empty-Data-Frame").setMaster("local")
    val sc = new SparkContext(sparkConf)
    //sqlcontext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)



    //Import Sql Implicit conversions

    import sqlContext.implicits._

    import org.apache.spark.sql.Row

    import org.apache.spark.sql.types.{StructType,StructField,StringType}


    //Create Schema RDD

    val schema_string = "id,score"
    val schema_rdd = StructType(schema_string.split(",").map(fieldName => StructField(fieldName, StringType, true)) )
    val empty_df = sqlContext.createDataFrame(sc.emptyRDD[Row], schema_rdd)



    //Some Operations on Empty Data Frame

    empty_df.show()

    println(empty_df.count())



    //You can register a Table on Empty DataFrame, it's empty table though

    empty_df.registerTempTable("empty_table")



    //let's check it ;)

    val res = sqlContext.sql("select * from empty_table")

    res.show



  }



}