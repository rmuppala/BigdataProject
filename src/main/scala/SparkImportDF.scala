
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkImportDF {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    //val df = spark.read.json("C:\\Users\\mraje\\Documents\\UMKC\\Big Data Analytics\\source\\Spark-WordCount\\data\\people.json")

    //df.show()

    val dfcsv = spark.read.format("csv").option("header", "true").load("C:\\Users\\mraje\\Documents\\UMKC\\Big Data Analytics\\source\\Spark-WordCount\\data\\ConsumerComplaints.csv")

    //dfcsv.show()

    //dfcsv.write.format("csv").save("C:\\Users\\mraje\\Documents\\UMKC\\Big Data Analytics\\source\\Spark-WordCount\\data\\ConsumerComplaintsWrite")

    //val dfComp = dfcsv.filter("Commpany" === 'Citibank'")


    dfcsv.registerTempTable("Consumer")

    val ct = spark.sql("select count(*) from Consumer where Company = 'Citibank' ")
    print("Citibank employee count :")
    ct.show()

    val csct = spark.sql("select * from Consumer where Company = 'Citibank' ")
    print("Citibank")
    //csct.show()

    val cscw = spark.sql("select * from Consumer where Company = 'Wells Fargo & Company' ")
    print("Wells Fargo & Company")
    //cscw.show()

    val unionDf = csct.union(cscw)
    unionDf.show()

    dfcsv.groupBy("Zip_code").count().show()

    import spark.implicits._
    dfcsv.filter($"Zip_code" === "30084").show()



    //Aggregate Max and Average

    val MaxDF = spark.sql("select Max(Complaint_ID) from Consumer")

    MaxDF.show()



    val AvgDF = spark.sql("select Avg(Complaint_ID) from Consumer")



    AvgDF.show()







    // Join the dataframe using sql







    csct.createOrReplaceTempView("citi")

    cscw.createOrReplaceTempView("wells")





    val joinSQl = spark.sql("select citi.Product_Name,wells.Product_name , wells.State_name FROM citi,wells where citi.State_Name = " +
      "wells.State_Name")

    joinSQl.show()




    val row13 = dfcsv.take(13).last
    print("13th row \n")
    print(row13.toString())



    val x = parseLine("Mortgage,Other mortgage,Loan modification,collection,foreclosure,null,null,null,Citibank,CA,95821")
    val y = spark.sparkContext.parallelize(x).toDF()
    y.show()
    print("parseLine result")
   // x.foreach(w => println(w))

  }
  def parseLine(line:String)  =
  {
    val data = line.split(",")
    data
    //val f1 = data(3).toString
    //val f2 = data(5).toString
    //print(f1)

  }

}