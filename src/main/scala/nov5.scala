import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, collect_list, count, countDistinct, current_date, date_sub, initcap, max, min, sum, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object nov5 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("spark.app.name","kushagra-spark-program")
    conf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    //Finding the maximum revenue for each product category and
    // the corresponding product.

    import spark.implicits._
//    val salesData = Seq(
//      ("Product1", "Category1", 100),
//      ("Product2", "Category2", 200),
//      ("Product3", "Category1", 150),
//      ("Product4", "Category3", 300),
//      ("Product5", "Category2", 250),
//      ("Product6", "Category3", 180)
//    ).toDF("Product", "Category", "Revenue")
//
//
//    val window=Window.partitionBy("Category").orderBy(col("Revenue").desc)
//
//    salesData.withColumn("max_revenue",max("Revenue").over(window))
//      .filter(col("max_revenue")===col("Revenue"))
//      .select(col("Product"),col("Category"),col("Revenue")).show()


    val salesData = Seq(
      ("Product1", "Category1", 100),
      ("Product2", "Category1", 200),
      ("Product3", "Category1", 150),
      ("Product4", "Category2", 300),
      ("Product5", "Category2", 250),
      ("Product6", "Category2", 180)
    ).toDF("Product", "Category", "Revenue")

      val window=Window.partitionBy("Category").orderBy(col("Revenue").desc)

      salesData.withColumn("sum_revenue",sum("Revenue").over(window))
        .filter(col("Category")===col("Category"))
        .select(col("Category"),col("Revenue"),col("sum_revenue")).show()



  }
}
