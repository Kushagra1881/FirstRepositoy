import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, collect_list, count, countDistinct, current_date, date_sub, initcap, max, min, sum, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, LogManager,Logger}

object nov14 {
  def main(args: Array[String]): Unit = {
//    val logger: Logger = Logger.getLogger(this.getClass)
    val conf = new SparkConf()
    conf.set("spark.app.name","kushagra-spark-program")
    conf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

//    SPARK_PROBLEMS_FOR_DATE_MANIPULATION_PRACTICE

//1. Extract the day of the month from a date column.
    //o Sample Data:
    // Scala: Seq(("2024-01-15"), ("2024-02-20"), ("2024-03-25")).toDF("date")
    // PySpark: data = [("2024-01-15",), ("2024-02-20",), ("2024-03-25",)]

//    val data = Seq(("2024-01-15"), ("2024-02-20"), ("2024-03-25")).toDF("date")
//
//    val df1 = data.withColumn("day_of_month",
//    dayofmonth(col("date")))
//
//    df1.show()

    //---------------------------------------------------------------
//2. Get the weekday name (e.g., Monday, Tuesday) from a date column.
    //o Sample Data:
    // Scala: Seq(("2024-04-02"), ("2024-04-03"), ("2024-04-04")).toDF("date")
    // PySpark: data = [("2024-04-02",), ("2024-04-03",), ("2024-04-04",)]

//    val data = Seq(("2024-04-02"), ("2024-04-03"), ("2024-04-04")).toDF("date")
//
//    val df1 = data.withColumn("name_of_weekday",
//    date_format(col("date"),"EEEE"))
//
//    df1.show()

    //---------------------------------------------------------------
    //3. Calculate the number of days between two dates.
    //o Sample Data:
    // Scala: Seq(("2024-01-01", "2024-01-10"), ("2024-02-01", "2024-02-
    //20")).toDF("start_date", "end_date")
    // PySpark: data = [("2024-01-01", "2024-01-10"), ("2024-02-01", "2024-02-
    //20")]

//    val data = Seq(("2024-01-01", "2024-01-10"),
//      ("2024-02-01", "2024-02-20"))
//      .toDF("start_date", "end_date")
//
//    val df1 = data.withColumn("days_btw_two_dates",
//      datediff(col("end_date"),col("start_date")))
//
//    df1.show()

    //---------------------------------------------------------------
    //4. Add 10 days to a given date column.
    //o Sample Data:
    // Scala: Seq(("2024-05-01"), ("2024-05-15")).toDF("date")
    // PySpark: data = [("2024-05-01",), ("2024-05-15",)]

//    val data = Seq(("2024-05-01"), ("2024-05-15")).toDF("date")
//
//    val df1 = data.withColumn("add_10_days",
//      date_add(col("date"),10))
//
//    df1.show()

    //---------------------------------------------------------------
    //5. Subtract 7 days from a given date column.
    //o Sample Data:
    // Scala: Seq(("2024-06-10"), ("2024-06-20")).toDF("date")
    // PySpark: data = [("2024-06-10",), ("2024-06-20",)]

//    val data = Seq(("2024-06-10"), ("2024-06-20")).toDF("date")
//
//    val df1 = data.withColumn("sub_7_days",
//      date_sub(col("date"),7))
//
//    df1.show()

    //---------------------------------------------------------------
    //6.Filter rows where the year in a date column is 2023.
    //o Sample Data:
    // Scala: Seq(("2023-08-12"), ("2024-08-15")).toDF("date")
    // PySpark: data = [("2023-08-12",), ("2024-08-15",)]

//    val data = Seq(("2024-06-10"), ("2023-06-20")).toDF("date")
//
//    val df1 = data.filter(year(col("date"))==="2023")
//
//    df1.show()

    //---------------------------------------------------------------
    //6. Get the last day of the month for a given date column.
    //o Sample Data:
    // Scala: Seq(("2024-07-10"), ("2024-07-25")).toDF("date")
    // PySpark: data = [("2024-07-10",), ("2024-07-25",)]

//    val data = Seq(("2024-06-10"), ("2023-06-20")).toDF("date")
//
//    val df1 = data.withColumn("last_day_of_month",
//      last_day(col("date"))
//    )
//
//    df1.show()

    //---------------------------------------------------------------
    //7. Extract only the year from a date column.
    //o Sample Data:
    // Scala: Seq(("2024-01-01"), ("2025-02-01")).toDF("date")
    // PySpark: data = [("2024-01-01",), ("2025-02-01",)]

//    val data = Seq(("2024-01-01"), ("2025-02-01")).toDF("date")
//
//    val df1 = data.withColumn("year_of_date",
//      year(col("date"))
//    )
//
//    df1.show()


    //---------------------------------------------------------------
    //8. Calculate the number of months between two dates.
    //o Sample Data:
    // Scala: Seq(("2024-01-01", "2024-04-01"), ("2024-05-01", "2024-08-
    //01")).toDF("start_date", "end_date")
    // PySpark: data = [("2024-01-01", "2024-04-01"), ("2024-05-01", "2024-08-
    //01")]

//    val data = Seq(
//      ("2024-01-01", "2024-04-01"),
//      ("2024-05-01", "2024-08-01"))
//      .toDF("start_date", "end_date")
//
//    val df1 = data.withColumn("no_of_months_btw_dates",
//      months_between(col("end_date"),col("start_date"))
//    )
//
//    df1.show()

    //---------------------------------------------------------------
    //9. Get the week number from a date column.
    //o Sample Data:
    // Scala: Seq(("2024-08-15"), ("2024-08-21")).toDF("date")
    // PySpark: data = [("2024-08-15",), ("2024-08-21",)]

//    val data = Seq(("2024-08-15"), ("2024-08-21")).toDF("date")
//
//    val df1 = data.withColumn("week_number",
//      weekofyear(col("date"))
//    )
//
//    df1.show()

    //---------------------------------------------------------------
    //10. Format a date column to "dd-MM-yyyy" format.
    //o Sample Data:
    // Scala: Seq(("2024-09-01"), ("2024-09-10")).toDF("date")
    // PySpark: data = [("2024-09-01",), ("2024-09-10",)]

//    val data = Seq(("2024-09-01"), ("2024-09-10")).toDF("date")
//
//    val df1 = data.withColumn("new_date_format",
//      date_format(col("date"),"dd-MM-yyyy")
//    )
//
//    df1.show()

    //---------------------------------------------------------------
    //11.Find if a given date falls on a weekend.
    //o Sample Data:
    // Scala: Seq(("2024-10-12"), ("2024-10-13")).toDF("date")
    // PySpark: data = [("2024-10-12",), ("2024-10-13",)]

//    val data = Seq(("2024-10-12"), ("2024-10-13")).toDF("date")
//
//    val df1 = data.withColumn("date_falls_on_weekend?",
//      when(dayofweek(col("date")).isin(1,7),"Yes")
//        .otherwise("No")
//    ).withColumn("day_of_week",
//      date_format(col("date"),"EEEE")
//    ).withColumn("day_of_week_number",
//      dayofweek(col("date")))
//
//    df1.show()

    //---------------------------------------------------------------
    //11. Check if the date is in a leap year.
    //o Sample Data:
    // Scala: Seq(("2024-02-29"), ("2023-02-28")).toDF("date")
    // PySpark: data = [("2024-02-29",), ("2023-02-28",)]

//    val data = Seq(("2024-02-29"), ("2023-02-28")).toDF("date")
//
//    val df1 = data.withColumn("is_date_in_leap_year?",
//      when((year(col("date"))%4)===0,"Yes")
//        .otherwise("No")
//    )
//
//    df1.show()

    //---------------------------------------------------------------
    //12. Extract the quarter (1-4) from a date column.
    //o Sample Data:
    // Scala: Seq(("2024-03-15"), ("2024-06-20")).toDF("date")
    // PySpark: data = [("2024-03-15",), ("2024-06-20",)]

//    val data = Seq(
//      ("2024-02-15"),
//      ("2024-05-20"),
//      ("2024-07-20"),
//      ("2024-11-20")).toDF("date")
//
//    val df1 = data.withColumn("quarter",
//      ceil(month(col("date"))/3)
//    )
//
//    df1.show()

    //---------------------------------------------------------------
    //13. Display the month as an abbreviation (e.g., "Mar" for March).
    //o Sample Data:
    // Scala: Seq(("2024-03-10"), ("2024-04-15")).toDF("date")
    // PySpark: data = [("2024-03-10",), ("2024-04-15",)]

//    val data = Seq(("2024-03-10"), ("2024-04-15")).toDF("date")
//
//    val df1 = data.withColumn("quarter",
//      ceil(month(col("date"))/3)
//    )
//
//    df1.show()

    //---------------------------------------------------------------
    //14. Add 5 months to a date column.
    //o Sample Data:
    // Scala: Seq(("2024-01-01"), ("2024-02-15")).toDF("date")
    // PySpark: data = [("2024-01-01",), ("2024-02-15",)]

//    val data = Seq(("2024-01-01"), ("2024-02-15")).toDF("date")
//
//    val df1 = data.withColumn("add_5_months",
//      add_months(col("date"),5)
//    )
//
//    df1.show()

    //---------------------------------------------------------------
    //15. Subtract 1 year from a date column.
    //o Sample Data:
    // Scala: Seq(("2024-08-10"), ("2025-10-20")).toDF("date")
    // PySpark: data = [("2024-08-10",), ("2025-10-20",)]

//    val data = Seq(("2024-08-10"), ("2025-10-20")).toDF("date")
//
//    val df1 = data.withColumn("sub_1_year",
//      (date_sub(col("date"),365))
//    )
//
//    df1.show()

    //---------------------------------------------------------------

//    Want to understand this problem in a better way

    //16. Round a timestamp column to the nearest hour.
    //o Sample Data:
    // Scala: Seq(("2024-03-10 12:25:30"), ("2024-03-10
    //12:55:45")).toDF("timestamp")
    // PySpark: data = [("2024-03-10 12:25:30",), ("2024-03-10 12:55:45",)]

//    val data = Seq(
//      ("2024-03-10 12:25:30"),
//      ("2024-03-10 12:55:45"))
//      .toDF("timestamp")
//
//    val df1 = data
//      .withColumn("timestamp", to_timestamp(col("timestamp")))
//      .withColumn("round_to_nearest_hour",
//        when(minute(col("timestamp")) >= 30,
//          date_add(date_trunc("hour", col("timestamp")), 1))
//        .otherwise(date_trunc("hour", col("timestamp"))))
//
//    df1.show()


    //---------------------------------------------------------------
    //17. Calculate the date 100 days after a given date.
    //o Sample Data:
    // Scala: Seq(("2024-04-01"), ("2024-05-10")).toDF("date")
    // PySpark: data = [("2024-04-01",), ("2024-05-10",)]


//    val data = Seq(("2024-04-01"), ("2024-05-10")).toDF("date")
//
//    val df1 = data.withColumn("post_100_day_date",
//        date_add(col("date"),100)
//      )
//
//    df1.show()

    //---------------------------------------------------------------
    //18. Calculate the difference in weeks between two dates.
    //o Sample Data:
    // Scala: Seq(("2024-01-01", "2024-02-01"), ("2024-03-01", "2024-04-
    //01")).toDF("start_date", "end_date")
    // PySpark: data = [("2024-01-01", "2024-02-01"), ("2024-03-01", "2024-04-
    //01")]

//    val data = Seq(
//      ("2024-01-01", "2024-02-01"),
//      ("2024-03-01", "2024-04-01"))
//      .toDF("start_date", "end_date")
//
//    val df1 = data.withColumn("start_date_week",
//      weekofyear(col("start_date"))
//    ).withColumn("end_date_week",
//      weekofyear(col("end_date"))
//    ).withColumn("week_difference_btw_dates",
//      (col("end_date_week") - col("start_date_week"))
//    )
//
//    df1.show()

    //---------------------------------------------------------------

    //19. Check if a date falls in the current year.
    //o Sample Data:
    // Scala: Seq(("2024-01-15"), ("2023-12-25")).toDF("date")
    // PySpark: data = [("2024-01-15",), ("2023-12-25",)]

//    val data = Seq(("2024-01-15"), ("2023-12-25")).toDF("date")
//
//    val df1 = data.withColumn("date_falls_in_current_year?",
//      when(year(col("date")) === year(current_date()),"Yes")
//        .otherwise("No")
//    )
//
//    df1.show()

    //---------------------------------------------------------------

    //20. Convert a date to timestamp and format it as "yyyy-MM-dd HH:mm".
    //o Sample Data:
    // Scala: Seq(("2024-09-30"), ("2024-10-01")).toDF("date")
    // PySpark: data = [("2024-09-30",), ("2024-10-01",)]

//    val data = Seq(("2024-09-30"), ("2024-10-01")).toDF("date")
//
//    val df1 = data.withColumn("updated_format",
//      date_format(to_timestamp(col("date")),"yyyy-MM-dd HH:mm")
//    )
//
//    df1.show()

    //---------------------------------------------------------------
    //21. Find the date of the next Sunday for a given date.
    //o Sample Data:
    // Scala: Seq(("2024-10-10"), ("2024-10-15")).toDF("date")
    // PySpark: data = [("2024-10-10",), ("2024-10-15",)]

//    val data = Seq(
//      ("2024-09-30"),
//      ("2024-11-19"),
//      ("2024-11-05"),
//      ("2024-10-01"))
//      .toDF("date")
//
//    val df1 = data.withColumn("date_of_next_sunday",
//      when(dayofweek(col("date"))===1,date_add(col("date"),13))
//        .when(dayofweek(col("date"))===2,date_add(col("date"),12))
//        .when(dayofweek(col("date"))===3,date_add(col("date"),11))
//        .when(dayofweek(col("date"))===4,date_add(col("date"),10))
//        .when(dayofweek(col("date"))===5,date_add(col("date"),9))
//        .when(dayofweek(col("date"))===6,date_add(col("date"),8))
//        .when(dayofweek(col("date"))===7,date_add(col("date"),7))
//        .otherwise("NA")
//      )
//
//    df1.show()

    //---------------------------------------------------------------
    //22. Display the date as "MMM dd, yyyy" format.
    //o Sample Data:
    // Scala: Seq(("2024-11-15"), ("2024-12-20")).toDF("date")
    // PySpark: data = [("2024-11-15",), ("2024-12-20",)]

//    val data = Seq(("2024-11-15"), ("2024-12-20")).toDF("date")
//
//    val df1 = data.withColumn("date_updated_format",
//      date_format(col("date"),"MMM dd, yyyy")
//    )
//
//    df1.show()

    //---------------------------------------------------------------

    //Need more clarity around how to approach this problem

    //23. Calculate the date 30 business days after a given date.
    //o Sample Data:
    // Scala: Seq(("2024-01-01"), ("2024-02-10")).toDF("date")
    // PySpark: data = [("2024-01-01",), ("2024-02-10",)]



    //---------------------------------------------------------------



  }
}
