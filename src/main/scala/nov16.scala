import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, collect_list, count, countDistinct, current_date, date_sub, initcap, max, min, sum, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, LogManager,Logger}

object nov16 {
  def main(args: Array[String]): Unit ={

    val logger: Logger = Logger.getLogger(this.getClass)

    val conf = new SparkConf()
    conf.set("spark.app.name","kushagra-spark-program")
    conf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

//    SPARK_PROBLEMS_FOR_DATE_MANIPULATION_PRACTICE_Assignment 4

//Sample Data for All Questions
    //PySpark List:
    //data = [
    //("Karthik", "Sales", 2023, 1200),
    //("Ajay", "Marketing", 2022, 2000),
    //("Vijay", "Sales", 2023, 1500),
    //("Mohan", "Marketing", 2022, 1500),
    //("Veer", "Sales", 2021, 2500),
    //("Ajay", "Finance", 2023, 1800),
    //("Kiran", "Sales", 2023, 1200),
    //("Priya", "Finance", 2023, 2200),
    //("Karthik", "Sales", 2022, 1300),
    //Seekho Bigdata Institute
    //("Ajay", "Marketing", 2023, 2100),
    //("Vijay", "Finance", 2022, 2100),
    //("Kiran", "Marketing", 2023, 2400),
    //("Mohan", "Sales", 2022, 1000)
    //]

    //Scala Spark List:
    val data = Seq(
    ("Karthik", "Sales", 2023, 1200),
    ("Ajay", "Marketing", 2022, 2000),
//("BAjay", "Marketing", 2022, 2000),
//("CAjay", "Marketing", 2022, 2000),
//("ZAjay", "Marketing", 2022, 2000),
    ("Vijay", "Sales", 2023, 1500),
    ("Mohan", "Marketing", 2022, 1500),
    ("Veer", "Sales", 2021, 2500),
    ("Ajay", "Finance", 2023, 1800),
    ("Kiran", "Sales", 2023, 1200),
    ("Priya", "Finance", 2023, 2200),
    ("Karthik", "Sales", 2022, 1300),
    ("Ajay", "Marketing", 2023, 2100),
    ("Vijay", "Finance", 2022, 2100),
    ("Kiran", "Marketing", 2023, 2400),
    ("Mohan", "Sales", 2022, 1000)
    ).toDF("name","department","year","salary")

    //Questions
    //---------------------------------------------------------------
    //1. For each department, assign ranks to employees based on their
    // salary in descending order for each year.

//    val window = Window.partitionBy(col("department"),col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.withColumn("rank_wrt_Department&Year",
//      rank().over(window)
//    ).orderBy(col("year"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------

    //2. In the "Sales" department, rank employees based on their salary.
    // If two employees have the same salary, they should share the same rank.

//    val window = Window.partitionBy(col("department"),col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.withColumn("Sales_rank_wrt_Department&Year",
//      rank().over(window)
//    ).filter(col("department")==="Sales").orderBy(col("year"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------

    //3. For each department, assign a row number to each employee based
    // on the year, ordered by salary in descending order.

//    val window = Window.partitionBy(col("department"),col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.withColumn("row_number_wrt_Department&Year",
//      row_number().over(window)
//    ).orderBy(col("year"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //4. Rank employees across all departments based on their salary
    // within each year.

//    val window = Window.partitionBy(col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.withColumn("rank_wrt_all_Department&Year",
//      rank().over(window)
//    ).orderBy(col("year"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //5. Rank employees by their salary in descending order.
    // If multiple employees have the same
    //salary, ensure they receive unique rankings without gaps.

//    val df1 = data.withColumn("rank_wrt_Salary",
//      row_number().over(Window.orderBy(col("salary").desc))
//    ).orderBy(col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //6. For each year, rank employees in the "Marketing" department
    // based on salary, but without
    //any gaps in ranks if salaries are tied.

//    val window = Window.partitionBy(col("department"),col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.withColumn("rank_wrt_Salary",
//      dense_rank().over(window)
//    )
//      .filter(col("department")==="Marketing")
//      .orderBy(col("year"),col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------

    //7. For each year, assign row numbers to employees based on salary
    // in ascending order within each department.

//      val window = Window.partitionBy(col("department"),col("year"))
//        .orderBy(col("salary"))
//
//      val df1 = data.withColumn("rank_wrt_Salary",
//          row_number().over(window)
//        )
//        .orderBy(col("year"),col("department"),col("rank_wrt_Salary"))
//
//      data.show()
//      df1.show()

    //---------------------------------------------------------------
    //8. Within each department, assign a dense rank to employees based
    // on their salary for each year.

//    val window = Window.partitionBy(col("department"),col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.withColumn("rank_wrt_Salary",
//        dense_rank().over(window)
//      )
//      .orderBy(col("year"),col("department"),col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //9. Identify the top 3 highest-paid employees in each department
    // for the year 2023 using any ranking function.

//    val window = Window.partitionBy(col("department"),col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.filter(col("year")===2023)
//      .withColumn("rank_wrt_Salary",
//        row_number().over(window)
//      )
//      .filter(col("rank_wrt_Salary") <=3)
//      .orderBy(col("year"),col("department"),col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //10. For the "Finance" department, list employees in descending order
    // of salary, showing their relative ranks without any gaps.

//    val window = Window.partitionBy(col("department"),col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.filter(col("department")==="Finance")
//      .withColumn("rank_wrt_Salary",
//        dense_rank().over(window)
//      )
//      .orderBy(col("year"),col("department"),col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //11. Rank employees across departments based on their salary,
    // grouping by year, and sort by department as the secondary sorting criteria.

//    val window = Window.partitionBy(col("department"),col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.withColumn("rank_wrt_Salary",
//        dense_rank().over(window)
//      )
//      .orderBy(col("year"),col("department"),col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //12. In each department, assign a rank to employees based on their
    // salaries within each year. If two employees have the same salary,
    // they should get the same rank without any gaps.

//    val window = Window.partitionBy(col("department"),col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.withColumn("rank_wrt_Salary",
//        dense_rank().over(window)
//      )
//      .orderBy(col("year"),col("department"),col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //13. For each department, assign row numbers to employees based on year,
    // ordering by salary in descending order.

//    val window = Window.partitionBy(col("department"),col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.withColumn("rank_wrt_Salary",
//        row_number().over(window)
//      )
//      .orderBy(col("year"),col("department"),col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //14. Find the lowest-ranked employees in each department
    // for the year 2022 based on salary.

//    val window = Window.partitionBy(col("department"),col("year"))
//      .orderBy(col("salary"))
//
//    val df1 = data.filter(col("year")===2022)
//      .withColumn("rank_wrt_Salary",
//        dense_rank().over(window)
//      )
//      .filter(col("rank_wrt_Salary")===1)
//      .orderBy(col("year"),col("department"),col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //15. In each department, rank employees based on salary and year.
    // If employees have the same salary, they should share the same rank.

//    val window = Window.partitionBy(col("department"),col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.withColumn("rank_wrt_Salary",
//        dense_rank().over(window)
//      )
//      .orderBy(col("year"),col("department"),col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //16. Assign row numbers to employees across all departments,
    // ordered by salary, with ties in salary broken by
    // alphabetical order of employee names.

//    val window = Window.partitionBy(col("department"),col("year"))
//      .orderBy(col("salary").desc, col("name"))
//
//    val df1 = data.withColumn("rank_wrt_Salary",
//        row_number().over(window)
//      )
//      .orderBy(col("year"),col("department"),col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //17. For each department, assign ranks to employees by year.
    // Use a ranking function that assigns the same rank for ties and has no gaps.

//    val window = Window.partitionBy(col("department"), col("year"))
//      .orderBy(col("salary"))
//
//    val df1 = data.withColumn("rank_wrt_Salary",
//        dense_rank().over(window)
//      )
//      .orderBy(col("department"),col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //18. List employees ranked in descending order of their salaries
    // within each department. If employees have the same salary,
    // they should receive consecutive ranks without gaps.

//    val window = Window.partitionBy(col("department"), col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.withColumn("rank_wrt_Salary",
//        row_number().over(window)
//      )
//      .orderBy(col("year"),col("department"),col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //19. Assign a dense rank to employees in the "Sales" department
    // based on salary across all years.

//    val window = Window.partitionBy(col("department"), col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.filter(col("department")==="Sales")
//      .withColumn("rank_wrt_Salary",
//        dense_rank().over(window)
//      )
//      .orderBy(col("year"),col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //20. For each department and year, assign a row number to employees
    // ordered by salary, showing only the top 2 in each department and year.

//    val window = Window.partitionBy(col("department"), col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.withColumn("rank_wrt_Salary",
//        row_number().over(window)
//      ).filter(col("rank_wrt_Salary")<=2)
//      .orderBy(col("year"), col("department") ,col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //21. In each department, rank employees based on their salary.
    // Show ranks as consecutive numbers even if salaries are tied.

//    val window = Window.partitionBy(col("department"), col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.withColumn("rank_wrt_Salary",
//        dense_rank().over(window)
//      )
//      .orderBy(col("year"), col("department") ,col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //22. List employees with the top 2 highest salaries in the
    // "Finance" department for each year.

//    val window = Window.partitionBy(col("department"), col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.filter(col("department")==="Finance")
//      .withColumn("rank_wrt_Salary",
//        dense_rank().over(window)
//      ).filter(col("rank_wrt_Salary")<=2)
//      .orderBy(col("year"), col("department") ,col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //23. For each department, rank employees based on their salary
    // within each year. Display only employees ranked in the top 3
    // for each department and year.

//    val window = Window.partitionBy(col("department"), col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.withColumn("rank_wrt_Salary",
//        dense_rank().over(window)
//      ).filter(col("rank_wrt_Salary")<=3)
//      .orderBy(col("year"), col("department") ,col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //24. Within the "Marketing" department, assign a row number
    // to employees based on salary in descending order across all years.

//    val window = Window.partitionBy(col("department"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.filter(col("department")==="Marketing")
//      .withColumn("rank_wrt_Salary",
//        row_number().over(window)
//      )
//      .orderBy(col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
    //25. Rank all employees based on their salaries in descending order
    // across departments. Handle ties in a way that avoids gaps between ranks.

//    val window = Window.partitionBy(col("department"), col("year"))
//      .orderBy(col("salary").desc)
//
//    val df1 = data.withColumn("rank_wrt_Salary",
//        row_number().over(window)
//      )
//      .orderBy(col("year"), col("department") ,col("rank_wrt_Salary"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------

  }
}
