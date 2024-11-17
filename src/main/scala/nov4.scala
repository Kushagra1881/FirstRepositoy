import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.And
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, collect_list, count, countDistinct, current_date, date_format, date_sub, datediff, initcap, lag, lead, lit, max, min, round, sum, to_date, to_timestamp, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.runtime.BoxesRunTime.multiply


object nov4 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
              conf.set("spark.app.name","kushagra-spark-program")
              conf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
                .config(conf)
                .getOrCreate()

    import spark.implicits._

    //    SPARK_PROBLEMS_FOR_PRACTICE_Assignment 1

    //---------------------------------------------------------------

    //Question 1: Employee Status Check
    //Create a DataFrame that lists employees with names and their work status.
    // For each employee,
    //determine if they are “Active” or “Inactive” based on the last check-in date.
    // If the check-in date is
    //within the last 7 days, mark them as "Active"; otherwise,
    // mark them as "Inactive." Ensure the first
    //letter of each name is capitalized.
    // Scala Sample Data
//    val employees = List(
//    ("karthik", "2024-11-01"),
//    ("neha", "2024-10-20"),
//    ("priya", "2024-10-28"),
//    ("mohan", "2024-11-02"),
//    ("ajay", "2024-09-15"),
//    ("vijay", "2024-10-30"),
//    ("veer", "2024-10-25"),
//    ("aatish", "2024-10-10"),
//    ("animesh", "2024-10-15"),
//    ("nishad", "2024-11-01"),
//    ("varun", "2024-10-05"),
//    ("aadil", "2024-09-30")
//    ).toDF("name", "last_checkin")
//
//    employees.select(initcap(col("name")).alias("capital_name"),col("last_checkin"),
//      when(col("last_checkin")>(date_sub(current_date,7)),"Active")
//        .otherwise("Inactive").alias("work_status")).show()

//    scala.io.StdIn.readLine()

//---------------------------------------------------------------

//    Question 2: Sales Performance by Agent
//      Given a DataFrame of sales agents with their total sales amounts,
//      calculate the performance status based on sales thresholds:
//      “Excellent” if sales are above 50,000, “Good” if between 25,000 and
//      50,000, and “Needs Improvement” if below 25,000.
//      Capitalize each agent's name, and
//      show total sales aggregated by performance status.
//     Scala Sample Data
//    val sales = List(
//      ("karthik", 60000),
//      ("neha", 48000),
//      ("priya", 30000),
//      ("mohan", 24000),
//      ("ajay", 52000),
//      ("vijay", 45000),
//      ("veer", 70000),
//      ("aatish", 23000),
//      ("animesh", 15000),
//      ("nishad", 8000),
//      ("varun", 29000),
//      ("aadil", 32000)
//    ).toDF("name", "total_sales")
//
//    sales.select(initcap(col("name")).alias("capital_name"),col("total_sales"),
//        when(col("total_sales")>=50000,"Excellent")
//          .when(col("total_sales")>=25000 && col("total_sales")<50000,"Good")
//          .otherwise("Needs Improvement").alias("performance_status")).show
//
//    sales.select(initcap(col("name")).alias("capital_name"),col("total_sales"),
//      when(col("total_sales")>=50000,"Excellent")
//        .when(col("total_sales")>=25000 && col("total_sales")<50000,"Good")
//        .otherwise("Needs Improvement").alias("performance_status"))
//      .groupBy("performance_status").agg(sum(col("total_sales"))).show

    //---------------------------------------------------------------

//    Question 3: Project Allocation and Workload Analysis
//    Given a DataFrame with project allocation data for multiple employees,
//    determine each employee's workload level based on their hours worked in a month
//    across various projects. Categorize employees as “Overloaded”
//    if they work more than 200 hours, “Balanced” if between 100-200 hours,
//    and “Underutilized” if below 100 hours.
//    Capitalize each employee’s name, and show the aggregated
//      workload status count by category.
//     Scala Sample Data

//    val workload = List(
//      ("karthik", "ProjectA", 120),
//      ("karthik", "ProjectB", 100),
//      ("neha", "ProjectC", 80),
//      ("neha", "ProjectD", 30),
//      ("priya", "ProjectE", 110),
//      ("mohan", "ProjectF", 40),
//      ("ajay", "ProjectG", 70),
//      ("vijay", "ProjectH", 150),
//      ("veer", "ProjectI", 190),
//      ("aatish", "ProjectJ", 60),
//      ("animesh", "ProjectK", 95),
//      ("nishad", "ProjectL", 210),
//      ("varun", "ProjectM", 50),
//      ("aadil", "ProjectN", 90)
//      ).toDF("name", "project", "hours")
//
//    workload.select(initcap(col("name")).alias("capital_name"),
//      when(col("hours")>=200,"Overloaded")
//      .when(col("hours")>=100 && col("hours")<200,"Balanced")
//      .otherwise("Underutilized").alias("workload_status")).show()
//
//    workload.groupBy("name").agg(sum(col("hours")).alias("total_hours")).show()


    //---------------------------------------------------------------

//    5. Overtime Calculation for Employees
//    Determine whether an employee has "Excessive Overtime" if their weekly hours
    //    exceed 60, "Standard Overtime" if between 45-60 hours,
    //    and "No Overtime" if below 45 hours.
    //    Capitalize each name and group by overtime status.
//     Scala Spark Data

//    val employees = List(
//      ("karthik", 62),
//      ("neha", 50),
//      ("priya", 30),
//      ("mohan", 65),
//      ("ajay", 40),
//      ("vijay", 47),
//      ("veer", 55),
//      ("aatish", 30),
//      ("animesh", 75),
//      ("nishad", 60)
//    ).toDF("name", "hours_worked")
//
//   val df1= employees.select(initcap(col("name")).alias("capital_name"),
//      when(col("hours_worked")>=60,"Excessive Overtime")
//        .when(col("hours_worked")>=45 && col("hours_worked")<60,"Standard Overtime")
//        .otherwise("No Overtime").alias("workload_status"))
//
//    val df2= df1.groupBy("workload_status").agg(collect_list("capital_name"))
//      .alias("total_hours")
//
//    df1.show()
//    df2.show()
//    scala.io.StdIn.readLine()

    //---------------------------------------------------------------

//    6. Customer Age Grouping
//      Group customers as "Youth" if under 25,
//      "Adult" if between 25-45, and "Senior" if over 45. Capitalize
//      names and show total customers in each group.
//     Scala Spark Data
//    val customers = List(
//      ("karthik", 22),
//      ("neha", 28),
//      ("priya", 40),
//      ("mohan", 55),
//      ("ajay", 32),
//      ("vijay", 18),
//      ("veer", 47),
//      ("aatish", 38),
//      ("animesh", 60),
//      ("nishad", 25)
//    ).toDF("name", "age")
//
//    val df1= customers.select(initcap(col("name")).alias("capital_name"),col("age"),
//      when(col("age")<=25,"Youth")
//        .when(col("age")>25 && col("age")<=45,"Adult")
//        .otherwise("Senior").alias("age_group_status"))
//
//    val df2= df1.groupBy("age_group_status").agg(collect_list("capital_name"))
//      .alias("total_customers")
//
//    df1.show()
//    df2.show()
    //---------------------------------------------------------------
//    7. Vehicle Mileage Analysis
//      Classify each vehicle’s mileage as "High Efficiency"
    //      if mileage is above 25 MPG, "Moderate Efficiency"
//    if between 15-25 MPG, and "Low Efficiency" if below 15 MPG.
//     Scala Spark Data
//    val vehicles = List(
//      ("CarA", 30),
//      ("CarB", 22),
//      ("CarC", 18),
//      ("CarD", 15),
//      ("CarE", 10),
//      ("CarF", 28),
//      ("CarG", 12),
//      ("CarH", 35),
//      ("CarI", 25),
//      ("CarJ", 16)
//    ).toDF("vehicle_name", "mileage")
//
//    val df1= vehicles.select(col("vehicle_name"),col("mileage"),
//      when(col("mileage")>=25,"High Efficiency")
//        .when(col("mileage")>=15 && col("mileage")<25,"Moderate Efficiency")
//        .otherwise("Low Efficiency").alias("mileage_status"))
//
//    val df2= df1.groupBy("mileage_status").agg(collect_list("vehicle_name")
//      .alias("total_vehicle_name"))
//
//    df1.show()
//    df2.show()

    //---------------------------------------------------------------

//    8. Student Grade Classification
//      Classify students based on their scores as "Excellent"
//      if score is 90 or above, "Good" if between 75-
//      89, and "Needs Improvement" if below 75. Count students in each category.
//     Scala Spark Data
//    val students = List(
//      ("karthik", 95),
//      ("neha", 82),
//      ("priya", 74),
//      ("mohan", 91),
//      ("ajay", 67),
//      ("vijay", 80),
//      ("veer", 85),
//      ("aatish", 72),
//      ("animesh", 90),
//      ("nishad", 60)
//    ).toDF("name", "score")
//
//    val df1= students.select(col("name"),col("score"),
//      when(col("score")>=90,"Excellent")
//        .when(col("score")>=75 && col("score")<=89,"Good")
//        .otherwise("Needs Improvement").alias("score_status"))
//
//    val df2= df1.groupBy("score_status").agg(count("name")
//      .alias("total_student_count"))
//
//    df1.show()
//    df2.show()

    //---------------------------------------------------------------

//    9. Product Inventory Check
//      Classify inventory stock levels as "Overstocked"
//      if stock exceeds 100, "Normal" if between 50-100,
//    and "Low Stock" if below 50. Aggregate total stock in each category.
//     Scala Spark Data
//    val inventory = List(
//      ("ProductA", 120),
//      ("ProductB", 95),
//      ("ProductC", 45),
//      ("ProductD", 200),
//      ("ProductE", 75),
//      ("ProductF", 30),
//      ("ProductG", 85),
//      ("ProductH", 100),
//      ("ProductI", 60),
//      ("ProductJ", 20)
//    ).toDF("product_name", "stock_quantity")
//
//    val df1= inventory.select(col("product_name"),col("stock_quantity"),
//      when(col("stock_quantity")>=100,"Overstocked")
//        .when(col("stock_quantity")>=50 && col("stock_quantity")<=100,"Normal")
//        .otherwise("Low Stock").alias("stock_status"))
//
//    val df2= df1.groupBy("stock_status").agg(sum("stock_quantity")
//      .alias("total_stock_quantity"),count("stock_quantity"))
//
//    df1.show()
//    df2.show()

    //---------------------------------------------------------------


//    10. Employee Bonus Calculation Based on Performance and Department
//    Classify employees for a bonus eligibility program. Employees in
//    "Sales" and "Marketing" with
//    performance scores above 80 get a 20% bonus, while others with
//    scores above 70 get 15%. All other
//      employees receive no bonus. Group by department and calculate
//      total bonus allocation.
//     Scala Spark Data
//    val employees = List(
//      ("karthik", "Sales", 85),
//      ("neha", "Marketing", 78),
//      ("priya", "IT", 90),
//      ("mohan", "Finance", 65),
//      ("ajay", "Sales", 55),
//      ("vijay", "Marketing", 82),
//      ("veer", "HR", 72),
//      ("aatish", "Sales", 88),
//      ("animesh", "Finance", 95),
//      ("nishad", "IT", 60)
//    ).toDF("name", "department", "performance_score")
//
//    val df1= employees.select(col("name"),col("department"),col("performance_score"),
//      when(
//        col("performance_score")>=80 &&
//          (col("department")==="Marketing" ||
//            col("department")==="Sales")
//        ,"20% bonus")
//        .when(
//          col("performance_score")>=70,"15% bonus")
//        .otherwise("No bonus")
//      .alias("bonus_status"))
//
//    val df2= df1.groupBy("department").agg(avg("performance_score")
//      .alias("avg_performance_score"),count("performance_score"),
//      count(col("bonus_status")))
//
//    df1.show()
//    df2.show()

    //---------------------------------------------------------------

//    11. Product Return Analysis with Multi-Level Classification
//      For each product, classify return reasons as "High Return Rate"
//      if return count exceeds 100 and
//      satisfaction score below 50, "Moderate Return Rate"
//      if return count is between 50-100 with a score
//      between 50-70, and "Low Return Rate" otherwise.
//      Group by category to count product return rates.
//     Scala Spark Data
//    val products = List(
//      ("Laptop", "Electronics", 120, 45),
//      ("Smartphone", "Electronics", 80, 60),
//      ("Tablet", "Electronics", 50, 72),
//      ("Headphones", "Accessories", 110, 47),
//      ("Shoes", "Clothing", 90, 55),
//      ("Jacket", "Clothing", 30, 80),
//      ("TV", "Electronics", 150, 40),
//      ("Watch", "Accessories", 60, 65),
//      ("Pants", "Clothing", 25, 75),
//      ("Camera", "Electronics", 95, 58)
//      ).toDF("product_name", "category", "return_count", "satisfaction_score")
//
//    val df1= products.select(
//      col("product_name"),
//      col("category"),
//      col("return_count"),
//      col("satisfaction_score"),
//      when(
//        col("return_count")>=100 &&
//          (col("satisfaction_score")<50),"High Return Rate")
//        .when(
//          (col("return_count") between (50,100)) &&
//            (col("satisfaction_score") between(50,70)),"Moderate Return Rate")
//        .otherwise("Low Return Rate")
//        .alias("return_reason"))
//
//    val df2= df1.groupBy("category").agg(
//      round(avg("return_count"),2)
//      .alias("avg_return_count"),
//      round(avg("satisfaction_score"),2).alias("avg_satisfaction_score"))
//
//    df1.show()
//    df2.show()

    //---------------------------------------------------------------
//    12. Customer Spending Pattern Based on Age and Membership Level
//      Classify customers' spending as "High Spender"
    //      if spending exceeds $1000 with "Premium"
//    membership, "Average Spender" if spending between
    //    $500-$1000 and membership is "Standard",
//    and "Low Spender" otherwise. Group by membership
    //    and calculate average spending.
//     Scala Spark Data
//    val customers = List(
//      ("karthik", "Premium", 1050, 32),
//      ("neha", "Standard", 800, 28),
//      ("priya", "Premium", 1200, 40),
//      ("mohan", "Basic", 300, 35),
//      ("ajay", "Standard", 700, 25),
//      ("vijay", "Premium", 500, 45),
//      ("veer", "Basic", 450, 33),
//      ("aatish", "Standard", 600, 29),
//      ("animesh", "Premium", 1500, 60),
//      ("nishad", "Basic", 200, 21)
//    ).toDF("name", "membership", "spending", "age")
//
//    val df1= customers.select(
//      col("name"),
//      col("membership"),
//      col("spending"),
//      col("age"),
//      when(
//        col("spending")>=1000 &&
//          (col("membership")==="Premium"),"High Spender")
//        .when(
//          (col("spending") between (500,1000)) &&
//            (col("membership")==="Standard"),"Average Spender")
//        .otherwise("Low Spender")
//        .alias("customer_spending_type"))
//
//    val df2= df1.groupBy("membership")
//      .agg(
//      round(avg("spending"),2)
//      .alias("avg_spending")
//      )
//
//    df1.show()
//    df2.show()


    //---------------------------------------------------------------

//    13. E-commerce Order Fulfillment Timeliness Based on Product Type and Location
//      Classify orders as "Delayed" if delivery time exceeds 7 days
    //      and origin location is "International",
//    "On-Time" if between 3-7 days, and "Fast" if below 3 days.
    //    Group by product type to see the count of
//      each delivery speed category.
//     Scala Spark Data
//    val orders = List(
//      ("Order1", "Laptop", "Domestic", 2),
//      ("Order2", "Shoes", "International", 8),
//      ("Order3", "Smartphone", "Domestic", 3),
//      ("Order4", "Tablet", "International", 5),
//      ("Order5", "Watch", "Domestic", 7),
//      ("Order6", "Headphones", "International", 10),
//      ("Order7", "Camera", "Domestic", 1),
//      ("Order8", "Shoes", "International", 9),
//      ("Order9", "Laptop", "Domestic", 6),
//      ("Order10", "Tablet", "International", 4)
//    ).toDF("order_id", "product_type", "origin", "delivery_days")
//
//    val df1= orders.select(
//      col("order_id"),
//      col("product_type"),
//      col("origin"),
//      col("delivery_days"),
//      when(
//        col("delivery_days")>=7 &&
//          (col("origin")==="International"),"Delayed")
////        .when(
////          (col("delivery_days") between (3,7)),"On-Time")
//        .when(
//          (col("delivery_days")>=3 && col("delivery_days")<7),"On-Time")
//        .otherwise("Fast")
//        .alias("fulfillment_status"))
//
//    val df2= df1.groupBy("product_type")
//      .agg(
//        round(avg("delivery_days"),2)
//          .alias("avg_delivery_days")
//      )
//
//    df1.show()
//    df2.show()


//-------------------------------------------------------------------
//
//    1. we want to find the difference between the price on each day with it’s previous day.
//      +-----+-------+------+----------+
//    |IT_ID|IT_Name| Price| PriceDate|
//    +-----+-------+------+----------+

//    val kitkat = List(
//      (1,"KitKat",1000.0,"2021-01-01"),
//      (1,"KitKat",2000.0,"2021-01-02"),
//      (1,"KitKat",1000.0,"2021-01-03"),
//      (1,"KitKat",2000.0,"2021-01-04"),
//      (1,"KitKat",3000.0,"2021-01-05"),
//      (1,"KitKat",1000.0,"2021-01-06")
//    ).toDF("IT_ID","IT_Name","Price","PriceDate")
//
//    val window = Window.orderBy(col("PriceDate"))
//
//    val df1 = kitkat.withColumn("lag_column",
//      lag(col("Price"),1).over(window))
//
//    val df2 = kitkat.withColumn("price_difference_btw_days",
//      col("Price")-lag(col("Price"),1).over(window))
//
//
//    df1.show()
//    df2.show()



    //---------------------------------------------------------------

//    2. If salary is less than previous month we will mark it as "DOWN",
//    if salary has increased then "UP"

//    val john_salary = List(
//      (1,"John",1000,"01/01/2016"),
//      (1,"John",2000,"02/01/2016"),
//      (1,"John",1000,"03/01/2016"),
//      (1,"John",2000,"04/01/2016"),
//      (1,"John",3000,"05/01/2016"),
//      (1,"John",1000,"06/01/2016")
//    ).toDF("ID","NAME","SALARY","DATE")
//
//    val window = Window.orderBy("DATE")
//
//    val df1 = john_salary.withColumn("consecutive_salary_status",
//      when(col("SALARY")<(lag(col("SALARY"),1).over(window)),"DOWN")
//        .when(col("SALARY")>(lag(col("SALARY"),1).over(window)),"UP")
//        .otherwise("Null")
//    )
//
//    df1.show()

    //---------------------------------------------------------------
//    3) Calculate the lead time for each order within the same customer.
//    +--------+---------+----------+

//    val customer = List(
//      (101,"CustomerA","2023-09-01"),
//      (103,"CustomerA","2023-09-03"),
//      (102,"CustomerB","2023-09-02"),
//      (104,"CustomerB","2023-09-04")
//    ).toDF("order_id", "customer", "order_date")
//
//    val window = Window.partitionBy(col("customer")).orderBy(col("order_date"))
//
//    val df1 = customer.withColumn("lead_time_within_customer",
//      datediff(lead(col("order_date"),1).over(window),col("order_date")))
//
//    df1.show()

//    val kitkat = List(
//      (1,"KitKat",1000.0,"2021-01-01"),
//      (1,"KitKat",2000.0,"2021-01-02"),
//      (1,"KitKat",1000.0,"2021-01-03"),
//      (1,"KitKat",2000.0,"2021-01-04"),
//      (1,"KitKat",3000.0,"2021-01-05"),
//      (1,"KitKat",1000.0,"2021-01-06")
//    ).toDF("IT_ID","IT_Name","Price","PriceDate")
//
//    kitkat.createOrReplaceTempView("sales")
//
//    spark.sql(
//
//    """
//       select IT_ID,IT_Name,Price,PriceDate,
//       Price - lead(Price,1) over(order by price) as status
//       from sales
//    """
//    ).show()
//
    //---------------------------------------------------------------

    //    Scenario 14: Financial Risk Level Classification for Loan Applicants
    //      Question Set:
    //      1. Classify loan applicants as "High Risk" if the loan amount
    //      exceeds twice their income and
    //      credit score is below 600, "Moderate Risk" if the
    //      loan amount is between 1-2 times their
    //    income and credit score between 600-700, and
    //    "Low Risk" otherwise. Find the total count of
    //    each risk level.
    //    2. For applicants classified as "High Risk," calculate the average
    //    loan amount by income range
    //    (e.g., < 50k, 50-100k, >100k).
    //    3. Group by income brackets (<50k, 50-100k, >100k) and
    //    calculate the average credit score for
    //      each risk level. Filter for groups where average credit score is below 650.
    //     Scala Spark Data

//    val loanApplicants = List(
//      ("karthik", 60000, 120000, 590),
//      ("neha", 90000, 180000, 610),
//      ("priya", 50000, 75000, 680),
//      ("mohan", 120000, 240000, 560),
//      ("ajay", 45000, 60000, 620),
//      ("vijay", 100000, 100000, 700),
//      ("veer", 30000, 90000, 580),
//      ("aatish", 85000, 85000, 710),
//      ("animesh", 50000, 100000, 650),
//      ("nishad", 75000, 200000, 540)
//    ).toDF("name", "income", "loan_amount", "credit_score")
//
//    val df1= loanApplicants.select(
//      col("name"),
//      col("income"),
//      col("loan_amount"),
//      col("credit_score"),
//      when(
//        (col("loan_amount")>= (col("income") * 2)) &&
//          (col("credit_score")<=600),"High Risk"
//      )
//        .when(
//          (col("loan_amount") >= col("income")) &&
//            (col("loan_amount") < (col("income") * 2)) &&
//            (col("credit_score").between(600, 700)), "Moderate Risk"
//        )
//        .otherwise("Low Risk").alias("risk_status")
//      )
//
//    val df2= df1.filter(col("risk_status")==="High Risk")
//      .groupBy(
//        when(col("income")<50000,"<50k")
//          .when(col("income") between(50000,100000),"50-100k")
//          .when(col("income")>100000,">100k")
//          .otherwise("null").alias("income_range_status")
//      ).agg(avg(col("loan_amount")).alias("avg_loan_amount"))
//
//
//    val df3= df1.filter(col("credit_score") < 650)
//          .groupBy(
//            when(col("income")<50000,"<50k")
//              .when(col("income") between(50000,100000),"50-100k")
//              .when(col("income")>100000,">100k")
//              .otherwise("null").alias("income_range_status")
//          ).agg(avg(col("credit_score")).alias("avg_credit_score"))
//
//
//    df1.show()
//    df2.show()
//    df3.show()

    //---------------------------------------------------------------

//    Scenario 15: Customer Purchase Recency Categorization
//      Question Set: 4. Categorize customers based on purchase recency:
//      "Frequent" if last purchase within
//    30 days, "Occasional" if within 60 days, and
//    "Rare" if over 60 days. Show the number of each
//    category per membership type.
//    5. Find the average total purchase amount for customers
//    with "Frequent" purchase recency
//    and "Premium" membership.
//    6. For customers with "Rare" recency, calculate
//    the minimum purchase amount across different
//    membership types.
//     Scala Spark Data
//  val customerPurchases = List(
//    ("karthik", "Premium", 50, 5000),
//    ("neha", "Standard", 10, 2000),
//    ("priya", "Premium", 65, 8000),
//    ("mohan", "Basic", 90, 1200),
//    ("ajay", "Standard", 25, 3500),
//    ("vijay", "Premium", 15, 7000),
//    ("veer", "Basic", 75, 1500),
//    ("aatish", "Standard", 45, 3000),
//    ("animesh", "Premium", 20, 9000),
//    ("nishad", "Basic", 80, 1100)
//    ).toDF("name", "membership",
//    "days_since_last_purchase", "total_purchase_amount")
//
//    val df1 = customerPurchases.withColumn(
//      "category",
//      when(
//        col("days_since_last_purchase") < 30, "Frequent"
//      )
//        .when(
//          col("days_since_last_purchase") < 60, "Occasional"
//        )
//        .when(
//          col("days_since_last_purchase") > 60, "Rare"
//        )
//        .otherwise("Null")
//    )
//
//    val df2 = df1.groupBy(col("membership"),col("category")).count()
//      .orderBy(col("count").desc)
//
//    //    Q15-5. Find the average total purchase amount for customers
//    //    with "Frequent" purchase recency
//    //    and "Premium" membership.
//
//    val df3 = df1.filter(col("category")==="Frequent" &&
//      col("membership")==="Premium")
//      .agg(avg(col("total_purchase_amount"))
//      .alias("avg_total_purchase_amount"))
//
//    //    Q15-6. For customers with "Rare" recency, calculate
//    //    the minimum purchase amount across different
//    //    membership types.
//
//    val df4 = df1.filter(col("category")==="Rare")
//      .groupBy(col("membership"))
//      .agg(min(col("total_purchase_amount")))
//
//
//    df1.show()
//    df2.show()
//    df3.show()
//    df4.show()

//    val df5 = df1.groupBy(col("membership")).agg(count(col("name")))
//    df5.show()


    //---------------------------------------------------------------

//    Scenario 16: Electricity Consumption and Rate Assignment
//    Question Set: 7. Classify households into "High Usage"
//    if kWh exceeds 500 and bill exceeds $200,
//    "Medium Usage" for kWh between 200-500 and
//    bill between $100-$200, and "Low Usage"
//    otherwise. Calculate the total number of households in each usage category.
//    8. Find the maximum bill amount for "High Usage" households and
//    calculate the average kWh
//    for "Medium Usage" households.
//    9. Identify households with "Low Usage" but
//    kWh usage exceeding 300. Count such
//    households.
//     Scala Spark Data
//    val electricityUsage = List(
//    ("House1", 550, 250),
//    ("House2", 400, 180),
//    ("House3", 150, 50),
//    ("House4", 500, 200),
//    ("House5", 600, 220),
//    ("House6", 350, 120),
//    ("House7", 100, 30),
//    ("House8", 480, 190),
//    ("House9", 220, 105),
//    ("House10", 150, 60)
//    ).toDF("household", "kwh_usage", "total_bill")
//
//    val df1 = electricityUsage.withColumn("consumption_status",
//      when(col("kwh_usage") > 500 &&
//        col("total_bill") > 200
//        ,"High Usage")
//        .when(col("kwh_usage").between(200,500) &&
//          col("total_bill").between(100,200)
//          ,"Medium Usage")
//        .otherwise("Low Usage")
//    )
//
//    //    Calculate the total number of households in each usage category.
//
//    val df2 = df1.groupBy("consumption_status").count().orderBy(col("count").desc)
//
//
//    //    8. Find the maximum bill amount for "High Usage" households and
//    //    calculate the average kWh
//    //    for "Medium Usage" households.
//
//    val df3 = df1.filter(col("consumption_status")==="High Usage")
//      .agg(max(col("total_bill")))
//
//    val df4 =  df1.filter(col("consumption_status")==="Medium Usage")
//      .agg(avg(col("kwh_usage")))
//
//    //    9. Identify households with "Low Usage" but
//    //    kWh usage exceeding 300. Count such
//    //    households.
//
//    val df5 = df1.filter(col("consumption_status")==="Low Usage"
//      && col("kwh_usage")>300).agg(count(col("household")))
//
//    df1.show()
//    df2.show()
//    df3.show()
//    df4.show()
//    df5.show()
    //---------------------------------------------------------------

//    Scenario 17: Employee Salary Band and Performance Classification
//      Question Set: 10. Classify employees into salary bands:
//      "Senior" if salary > 100k and experience > 10
//    years, "Mid-level" if salary between 50-100k and
//    experience 5-10 years, and "Junior" otherwise.
//    Group by department to find count of each salary band.
//    11. For each salary band, calculate the average performance score.
//    Filter for bands where
//      average performance exceeds 80.
//    12. Find employees in "Mid-level" band with performance
//    above 85 and experience over 7 years.
//     Scala Spark Data
//    val employees = List(
//      ("karthik", "IT", 110000, 12, 88),
//      ("neha", "Finance", 75000, 8, 70),
//      ("priya", "IT", 50000, 5, 65),
//      ("mohan", "HR", 120000, 15, 92),
//      ("ajay", "IT", 45000, 3, 50),
//      ("vijay", "Finance", 80000, 7, 78),
//      ("veer", "Marketing", 95000, 6, 85),
//      ("aatish", "HR", 100000, 9, 82),
//      ("animesh", "Finance", 105000, 11, 88),
//      ("nishad", "IT", 30000, 2, 55)
//    ).toDF("name", "department", "salary", "experience", "performance_score")
//
//    val df1 = employees.withColumn("salary_bands",
//      when(
//        (col("salary") > 100000) && (col("experience") > 10),"Senior"
//      )
//        .when(
//          (col("salary").between(50000,100000)) &&
//            (col("experience").between(5,10)),"Mid-level"
//        )
//        .otherwise("Junior")
//    )
//    //    Group by department to find count of each salary band.
//
//    val df2 = df1.groupBy(col("department"),col("salary_bands"))
//      .agg(count(col("name")).alias("count_of_people"))
//      .orderBy(col("count_of_people").desc)
//
////    11. For each salary band, calculate the average performance score.
////      Filter for bands where average performance exceeds 80.
//
//    val df3 = df1.groupBy(
//      col("salary_bands")
//    ).agg(avg(col("performance_score")).alias("avg_performance_score"))
//      .filter(col("avg_performance_score")>80)
//
////    12. Find employees in "Mid-level" band with performance
////    above 85 and experience over 7 years.
//
//    val df4 = df1.filter(
//      (col("salary_bands")==="Mid-level") &&
//        (col("performance_score")>85) &&
//        (col("experience")>7)
//    )
//
//
//    df1.show()
//    df2.show()
//    df3.show()
//    df4.show()
//    scala.io.StdIn.readLine()


    //    val df1 = (employees.withColumn("salary_bands",
    //      when(
    //        (col("salary") > 100000) && (col("experience") > 10),"Senior"
    //      )
    //        .when(
    //          (col("salary").between(50000,100000)) &&
    //            (col("experience").between(5,10)),"Mid-level"
    //        )
    //        .otherwise("Junior")
    //    ))
    //    .groupBy(col("department"),col("salary_bands"))
    //      .agg(count(col("name")).alias("count_of_people"))
    //      .orderBy(col("count_of_people").desc)
//    df1.show()

    //---------------------------------------------------------------

//    Scenario 18: Product Sales Analysis
//    Question Set:
//    1. Classify products as "Top Seller" if total sales exceed 200,000
//    and discount offered is less than 10%,
//    "Moderate Seller" if total sales are between 100,000 and 200,000,
//    and "Low Seller" otherwise.
//    Count the total number of products in each classification.
//    2. Find the maximum sales value among "Top Seller" products
//    and the minimum discount rate
//    among "Moderate Seller" products.
//    3. Identify products from the "Low Seller" category with a
//    total sales value below 50,000 and
//      discount offered above 15%.
//     Scala Spark Data
//    val productSales = List(
//      ("Product1", 250000, 5),
//      ("Product2", 150000, 8),
//      ("Product3", 50000, 20),
//      ("Product4", 120000, 10),
//      ("Product5", 300000, 7),
//      ("Product6", 60000, 18),
//      ("Product7", 180000, 9),
//      ("Product8", 45000, 25),
//      ("Product9", 70000, 15),
//      ("Product10", 10000, 30)
//    ).toDF("product_name", "total_sales", "discount")
//
//    val df1 = productSales.withColumn("classification",
//      when(
//        (col("total_sales") > 200000) && (col("discount") < 10),"Top Seller"
//      )
//        .when(
//          (col("total_sales").between(100000,200000)),"Moderate Seller"
//        )
//        .otherwise("Low Seller")
//    )
//
//    //    Count the total number of products in each classification.
//    val df2 = df1.groupBy(col("classification")).agg(count(col("product_name"))
//      .alias("count_of_product")).orderBy(col("count_of_product").desc)
//
//    //    2. Find the maximum sales value among "Top Seller" products
//    //    and the minimum discount rate among "Moderate Seller" products.
//
//    val df3 = df1.filter(col("classification")==="Top Seller")
//      .agg(max(col("total_sales")).alias("max_sales_value"))
//
//    val df4 = df1.filter(col("classification")==="Moderate Seller")
//      .agg(min(col("discount")).alias("min_discount_rate"))
//
//    //    3. Identify products from the "Low Seller" category with a
//    //    total sales value below 50,000 and
//    //      discount offered above 15%.
//
//    val df5 = df1.filter(
//      (col("classification")==="Low Seller") &&
//        (col("total_sales")<50000) &&
//        (col("discount")>15)
//    )
//
//
//    df1.show()
//    df2.show()
//    df3.show()
//    df4.show()
//    df5.show()


    //---------------------------------------------------------------
//    Scenario 19: Customer Loyalty Analysis
//    Question Set: 4. Classify customers as "Highly Loyal" if
    //    purchase frequency is greater than 20 times
//      and average spending is above 500, "Moderately Loyal"
    //      if frequency is between 10-20 times, and
//    "Low Loyalty" otherwise. Count customers in each classification.
//    5. Calculate the average spending of "Highly Loyal" customers
    //    and the minimum spending for
//      "Moderately Loyal" customers.
//    6. Identify "Low Loyalty" customers with an average spending
    //    less than 100 and purchase frequency under 5.
//     Scala Spark Data
//    val customerLoyalty = List(
//      ("Customer1", 25, 700),
//      ("Customer2", 15, 400),
//      ("Customer3", 5, 50),
//      ("Customer4", 18, 450),
//      ("Customer5", 22, 600),
//      ("Customer6", 2, 80),
//      ("Customer7", 12, 300),
//      ("Customer8", 6, 150),
//      ("Customer9", 10, 200),
//      ("Customer10", 1, 90)
//    ).toDF("customer_name", "purchase_frequency", "average_spending")
//
//    val df1 = customerLoyalty.withColumn("classification",
//      when(
//        (col("purchase_frequency") > 20) &&
//          (col("average_spending") > 500),"Highly Loyal"
//      )
//        .when(
//          (col("purchase_frequency").between(10,20)),"Moderately Loyal"
//        )
//        .otherwise("Low Loyalty")
//    )
//
//    val df2 = df1.groupBy(col("classification"))
//      .agg(count(col("customer_name")).alias("customer_per_classification"))
//      .orderBy(col("customer_per_classification").desc)
//
//
//    //    5. Calculate the average spending of "Highly Loyal" customers
//    //    and the minimum spending for "Moderately Loyal" customers.
//
//    val df3 = df1.filter(col("classification")==="Highly Loyal")
//      .agg(avg(col("average_spending")).alias("avg_spending_per_customer"))
//
//    val df4 = df1.filter(col("classification")==="Moderately Loyal")
//      .agg(min(col("average_spending")).alias("min_spending_per_customer"))
//
//    //    6. Identify "Low Loyalty" customers with an average spending
//    //    less than 100 and purchase frequency under 5.
//
//    val df5 = df1.filter(
//      (col("classification")==="Low Loyalty") &&
//        (col("average_spending")<100) &&
//        (col("purchase_frequency")<5)
//    )
//
//    df1.show()
//    df2.show()
//    df3.show()
//    df4.show()
//    df5.show()
    //---------------------------------------------------------------
//    Scenario 20: E-commerce Return Rate Analysis
//      Question Set: 7. Classify products by return rate:
    //      "High Return" if return rate is over 20%, "Medium Return"
    //      if return rate is between 10% and 20%,
    //    and "Low Return" otherwise. Count products in each
//    classification.
//    8. Calculate the average sale price for "High Return" products and
    //    the maximum return rate for "Medium Return" products.
//    9. Identify "Low Return" products with a sale price under 50
    //    and return rate less than 5%.
//     Scala Spark Data
//    val ecommerceReturn = List(
//      ("Product1", 75, 25),
//      ("Product2", 40, 15),
//      ("Product3", 30, 5),
//      ("Product4", 60, 18),
//      ("Product5", 100, 30),
//      ("Product6", 45, 10),
//      ("Product7", 80, 22),
//      ("Product8", 35, 8),
//      ("Product9", 25, 3),
//    ("Product10", 90, 12)
//    ).toDF("product_name", "sale_price", "return_rate")
//
//    val df1 = ecommerceReturn.withColumn("classification",
//      when(
//        (col("return_rate") > 20),"High Return"
//      )
//        .when(
//          (col("return_rate").between(10,20)),"Medium Return"
//        )
//        .otherwise("Low Return")
//    )
//
//    val df2 = df1.groupBy(col("classification"))
//      .agg(count(col("product_name")).alias("product_per_classification"))
//      .orderBy(col("product_per_classification").desc)
//
//    //    8. Calculate the average sale price for "High Return" products and
//    //    the maximum return rate for "Medium Return" products.
//
//    val df3 = df1.filter(col("classification")==="High Return")
//      .agg(avg(col("sale_price")).alias("avg_sales_price_wrt_classification"))
//
//    val df4 = df1.filter(col("classification")==="Medium Return")
//      .agg(max(col("return_rate")).alias("max_return_rate_wrt_classification"))
//
//    //    9. Identify "Low Return" products with a sale price under 50
//    //    and return rate less than 5%.
//
//    val df5 = df1.filter(
//      (col("classification")==="Low Return") &&
//        (col("sale_price")<50) &&
//        (col("return_rate")<5)
//    )
//
//    df1.show()
//    df2.show()
//    df3.show()
//    df4.show()
//    df5.show()

    //---------------------------------------------------------------
//    Scenario 21: Employee Productivity Scoring
//    Question Set: 10. Classify employees as "High Performer"
    //    if productivity score > 80 and project count
//      is greater than 5, "Average Performer" if productivity score
    //      is between 60 and 80, and "Low
//    Performer" otherwise. Count employees in each classification.
//    11. Calculate the average productivity score for "High Performer"
    //    employees and the minimum score for "Average Performers."
//    12. Identify "Low Performers" with a productivity score below 50
    //    and project count under 2.
//     Scala Spark Data
//    val employeeProductivity = List(
//      ("Emp1", 85, 6),
//      ("Emp2", 75, 4),
//      ("Emp3", 40, 1),
//      ("Emp4", 78, 5),
//      ("Emp5", 90, 7),
//      ("Emp6", 55, 3),
//      ("Emp7", 80, 5),
//      ("Emp8", 42, 2),
//      ("Emp9", 30, 1),
//      ("Emp10", 68, 4)
//      ).toDF("employee_id", "productivity_score", "project_count")
//
//    val df1 = employeeProductivity.withColumn("classification",
//      when(
//        (col("productivity_score") > 80) &&
//          (col("project_count") > 5),"High Performer"
//      )
//        .when(
//          (col("productivity_score").between(60,80)),"Average Performer"
//        )
//        .otherwise("Low Performer")
//    )
//
//    val df2 = df1.groupBy(col("classification"))
//      .agg(count(col("employee_id")).alias("employee_count_per_classification"))
//      .orderBy(col("employee_count_per_classification").desc)
//
//    //    11. Calculate the average productivity score for "High Performer"
//    //    employees and the minimum score for "Average Performers."
//
//    val df3 = df1.filter(col("classification")==="High Performer")
//      .agg(avg(col("productivity_score"))
//        .alias("avg_productivity_score_wrt_classification"))
//
//    val df4 = df1.filter(col("classification")==="Average Performer")
//      .agg(min(col("productivity_score"))
//        .alias("min_productivity_score_wrt_classification"))
//
//    //    12. Identify "Low Performers" with a productivity score below 50
//    //    and project count under 2.
//
//    val df5 = df1.filter(
//      (col("classification")==="Low Performer") &&
//        (col("productivity_score")<50) &&
//        (col("project_count")<2)
//    )
//
//    df1.show()
//    df2.show()
//    df3.show()
//    df4.show()
//    df5.show()
//    scala.io.StdIn.readLine()

    //---------------------------------------------------------------

//    Scenario 22: Banking Fraud Detection
//    Question Set:
//      1. Classify transactions as "High Risk" if the transaction amount
    //      is above 10,000 and frequency
//    of transactions from the same account within a day exceeds 5,
    //    "Moderate Risk" if the
//    amount is between 5,000 and 10,000 and
    //    frequency is between 2 and 5, and "Low Risk"
//    otherwise. Calculate the total number of transactions in each risk level.
//    2. Identify accounts with at least one "High Risk" transaction and
    //    the total amount transacted by those accounts.
//    3. Find all "Moderate Risk" transactions where the account type
    //    is "Savings" and the amount is above 7,500.
//     Scala Spark Data
//    val transactions = List(
//      ("Account1", "2024-11-01", 12000, 6, "Savings"),
//      ("Account2", "2024-11-01", 8000, 3, "Current"),
//      ("Account3", "2024-11-02", 2000, 1, "Savings"),
//      ("Account4", "2024-11-02", 15000, 7, "Savings"),
//      ("Account5", "2024-11-03", 9000, 4, "Current"),
//      ("Account6", "2024-11-03", 3000, 1, "Current"),
//      ("Account7", "2024-11-04", 13000, 5, "Savings"),
//      ("Account8", "2024-11-04", 6000, 2, "Current"),
//      ("Account9", "2024-11-05", 20000, 8, "Savings"),
//      ("Account10", "2024-11-05", 7000, 3, "Savings")
//    ).toDF("account_id", "transaction_date", "amount", "frequency", "account_type")
//
//    //      1. Classify transactions as "High Risk" if the transaction amount
//    //      is above 10,000 and frequency of transactions from the same account
//    //      within a day exceeds 5, "Moderate Risk" if the amount is between
//    //      5,000 and 10,000 and frequency is between 2 and 5, and
//    //      "Low Risk" otherwise.
//    //      Calculate the total number of transactions in each risk level.
//
//    val df1 = transactions.withColumn("classification",
//      when(
//        (col("amount") > 10000) &&
//          (col("frequency") > 5),"High Risk"
//      )
//        .when(
//          (col("amount").between(5000,10000)) &&
//            (col("frequency").between(2,5)),"Moderate Risk"
//        )
//        .otherwise("Low Risk")
//    )
//
//    val df2 = df1.groupBy(col("classification"))
//      .agg(count(col("account_id")).alias("no_of_tran_per_classification"))
//      .orderBy(col("no_of_tran_per_classification").desc)
//
//    //    2. Identify accounts with at least one "High Risk" transaction and
//    //    the total amount transacted by those accounts.
//
//    val df3 = df1.filter(col("classification")==="High Risk")
//      .alias("atleastone_High_Risk_Transaction")
//
//    val df4 = df1.filter(col("classification")==="High Risk")
//      .agg(sum(col("amount"))
//        .alias("sum_of_atleastone_High_Risk_Transaction"))
//
//    //    3. Find all "Moderate Risk" transactions where the account type
//    //    is "Savings" and the amount is above 7,500.
//
//    val df5 = df1.filter(
//      (col("classification")==="Moderate Risk") &&
//        (col("account_type")==="Savings") &&
//        (col("amount")>7500)
//    )
//
//    df1.show()
//    df2.show()
//    df3.show()
//    df4.show()
//    df5.show()
//    scala.io.StdIn.readLine()

    //---------------------------------------------------------------

//    Scenario 23: Hospital Patient Readmission Analysis
//      Question Set: 4. Classify patients as "High Readmission Risk"
//      if their last readmission interval (in days) is
//      less than 15 and their age is above 60,
//      "Moderate Risk" if the interval is between 15 and 30
//    days, and "Low Risk" otherwise. Count patients in each category.
//    5. Find the average readmission interval for "High Readmission Risk" patients.
//    6. Identify "Moderate Risk" patients who were admitted
//    to the "ICU" more than twice in the past year.
//     Scala Spark Data
//    val patients = List(
//      ("Patient1", 62, 10, 3, "ICU"),
//      ("Patient2", 45, 25, 1, "General"),
//      ("Patient3", 70, 8, 2, "ICU"),
//      ("Patient4", 55, 18, 3, "ICU"),
//      ("Patient5", 65, 30, 1, "General"),
//      ("Patient6", 80, 12, 4, "ICU"),
//      ("Patient7", 50, 40, 1, "General"),
//      ("Patient8", 78, 15, 2, "ICU"),
//      ("Patient9", 40, 35, 1, "General"),
//      ("Patient10", 73, 14, 3, "ICU")
//    ).toDF("patient_id", "age", "readmission_interval", "icu_admissions",
//      "admission_type")
//
//    val df1 = patients.withColumn("classification",
//      when(
//        (col("readmission_interval") < 15) &&
//          (col("age") > 60),"High Readmission Risk"
//      )
//        .when(
//          (col("readmission_interval").between(15,30)),"Moderate Risk"
//        )
//        .otherwise("Low Risk")
//    )
//
//    val df2 = df1.groupBy(col("classification"))
//      .agg(count(col("patient_id")).alias("no_of_patients_per_classification"))
//      .orderBy(col("no_of_patients_per_classification").desc)
//
//    //    5. Find the average readmission interval for
//    //    "High Readmission Risk" patients.
//
//    val df3 = df1.filter(col("classification")==="High Readmission Risk")
//      .agg(avg(col("readmission_interval"))
//        .alias("avg_readmission_interval_High_Readmission_Risk"))
//
//    //    6. Identify "Moderate Risk" patients who were admitted
//
//    val df4 = df1.filter(col("classification")==="Moderate Risk")
//      .alias("Moderate_Risk_Patients")
//
//    df1.show()
//    df2.show()
//    df3.show()
//    scala.io.StdIn.readLine()
//    df4.show()

    //---------------------------------------------------------------
//    Scenario 24: Student Graduation Prediction
//    Question Set: 7. Classify students as "At-Risk" if attendance
    //    is below 75% and the average test score
//    is below 50, "Moderate Risk" if attendance is between 75% and 85%,
    //    and "Low Risk" otherwise.
//    Calculate the number of students in each risk category.
//    8. Find the average score for students in the "At-Risk" category.
//    9. Identify "Moderate Risk" students who have scored above 70 in
    //    at least three subjects.
//     Scala Spark Data

//  val students = List(
//    ("Student1", 70, 45, 60, 65, 75),
//    ("Student2", 80, 55, 58, 62, 67),
//    ("Student3", 65, 30, 45, 70, 55),
//    ("Student4", 90, 85, 80, 78, 76),
//    ("Student5", 72, 40, 50, 48, 52),
//    ("Student6", 88, 60, 72, 70, 68),
//    ("Student7", 74, 48, 62, 66, 70),
//    ("Student8", 82, 56, 64, 60, 66),
//    ("Student9", 78, 50, 48, 58, 55),
//    ("Student10", 68, 35, 42, 52, 45)
//    ).toDF("student_id", "attendance_percentage", "math_score",
//    "science_score", "english_score",
//      "history_score")

//
//  val df1 = students.withColumn("classification",
//    when(
//      (col("attendance_percentage") < 75) &&
//        (((col("math_score")+col("science_score")+
//          col("english_score")+col("history_score"))/5) < 50),"At-Risk"
//    )
//      .when(
//        (col("attendance_percentage").between(75,85)),"Moderate Risk"
//      )
//      .otherwise("Low Risk")
//  )
//
//  val df2 = df1.groupBy(col("classification"))
//    .agg(count(col("student_id")).alias("no_of_students_per_classification"))
//    .orderBy(col("no_of_students_per_classification").desc)
//
////    8. Find the average score for students in the "At-Risk" category.
//
//  val df3 = df1.filter(col("classification")==="At-Risk")
//    .agg(avg(
//      ((col("math_score")+col("science_score")+
//        col("english_score")+col("history_score"))/5)
//    )
//      .alias("avg_score_Students_At_Risk"))
//
////    9. Identify "Moderate Risk" students who have scored above 70 in
////    at least three subjects.
//
//
//  val df4 = df1.filter(
//      (col("classification")==="Moderate Risk")
//    )
//    .alias("Moderate_Risk_Students")
//
//  val df5 = df4.withColumn("subjects_above_70",
//    (
//      when(col("math_score")>70,lit(1)).otherwise(lit(0))+
//        when(col("science_score")>70,lit(1)).otherwise(lit(0))+
//        when(col("english_score")>70,lit(1)).otherwise(lit(0))+
//        when(col("history_score")>70,lit(1)).otherwise(lit(0))
//          .alias("subjects_above_70")
//      )
//  )
//
//  val df6 = df5.filter(col("subjects_above_70") >= 3)
//
//  df1.show()
//  df2.show()
//  df3.show()
//  df4.show()
//  df5.show()
//  df6.show()
//  scala.io.StdIn.readLine()


    //---------------------------------------------------------------
    //---------------------------------------------------------------
    //---------------------------------------------------------------
    //---------------------------------------------------------------



  }
}
