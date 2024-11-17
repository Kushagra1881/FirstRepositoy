import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, collect_list, count, countDistinct, current_date, date_sub, initcap, max, min, sum, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object nov11 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("spark.app.name","kushagra-spark-program")
    conf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    //    SPARK_PROBLEMS_FOR_PRACTICE_Assignment 2

    //    1. Count Items per Category
    //    Question: Count the number of products in each category.
    //    Sample Data:

//    val data = Seq(
//      ("Electronics", "Laptop"),
//      ("Electronics", "Phone"),
//      ("Clothing", "T-Shirt"),
//      ("Clothing", "Jeans"),
//      ("Furniture", "Chair")
//    ).toDF("category", "product")
//
//    val df1 = data.groupBy(col("category"))
//      .agg(count(col("product")).alias("count_no_of_product"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------

//    2. Find Minimum, Maximum, and Average Price per Product
//    Question: Calculate the minimum, maximum,
//    and average price for each product.

//    val productData = List(
//      ("Laptop", 1000),
//      ("Laptop", 500),
//      ("T-Shirt", 20),
//      ("Jeans", 50),
//      ("Chair", 150)
//    ).toDF("product","price")
//
//    val df1 = productData.groupBy(col("product"))
//      .agg(min(col("price")),max(col("price")),avg(col("price")))
//
//    productData.show()
//    df1.show()

    //---------------------------------------------------------------
//    3. Group Sales by Month and Year
//      Question: Group sales by month and year,
    //      and calculate the total amount for each month-year combination.

//  val salesData = List(
//    ("2023-01-01", "New York", 100),
//    ("2023-02-15", "London", 200),
//    ("2023-03-10", "Paris", 300),
//    ("2023-04-20", "Berlin", 400),
//    ("2023-05-05", "Tokyo", 500)
//  ).toDF("order_date","city","amount")
//
//  val df1 = salesData.groupBy(month(col("order_date")),year(col("order_date")))
//    .agg(sum(col("amount")))
//
//  df1.show()

    //---------------------------------------------------------------

//    4. Find Top N Products by Sales
//    Question: Find the top 5 products (by total quantity sold) across all orders.
//    Sample Data:

//    val data = List(
//  ("Laptop", "order_1", 2),
//  ("Phone", "order_2", 1),
//  ("T-Shirt", "order_1", 3),
//  ("Jeans", "order_3", 4),
//  ("Chair", "order_2", 2)
//    ).toDF("product", "order_id", "quantity")
//
//  val df1 = data.groupBy(col("product"))
//    .agg(sum(col("quantity"))
//    .alias("total_quantity_sold"))
//    .orderBy(col("total_quantity_sold").desc).limit(5)
//
//  data.show
//  df1.show

    //---------------------------------------------------------------
//    5. Calculate Average Rating per User
//      Question: Calculate the average rating given by each user.
//      Sample Data:
//    val data = List(
//      (1, 1, 4),
//      (1, 2, 5),
//      (2, 1, 3),
//      (2, 3, 4),
//      (3, 2, 5)
//    ).toDF("user_id", "product_id", "rating")
//
//    val df1 = data.groupBy(col("user_id"))
//      .agg(avg(col("rating")).alias("avg_rating_per_user"))
//      .orderBy(col("user_id"))
//
//    data.show
//    df1.show


    //---------------------------------------------------------------

//    6. Group Customers by Country and Calculate Total Spend
//    Question: Group customers by country and calculate the
//    total amount spent by customers in each country.
//    Sample Data:

//  val data = List(
//    (1, "USA", "order_1", 100),
//    (1, "USA", "order_2", 200),
//    (2, "UK", "order_3", 150),
//    (3, "France", "order_4", 250),
//    (3, "France", "order_5", 300)
//  ).toDF("customer_id", "country", "order_id", "amount")
//
//  val df1 = data.groupBy(col("country"))
//    .agg(sum(col("amount")).alias("total_amount_spent_per_country"))
//
//    data.show
//    df1.show
    //---------------------------------------------------------------

//    7. Find Products with No Sales in a Specific Time Period
//    Question: Identify products that had no sales between
    //    2023-02-01 and 2023-03-31.
//    Sample Data:

//  val data = List(
//    ("Laptop", "2023-01-01"),
//    ("Phone", "2023-02-15"),
//    ("T-Shirt", "2023-03-10"),
//    ("Jeans", "2023-04-20")
//  ).toDF("product", "order_date")
//
//  val df1 = data.withColumn("new_order_date",
//    to_date(col("order_date"))
//  )
//
//  val product_with_sales = df1.filter(
//    col("new_order_date").between("2023-02-01","2023-03-31")
//  ).select(col("product")
//    .alias("product_with_sales")).distinct()
//
//  val product_with_no_sales = df1.select(col("product")
//      .alias("product_with_no_sales"))
//    .distinct()
//    .except(product_with_sales)
//
//    data.show()
//    df1.show()
//    product_with_sales.show()
//    scala.io.StdIn.readLine()
//    product_with_no_sales.show()


//  val df1 = data.withColumn("Sales_btw_range",
//    when(
//      col("order_date").between("2023-02-01","2023-03-31"),col("order_date")
//    )
//      .otherwise("No Sales")
//  )

    //---------------------------------------------------------------

//    8. Calculate Order Count per Customer and City
//      Sample Data:
//    val data = List(
//      (1, "New York", "order_1"),
//      (1, "New York", "order")
//    ).toDF("customer_id", "city", "order_id")
//
//    val df1 = data.groupBy(col("customer_id"))
//      .agg(count(col("order_id")).alias("order_count_per_customer"))
//
//    val df2 = data.groupBy(col("city"))
//      .agg(count(col("order_id")).alias("order_count_per_city"))
//
//    data.show()
//    df1.show()
//    df2.show()

    //---------------------------------------------------------------
//    9. Group Orders by Weekday and Calculate Average Order Value (when-otherwise)
//    Question: Group orders by weekday (use dayofweek) and
    //    calculate the average order value for
//      weekdays and weekends using when and otherwise.
//    Sample Data:
//    val data = List(
//      ("2023-04-10", 1, 100),
//      ("2023-04-11", 2, 200),
//      ("2023-04-12", 3, 300),
//      ("2023-04-13", 1, 400),
//      ("2023-04-14", 2, 500)
//    ).toDF("order_date", "customer_id", "amount")
//
//    val df1 = data.withColumn("day_type",
//      when(
//        dayofweek(col("order_date")).isin(1,7),"weekend")
//        .otherwise("weekday")
//    ).groupBy(col("day_type"))
//      .agg(avg(col("amount"))
//        .alias("avg_order_value"))

//    data.show()
//    df1.show()

//    val df2 = df1.filter(col("day_type")==="weekend")
//      .agg(avg(col("amount")).alias("avg_order_value_weekend"))
//
//    val df3 = df1.filter(col("day_type")==="weekday")
//      .agg(avg(col("amount")).alias("avg_order_value_weekday"))


//    df2.show()
//    df3.show()

//    val df1 = data.groupBy(dayofweek(col("order_date")))
//      .agg(avg(col("amount")).alias("avg_order_amount"))


    //---------------------------------------------------------------

//    10. Filter Products Starting with "T" and Group by Category with Average Price
//      Question: Filter products starting with "T" and
    //      group them by category, calculating the average price
//    for each category.
//      Sample Data:

//  val data = List(
//    ("T-Shirt", "Clothing", 20),
//    ("Table", "Furniture", 150),
//    ("Jeans", "Clothing", 50),
//    ("Chair", "Furniture", 100)
//  ).toDF("product", "category", "price")
//
//  val df1 = data.filter(col("product").startsWith("T"))
//
//  val df2 = df1.groupBy(col("category"))
//    .agg(avg(col("price"))
//      .alias("avg_price_per_category"))
//
//    data.show()
//    df1.show()
//    df2.show()

    //---------------------------------------------------------------


//    11. Find Customers Who Spent More Than $200 in Total
//      Question: Group customers by customer ID and calculate the total amount spent.
    //  Filter customers who spent more than $200 in total.
//      Sample Data:

//    val data = List(
//      (1, "order_1", 100),
//      (1, "order_2", 150),
//      (2, "order_3", 250),
//      (3, "order_4", 100),
//      (3, "order_5", 120)
//    ).toDF("customer_id", "order_id", "amount")
//
//    val df1 = data.groupBy(col("customer_id"))
//      .agg(sum(col("amount")).alias("total_amount_spend_per_customer"))
//      .orderBy(col("customer_id"))
//
//    val df2 = df1.filter(col("total_amount_spend_per_customer")>200)
//      .orderBy(col("customer_id"))
//
//    data.show()
//    df1.show()
//    scala.io.StdIn.readLine()
//    df2.show()


    //---------------------------------------------------------------

//    12. Create a New Column with Order Status ("High" for > $100, "Low" Otherwise)
//      Question: Group orders by order ID and create a new column named
    //      "order_status" with values
//    "High" for orders with an amount greater than $100,
    //    and "Low" otherwise, using withColumn.

//    val data = List(
//      (1, "order_1", 100),
//      (1, "order_2", 150),
//      (2, "order_3", 250),
//      (3, "order_4", 100),
//      (3, "order_5", 120)
//    ).toDF("customer_id", "order_id", "amount")
//
//    val df1 = data.withColumn("order_status",
//      when(
//        col("amount")>100,"High"
//      ).otherwise("Low")
//    )
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------

//    13. Select Specific Columns and Apply GroupBy with Average
//     Question: Select only "product" and "price" columns,
//     then group by "product" and calculate the average price.

//    val data = List(
//      ("Laptop", "Electronics", 1000, 2),
//      ("Phone", "Electronics", 500, 1),
//      ("T-Shirt", "Clothing", 20, 3),
//      ("Jeans", "Clothing", 50, 4)
//    ).toDF("product", "category", "price", "quantity")
//
//    val df1 = data.select(col("product"),col("price"))
//      .groupBy(col("product"))
//      .agg(avg(col("price"))
//        .alias("avg_price"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------

//    14. Count Orders by Year and Month with Aggregation Functions (count, sum)
//      Question: Group orders by year and month,
    //      and calculate the total number of orders and total
//    amount for each month-year combination
//    Sample Data:

//    val data = List(
//      ("2023-01-01", 1, 100),
//      ("2023-02-15", 2, 200),
//      ("2023-03-10", 3, 300),
//      ("2023-04-20", 1, 400),
//      ("2023-05-05", 2, 500)
//    ).toDF("order_date", "customer_id", "amount")
//
//    val df1 = data.groupBy(year(col("order_date")),month(col("order_date")))
//      .agg(sum(col("amount")).alias("total_amount")
//        ,count(col("order_date")).alias("total_no_of_orders"))
//
//    data.show()
//    df1.show()


    //---------------------------------------------------------------

//    15. Find Products with Highest and Lowest Sales in Each Category (Top N)
//    Question: Group by category and find the top 2 products
//    (by total quantity sold) within each category

//    val data = List(
//      ("Laptop", "Electronics", 2),
//      ("Phone", "Electronics", 1),
//      ("Smart Watch", "Electronics", 3),
//      ("T-Shirt", "Clothing", 3),
//      ("Jeans", "Clothing", 4),
//      ("Chair", "Furniture", 2),
//      ("Table", "Furniture", 5),
//      ("Sofa", "Furniture", 1)
//    ).toDF("product", "category", "quantity")
//
//    val window = Window.partitionBy(col("category"))
//      .orderBy(col("quantity").desc)
//
//    val df1 = data.withColumn("rank", rank().over(window))
//      .filter(col("rank") <=2 )
//
//    val df2 = data.groupBy(col("category"))
//      .agg(sum(col("quantity"))
//        .alias("total_quantity_per_category"))
//      .orderBy(col("total_quantity_per_category").desc).limit(2)
//
//    data.show()
//    df1.show()
//    df2.show()

    //---------------------------------------------------------------

//    16. Calculate Average Rating per Product, Weighted by Quantity Sold
//      Question: Group by product ID, calculate the average rating,
//      weighted by the quantity sold for each order.

//    val data = List(
//      (1, "order_1", 4, 2),
//      (1, "order_2", 5, 1),
//      (2, "order_3", 3, 4),
//      (2, "order_4", 4, 3),
//      (3, "order_5", 5, 1)
//    ).toDF("product_id", "order_id", "rating", "quantity")
//
//    val df1 = data.withColumn("weighted_rating",
//      col("rating") * col("quantity")
//    ).groupBy(col("product_id"))
//      .agg((sum(col("weighted_rating"))/sum(col("quantity")))
//        .alias("weighted_avg_rating_per_product"))
//
//    df1.show()

    //---------------------------------------------------------------

  //17. Find Customers Who Placed Orders in More Than Two Different Months
  //Question: Group by customer ID and count the distinct number of months
  // in which they placed orders.
  // Filter customers who placed orders in more than two months.
  //Sample Data:

//    val data = Seq(
//      (1, "2023-01-01"),
//      (1, "2023-02-15"),
//      (1, "2023-03-22"),
//      (2, "2023-03-10"),
//      (2, "2023-03-20"),
//      (2, "2023-04-15"),
//      (2, "2023-05-25"),
//      (3, "2023-04-20"),
//      (3, "2023-05-05"),
//      (3, "2023-06-12"),
//      (4, "2023-01-10"),
//      (4, "2023-02-14"),
//      (4, "2023-03-18"),
//      (5, "2023-07-15"),
//      (5, "2023-08-22"),
//      (6, "2023-06-25"),
//      (6, "2023-09-15"),
//      (6, "2023-10-10")
//    ).toDF("customer_id", "order_date")
//
//    val df1 = data.groupBy(col("customer_id"))
//      .agg(countDistinct(month(col("order_date")))
//        .alias("distinct_month_count"))
//      .filter(col("distinct_month_count") > 2)
//      .orderBy("customer_id")
//
//    df1.show()

    //---------------------------------------------------------------
//    18. Group by Country and Calculate Total Sales, Excluding Orders Below $50
//      Question: Group by country, calculate the total sales amount,
//      excluding orders with an amount less than $50.
//      Sample Data:

//    val data = Seq(
//      ("USA", "order_1", 100),
//      ("USA", "order_2", 40),
//      ("UK", "order_3", 150),
//      ("France", "order_4", 250),
//      ("France", "order_5", 30)
//    ).toDF("country", "order_id", "amount")
//
//    val df1 = data.filter(col("amount")>50)
//
//    val df2 = df1.groupBy(col("country"))
//      .agg(sum(col("amount")).alias("total_sales_per_country"))
//
//    data.show()
//    df1.show()
//    df2.show()

    //---------------------------------------------------------------

    // Didn't understand this question

    // 19. Find Products Never Ordered Together (Pairwise Co-occurrence)
    //Question: Identify product pairs that never appeared together in any order.
    // This may require self-joins or other techniques for pairwise comparisons.
    // (Repeat order to avoid self-joins)
    //Sample Data:

//    val data = Seq(
//      ("order_1", 1, 2),
//      ("order_2", 1, 3),
//      ("order_3", 2, 4),
//      ("order_4", 3, 1)
//    ).toDF("order_id", "product_id1", "product_id2")
//
//    val productIds = data.select(col("product_id1") as "product_id").union(
//      data.select(col("product_id2") as "product_id")
//    ).distinct()
//
//    val productPairs = productIds.crossJoin(
//      productIds.withColumnRenamed("product_id", "product_id_other"))
//      .filter(col("product_id") < col("product_id_other"))
//
//    val actualPairs = data.select(
//      col("product_id1") as "product_id",
//      col("product_id2") as "product_id_other"
//    ).union(
//      data.select(
//        col("product_id2") as "product_id",
//        col("product_id1") as "product_id_other"
//      )
//    ).distinct()
//
//    val neverTogether = productPairs.join(
//      actualPairs, Seq("product_id", "product_id_other"), "left_anti")
//
//    neverTogether.show()

    //---------------------------------------------------------------
//
//    20. Group by Category and Calculate Standard Deviation of Price
//      Question: Group by category and
//      calculate the standard deviation of price for each category.
//      Sample Data:
//    val data = List(
//      ("Laptop", "Electronics", 1000),
//      ("Phone", "Electronics", 500),
//      ("T-Shirt", "Clothing", 20),
//      ("Jeans", "Clothing", 50),
//      ("Chair", "Furniture", 150),
//      ("Sofa", "Furniture", 200)
//    ).toDF("product", "category", "price")
//
//    val df1 = data.groupBy(col("category"))
//      .agg(stddev(col("price")).alias("std_dev_per_category"))
//
//    data.show()
//    df1.show()

    //---------------------------------------------------------------
//21. Find Most Frequent Customer City Combinations
    //Question: Group by customer_id and city, and find the
    // most frequent city for each customer.
    //Sample Data:
//    val customerCityData = List(
//      (1, "New York"),
//      (1, "New York"),
//      (2, "London"),
//      (2, "Paris"),
//      (3, "Paris"),
//      (3, "Paris")
//    ).toDF("customer_id", "city")
//
//    val df1 = customerCityData.groupBy(col("customer_id"),col("city"))
//      .agg(count(col("city")).alias("count_of_city_per_customer_id"))
//      .withColumn("ranking",
//        row_number().over(Window.partitionBy(col("customer_id"))
//          .orderBy(col("count_of_city_per_customer_id").desc))
//      ).filter(col("ranking")===1)
//
//    df1.show()


    //---------------------------------------------------------------
    //22. Calculate Customer Lifetime Value (CLTV) by Year
    //Question: Group by customer_id and year (use year),
    // calculate the total amount spent for each
    //customer in each year. This can be used to calculate CLTV.
    //Sample Data:
//  val cltvData = List(
//          (1, "2022-01-01", 100),
//          (1, "2023-02-15", 200),
//          (2, "2022-03-10", 300),
//          (2, "2023-04-20", 400),
//          (3, "2022-05-05", 500),
//          (3, "2023-06-06", 600)
//      ).toDF("customer_id", "order_date", "amount")

//    val df1 = cltvData.groupBy(col("customer_id"), year(col("order_date")))
//      .agg(sum(col("amount")).alias("sum_of_amount_per_year"))
//      .orderBy(col("customer_id"))
//
//    df1.show()


//    when you want to see the cumulative amount spent per customer over the years
//
//    val df1 = cltvData.withColumn("cumulative_amount_spent",
//        sum(col("amount"))
//          .over(Window.partitionBy(col("customer_id"))
//            .orderBy(year(col("order_date")))))
//
//    df1.show()


    //---------------------------------------------------------------

    //23. Find Products with a Decline in Average Rating Compared to Previous Month
    //Question: Group by product_id and month (use month),
    // calculate the average rating for each
    //product in each month. Identify products with a decrease
    // in average rating compared to the previous month.

    //Sample Data:
//    val productRatingData = List(
//            (1, "2023-01-01", 4),
//            (1, "2023-02-15", 3),
//            (2, "2023-01-10", 5),
//            (2, "2023-02-20", 4),
//            (3, "2023-01-20", 4),
//            (3, "2023-02-25", 5)
//        ).toDF("product_id", "order_date", "rating")
//
//    val df1 = productRatingData.groupBy(col("product_id"),
//        month(col("order_date")).alias("month"))
//      .agg(avg(col("rating")).alias("avg_rating_per_month"))
//      .orderBy(col("product_id"),col("month"))
//
//    val window = Window.partitionBy(col("product_id"))
//      .orderBy(col("month"))
//
//    val df2 = df1.withColumn("prev_month_rating",
//      lag(col("avg_rating_per_month"),1).over(window))
//
//    val df3 = df2.filter(col("prev_month_rating")>(col("avg_rating_per_month")))
//
//    df2.show()
//    df3.show()

    //---------------------------------------------------------------

    //24. Group Orders by Weekday and Find Peak Hour for Orders
    //Question: Group by weekday (use dayofweek) and hour,
    // and find the hour with the most orders for
    //each weekday.
    //Sample Data:
//    val orderData = List(
//            ("order_1", "2023-04-10", 10),
//            ("order_2", "2023-04-11", 15),
//            ("order_3", "2023-04-12", 12),
//            ("order_4", "2023-04-13", 11),
//            ("order_5", "2023-04-14", 18)
//        ).toDF("order_id", "order_date", "hour")

//    val df1 = orderData.groupBy(
//        dayofweek(col("order_date")).alias("day_number_of_week"))
//      .agg(max(col("hour")).alias("peak_hour_per_day_of_week"))
//      .orderBy(col("peak_hour_per_day_of_week").desc)


//        When you want to know the exact name of the day

//    val df2 = orderData.groupBy(
//      date_format(col("order_date"),"EEEE").alias("day_name_of_week"))
//      .agg(max(col("hour")).alias("peak_hour_per_day_of_week"))
//      .orderBy(col("peak_hour_per_day_of_week").desc)
//
//    df2.show()
//    scala.io.StdIn.readLine()
//    df1.show()

    //---------------------------------------------------------------

    //25. Calculate Average Order Value by Country, Excluding Cancelled Orders
    //Question: Group by country, calculate the average order value,
    // excluding orders with a "Cancelled" status.
    //Sample Data:

//    val orderValueData = List(
//          ("USA", "order_1", 100, "Shipped"),
//          ("USA", "order_2", 40, "Cancelled"),
//          ("UK", "order_3", 150, "Completed"),
//          ("France", "order_4", 250, "Pending"),
//          ("France", "order_5", 30, "Shipped")
//      ).toDF("country", "order_id", "amount", "status")
//
//    val df1 = orderValueData.filter(col("status")=!="Cancelled")
//      .groupBy(col("country"))
//      .agg(avg(col("amount")).alias("avg_order_value_by_country"))
//      .orderBy(col("avg_order_value_by_country").desc)
//
//    df1.show

    //---------------------------------------------------------------


  }
}
