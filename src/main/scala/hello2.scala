import org.apache.spark.SparkContext

object hello2 {
  def main(args: Array[String]): Unit = {

    //    val sc = new SparkContext("local[*]", "Kushagra")
//    val input = sc.textFile("C:/Users/kusha/Downloads/ip_address.txt")
//    val rdd1 = input.flatMap(x => x.split(" "))
//    val rdd2 = rdd1.map(x => (x, 1))
//    val rdd3 = rdd2.reduceByKey((x, y) => x + y)
//    val rdd4 = rdd3.sortBy(x=>x._2,false)
//    rdd4.take(2).foreach(println)
//
//    scala.io.StdIn.readLine()

//    val sc=new SparkContext("local[*]","Kushagra Srivastava")
//    val input=sc.textFile("C:/Users/kusha/Downloads/data.txt")
//    val rdd1=input.flatMap(x=>x.split(" "))
//    val rdd2=rdd1.map(x=>(x,1))
//    val rdd3=rdd2.reduceByKey((x,y)=>(x+y))
//    val rdd4=rdd3.sortBy(x=>x._2,true)
//    rdd4.collect().foreach(println)
//
//    scala.io.StdIn.readLine()

    // Top 2 IP Addresses
//    val sc = new SparkContext("local[*]","Kushagra IP Address Prob")
//    val input = sc.textFile("C:/Users/kusha/Downloads/ip_address.txt")
//    val rdd1 = input.flatMap(x => x.split(" "))
//    val rdd2 = rdd1.map(x => (x,1))
//    val rdd3 = rdd2.reduceByKey((x,y) => (x+y))
//    val rdd4 = rdd3.sortBy(x=> x._2)
//    rdd4.take(2).foreach(println)
//
//    scala.io.StdIn.readLine()

//    val sc = new SparkContext("local[*]","Kushagra IntelliJ")
//    val arr = Array(10,20,30,40,50,60,70,80,91)
//    val rdd1 = sc.parallelize(arr)
//    val sum = rdd1.reduce((x,y) => (x+y))
//    val c = rdd1.count()
//    val avg = sum/c.toDouble
//    print(avg)

//    val sc = new SparkContext("local[*]","Kush File to Text")
//    val rdd1 = sc.parallelize(Array(1,2,3,4,5))
//    rdd1.saveAsTextFile("C:/Users/kusha/Downloads/Data_File/nov1")

//    val sc = new SparkContext("local[*]","Kush Join")
//    val rdd1 = sc.parallelize(Array((1,"apple"),(2,"banana"),(3,"orange")))
//    val rdd2 = sc.parallelize(Array((1,"red"),(2,"yellow"),(4,"green")))
//    val joinrdd = rdd1.join(rdd2)
//    joinrdd.foreach(println)

//    val sc = new SparkContext("local[*]","Kush Join")
//    val rdd1 = sc.parallelize(Array((1,"apple"),(2,"banana"),(3,"orange")))
//    val rdd2 = sc.parallelize(Array((1,"red"),(2,"yellow"),(4,"green")))
//    val joinrdd = rdd1.rightOuterJoin(rdd2)
//    joinrdd.foreach(println)

    val sc = new SparkContext("local[*]","Kush Subtract")
    val rdd1 = sc.parallelize(Array(1,2,3,4,5))
    val rdd2 = sc.parallelize(Array(3,4,5))
//    val rdd3 = rdd1.union(rdd2)
    val rdd3 = rdd1.subtract(rdd2)
    //    ascending
//    val rdd4 = rdd3.sortBy(x => x)
    // ascending
//    val rdd4 = rdd3.sortBy(x => x, false)
    // descending
//    val rdd4 = rdd3.sortBy(x => x, true)
//    rdd4.collect().foreach(println)

//    val sc = new SparkContext("local[*]","Kush App")
//    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5))
//    val rdd2 = sc.parallelize(Array("A", "B", "C"))
//    val rdd3 = rdd1.cartesian(rdd2)
//    rdd3.collect().foreach(println)

//    val sc = new SparkContext("local[*]","Kush App")
//    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5))
//    val rdd2 = rdd1.filter(x=>x%2!=0)
//    rdd2.collect().foreach(println)

//    val sc = new SparkContext("local[*]","Kush App")
//    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5))
//    val search = 4
//    val rdd2 = rdd1.filter(x => x == search)
//    rdd2.collect().foreach(println)


    scala.io.StdIn.readLine()









  }
}