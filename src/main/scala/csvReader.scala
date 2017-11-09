package reviewrCsv
import java.nio.file.Paths
import com.github.tototoshi.csv._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.io.Source
import scala.reflect.io.File

case class Food(  id: String ,  ProductId: String,  UserId: String,ProfileName: String = null,  HelpfulnessNumerator: String= null,
                  HelpfulnessDenominator: String = null,
                  Score: String = null, Time: String = null ,
                  Summary: String = null, Text: String) {
}
object Food{
  def apply(line :List[String]): Food = {
    new Food(id = line(0),
      ProductId=line(1),
      UserId=line(2),
      Text = line.last)
  }
}

object SparkUtils {

  def readCsvToDf(sc :SparkSession , fileName: String, delimiter: String , escapeChar: String = null) = {
    import org.apache.spark.sql.Encoders
    val schema = Encoders.product[Food].schema
    sc.read
      .option("wholeFile", true)
      .option("header", "true")
      .option("delimiter", delimiter)
      .option("escape", escapeChar)
      .schema(schema)
      .csv(fileName)//.as[Food]
  }

  def initSparkSession() :  SparkSession = {
    // cofiguration of spark should be done here
    lazy val sparkConf : SparkConf = new SparkConf()
      .set("spark.master", "local[*]")
      .set("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir") +File.separator + "spark_warehouse")
      .set("spark.local.dir", System.getProperty("java.io.tmpdir") +File.separator +  "spark_local")
    //this is for tuning spark memory consumption, but i saw the default is already 500MB, so no need...
    //      .set("spark.executor.memory","500MB")
//      .set("spark.driver.memory","500MB")

    lazy val sc = SparkSession.builder()
      .appName("Read csv data collection with Spark SQL")
      .config("spark.master", "local")
      .config(sparkConf)
      .getOrCreate()
    sc
  }
}

trait StatisticsAbs {
  def execute(fileName: String, amount: Int)
}

object SparkStatistics extends StatisticsAbs{

  def checkIdIntegrity(df: DataFrame) ={
    val h = getMostSomeColumn(df,"Id").head()
    if (h.getLong(1) == 1)
      true
    else
      throw new Exception(s"File is corrupted, the Id: ${h.getString(0)} appear ${h.getString(1)} times ")
  }
  def getMostActiveUsers(df :DataFrame ) = {
    getMostSomeColumn(df,"UserId")
  }

  def getMostcommentedFoodItems(df :DataFrame ) = {
    getMostSomeColumn(df,"ProductId")
  }

  def getMostSomeColumn(df : DataFrame, col : String) ={
    df.select(col).groupBy(col).count().sort(org.apache.spark.sql.functions.desc("count"))
  }

  def getMostUsedWordInReview(df: DataFrame) = {
    import df.sparkSession.implicits._
    df.select("Text")
      .flatMap(line => line.getString(0).trim.split("\\s*(,|\\s)\\s*"))
      .map(l=>(l,1))//.rdd.reduceByKey(_+_).toDF()
      .groupBy("_1")
      .count
      .sort(org.apache.spark.sql.functions.desc("count"))

  }

  override def execute(fileName: String, amount: Int) = {
    val sc = SparkUtils.initSparkSession()
    val csvFile = Paths.get(fileName)
    val df = SparkUtils.readCsvToDf(sc, csvFile.toString, ",", "\"")


    val users = SparkStatistics.getMostActiveUsers(df)
    val prod = SparkStatistics.getMostcommentedFoodItems(df)
    val words = SparkStatistics.getMostUsedWordInReview(df)

    SparkStatistics.checkIdIntegrity(df)

    println(s"The $amount most active users  :")
    users.take(amount).foreach(u => println(u))
    println(s"\nThe $amount most active products:")
    prod.take(amount).foreach(u => println(u))
    prod.schema
    println(s"\nThe $amount most used words in the reviews text:")
    words.take(amount).foreach(u => println(u))
  }
}

object LocalStatistics extends StatisticsAbs{

  def putOrIncrement(hash: mutable.HashMap[String,Int], key: String, value :Int = 1) : Unit = {
    val newVal = hash.get(key).getOrElse(0) + value
    hash.put(key,newVal)
  }

  def getMostOf(hash: mutable.HashMap[String,Int], amount :Int = 1000)= {
    hash.toList.sortWith((x, y) => x._2 > y._2).take(amount)
  }

  //ignore the first line bug, read it as a data for now..
  def accumulations(fileName: String, usersHash: mutable.HashMap[String, Int],
                    activeProds: mutable.HashMap[String, Int],
                    frequentWords: mutable.HashMap[String, Int],
                    ids :mutable.Set[String]): Unit = {

    //    val reader = CSVReader.open(Source.fromFile(fileName)("UTF-8").bufferedReader())
    val reader = CSVReader.open(fileName)
    reader.foreach { line =>
          val prodLine = Food.apply(line.toList)
          putOrIncrement(usersHash, prodLine.UserId)
          putOrIncrement(activeProds, prodLine.ProductId)
          val words = prodLine.Text.trim.split("\\s*(,|\\s)\\s*")
          words.foreach(word => putOrIncrement(frequentWords, word))
    //check for duplicates ids in the file
          if (!ids.add(prodLine.id))
            throw new Exception(s"File is corrupted, the Id: ${prodLine.id} appears few times ")
    }
    reader.close()
  }

  /**do the work w/o spark, low memory consumption, but can not work on hdfs or distribute work to tasks*/
  override def execute(fileName: String, amount:Int) = {

    val ids :mutable.Set[String] = new mutable.HashSet[String] ()
    val usersHash :mutable.HashMap[String,Int] = new mutable.HashMap()
    val activeProds :mutable.HashMap[String,Int] = new mutable.HashMap()
    val frequentWords :mutable.HashMap[String,Int] = new mutable.HashMap()

    accumulations(fileName, usersHash, activeProds, frequentWords, ids)

    println(s"The $amount most active users  :")
    getMostOf(usersHash, amount).foreach{l => var i =0 ; println(l._1 +"\t" + l._2)}

    println(s"\nThe $amount most active products:")
    getMostOf(activeProds, amount).foreach(l =>  println(l._1 +"\t" + l._2))

    println(s"\nThe $amount most used words in the reviews text:")
    getMostOf(frequentWords,amount).foreach(l =>  println(l._1 +"\t" + l._2))
  }
}

object Main{

  /**2 options: to run with spark driver, or just read the csv locally and count the occurrences */
  def main(args: Array[String]): Unit = {
    var stat :StatisticsAbs = LocalStatistics

    if (args.size ==3 )
      if(args(2) == "spark")
        stat = SparkStatistics

    stat.execute(args(0), args(1).toInt)
  }

}
