import java.io.PrintWriter
import java.nio.file.{Files, Paths}

import org.junit._
import reviewrCsv.{SparkStatistics, SparkUtils}


object TestSparkCsvReader {
  var fileName: String = null
  var badFile: String = null

  @BeforeClass
  def createTempfile() = {
    val header = """Id,ProductId,UserId,ProfileName,HelpfulnessNumerator,HelpfulnessDenominator,Score,Time,Summary,Text"""
    val badLines: Array[String] = Array(
      """1,b1,user1,profile1,1,1,5,1303862400,summary1,text1a text1b""",
      """1,b1,user1,profile1,1,1,5,1303862400,summary1,text1a text2b""",
      """1,b1,user1,profile1,1,1,5,1303862400,summary1,text1a text2b""",
      """3,b1,user3,profile1,1,1,5,1303862400,summary1,text1a text2b""",
      """2,b1,user2,profile1,1,1,5,1303862400,summary1,text1a text2b""",
      """2,b1,user2,profile1,1,1,5,1303862400,summary1,text1a text2b""")
    val bfile = Files.createTempFile(null, null)
    new PrintWriter(bfile.toFile) {
      println(header);
      badLines.foreach(l => println(l)); close
    }

    badFile = bfile.toString

    val lines: Array[String] = Array(
      """1,b1,user1,profile1,1,1,5,1303862400,summary1,text1a text1b""",
      """2,b1,user1,profile1,1,1,5,1303862400,summary1,text1a text2b""",
      """3,b1,user1,profile1,1,1,5,1303862400,summary1,text1a text2b""",
      """4,b1,user3,profile1,1,1,5,1303862400,summary1,text1a text2b""",
      """5,b1,user2,profile1,1,1,5,1303862400,summary1,text1a text2b""",
      """6,b1,user2,profile1,1,1,5,1303862400,summary1,text1a text2b""")
    val gfile = Files.createTempFile(null, null)
    new PrintWriter(gfile.toFile) {
      println(header);
      lines.foreach(l => println(l)); close
    }

    fileName = gfile.toString

  }
}

@Test
class TestSparkCsvReader {


  @Test
  def testInitspark()={
    println("==== Start TestSparkSession ====")
    val sc = SparkUtils.initSparkSession()
    assert(sc != null)
  }

  @Test
  def sparkOpenCsvFile()={

    val sc = SparkUtils.initSparkSession()
    assert(sc != null)
    assert(!sc.sparkContext.isStopped)

    val df = SparkUtils.readCsvToDf(sc, TestSparkCsvReader.fileName.toString,",","\"")
    //    df.select("Id").groupBy("Id").sum($"Id")


    val users = SparkStatistics.getMostActiveUsers(df)
    val prod = SparkStatistics.getMostcommentedFoodItems(df)
    val words = SparkStatistics.getMostUsedWordInReview(df)
// should add more tests here like this one, to check each function results, but it too late hour


    assert(df.count() == 6)
    df.show(false)
    Files.delete(Paths.get(TestSparkCsvReader.fileName))
  }

}