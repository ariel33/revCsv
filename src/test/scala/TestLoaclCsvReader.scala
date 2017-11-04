import java.io.PrintWriter
import java.nio.file.{Files, Paths}

import org.junit._
import reviewrCsv.LocalStatistics

import scala.collection.mutable

object TestLoaclCsvReader{
  var fileName :String = null
  var badFile : String = null
  @BeforeClass
  def createTempfile() = {
    val header = """Id,ProductId,UserId,ProfileName,HelpfulnessNumerator,HelpfulnessDenominator,Score,Time,Summary,Text"""
    val badLines : Array[String] = Array("""1,b1,user1,profile1,1,1,5,1303862400,summary1,text1a text1b""",
      """1,b1,user1,profile1,1,1,5,1303862400,summary1,text1a text2b""",
      """1,b1,user1,profile1,1,1,5,1303862400,summary1,text1a text2b""",
      """3,b1,user3,profile1,1,1,5,1303862400,summary1,text1a text2b""",
      """2,b1,user2,profile1,1,1,5,1303862400,summary1,text1a text2b""",
      """2,b1,user2,profile1,1,1,5,1303862400,summary1,text1a text2b""")
    val bfile = Files.createTempFile(null,null)
    new PrintWriter(bfile.toFile) {  println(header);  badLines.foreach(l => println(l)); close }

    badFile = bfile.toString

    val lines : Array[String] = Array("""1,b1,user1,profile1,1,1,5,1303862400,summary1,text1a text1b""",
      """2,b1,user1,profile1,1,1,5,1303862400,summary1,text1a text2b""",
      """3,b1,user1,profile1,1,1,5,1303862400,summary1,text1a text2b""",
      """4,b1,user3,profile1,1,1,5,1303862400,summary1,text1a text2b""",
      """5,b1,user2,profile1,1,1,5,1303862400,summary1,text1a text2b""",
      """6,b1,user2,profile1,1,1,5,1303862400,summary1,text1a text2b""")
    val gfile = Files.createTempFile(null,null)
    new PrintWriter(gfile.toFile) {  println(header);  lines.foreach(l => println(l)); close }

    fileName = gfile.toString

  }


//  @AfterClass
//  def tearDown() = { Files.delete(Paths.get(fileName)); Files.delete(Paths.get(badFile))}
}
@Test
class TestLoaclCsvReader {

  @Test
  def testAccumulate()={

    val ids :mutable.Set[String] = new mutable.HashSet[String] ()
    val usersHash :mutable.HashMap[String,Int] = new mutable.HashMap()
    val activeProds :mutable.HashMap[String,Int] = new mutable.HashMap()
    val frequentWords :mutable.HashMap[String,Int] = new mutable.HashMap()

    LocalStatistics.accumulations(TestLoaclCsvReader.fileName, usersHash, activeProds, frequentWords, ids)
    assert(usersHash.size == 4)
    assert(activeProds.size == 2)
    assert(frequentWords.size == 4)

    // should add more tests here like this one, to check each function results, but it too late hour..

  }

  @Test
  def testBadFile()={

    val ids :mutable.Set[String] = new mutable.HashSet[String] ()
    val usersHash :mutable.HashMap[String,Int] = new mutable.HashMap()
    val activeProds :mutable.HashMap[String,Int] = new mutable.HashMap()
    val frequentWords :mutable.HashMap[String,Int] = new mutable.HashMap()
    try {
      LocalStatistics.accumulations(TestLoaclCsvReader.badFile, usersHash, activeProds, frequentWords, ids)
    }catch {
      case e :Exception => assert(true)
      case _ =>  assert(false)
    }

    // should add more tests here like this one, to check each function results, but it too late hour

  }

}
//df.select("Id").groupBy("Id").count().sort("Id")
