import java.io.{File, PrintWriter}
import java.util.ArrayList

import scala.collection.JavaConversions._
import scala.util.Random

/**
  * Created by psangats on 29/06/2017.
  */
object TestDataCreator {
  val _RStream = new ArrayList[String]()
  val _SStream = new ArrayList[String]()
  val _TStream = new ArrayList[String]()
  val _UStream = new ArrayList[String]()
  val dataDirectory = "C:\\Raw_Data_Source_For_Test\\StreamingAlgorithmTestData\\";

  def main(args: Array[String]): Unit = {
    for (i <- 1 until 100) {
      println("=================== START ======================")
      _RStream.add(i + " " + Random.alphanumeric.head)
      println(i + " " + Random.alphanumeric.head)
      if (i % 2 == 0) {
        _SStream.add(i + " " + Random.alphanumeric.head)
        println(i + " " + Random.alphanumeric.head)
      }
      if (i % 3 == 0) {
        _TStream.add(i + " " + Random.alphanumeric.head)
        println(i + " " + Random.alphanumeric.head)
      }
      if (i % 4 == 0) {
        _UStream.add(i + " " + Random.alphanumeric.head)
        println(i + " " + Random.alphanumeric.head)
      }
      println("=================== END =====================")
    }
    printFile(_RStream, dataDirectory, "RStreamCommon")
    printFile(_SStream, dataDirectory, "SStreamCommon")
    printFile(_TStream, dataDirectory, "TStreamCommon")
    printFile(_UStream, dataDirectory, "UStreamCommon")
  }

  def printFile(_RecordsList: ArrayList[String], saveDir: String, fileName: String): Unit = {
    //
    //val pw = new PrintWriter(new File("/mnt/outputFiles/SampleFile" + count + ".json"))
    val pw = new PrintWriter(new File(saveDir + fileName + ".txt"))
    _RecordsList.foreach { record =>
      pw.println(record)
    }
    pw.close
    _RecordsList.clear()
    //System.exit(1)
  }
}
