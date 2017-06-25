import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import com.javamex.classmexer.MemoryUtil
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object main {

  val hbit = new mutable.HashMap[String, mutable.BitSet]
  val m1 = new mutable.HashMap[String, mutable.ListBuffer[String]]
  val m2 = new mutable.HashMap[String, mutable.ListBuffer[String]]
  val m3 = new mutable.HashMap[String, mutable.ListBuffer[String]]
  val m4 = new mutable.HashMap[String, mutable.ListBuffer[String]]
  val m12 = new mutable.HashMap[String, Array[mutable.ListBuffer[String]]]
  val m123 = new mutable.HashMap[String, Array[mutable.ListBuffer[String]]]
  val mFix = List(m1, m2, m3, m4)
  val MAX_HASH_TABLE_SIZE = 24
  // bytes
  val hashTableCollectionR = new mutable.HashMap[String, mutable.HashMap[String, mutable.ListBuffer[String]]]
  // Global Variables
  var firstTime = 0L
  y += 1
  var isFirst = true
  for (i <- mFix.indices)
    bittot += i
  var y = new mutable.BitSet()
  var bittot = new mutable.BitSet(mFix.length)
  var kafkaServer = "118.138.244.164:9092"

  def main(args: Array[String]): Unit = {
    // Disabling the logs of type "org" and "akka"
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    if (args.length != 1) {
      println("Must have parameter x || m || am")
      sys.exit()
    }

    val master = "local[*]" // At least 2 required, 1 thread used to run receiver. local[n] n > number of receivers
    val ssc = new StreamingContext(master, "Kafka Streaming", Seconds(1))
    val kafkaParams = Map("metadata.broker.list" -> kafkaServer)
    val topics1 = List("stream1").toSet
    val topics2 = List("stream2").toSet
    //    val topics3 = List("stream3").toSet
    //    val topics4 = List("stream4").toSet

    val stream1 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics1)
    val stream2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics2)
    //    val stream3 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics3)
    //    val stream4 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics4)

    val words1 = stream1.map(_._2).map(x => (x.split(" ")(0), x.split(" ")(1)))
    val words2 = stream2.map(_._2).map(x => (x.split(" ")(0), x.split(" ")(1)))
    //    val words3 = stream3.map(_._2).map(x => (x.split(" ")(0), x.split(" ")(1)))
    //    val words4 = stream4.map(_._2).map(x => (x.split(" ")(0), x.split(" ")(1)))


    words1.foreachRDD(rdd => rdd.foreach {
      case (k, v) => if (args(0).equals("x")) xJoin(k, v, m1, 0)
      else if (args(0).equals("m")) mJoin(k, v, m1, 0)
      else if (args(0).equals("am")) amJoin(k, v, m1, 0)
      else earlyHashJoin(k, v, m1, 0)
    })

    words2.foreachRDD(rdd => rdd.foreach {
      case (k, v) => if (args(0).equals("x")) xJoin(k, v, m2, 1)
      else if (args(0).equals("m")) mJoin(k, v, m2, 1)
      else if (args(0).equals("am")) amJoin(k, v, m2, 1)
      else earlyHashJoin(k, v, m2, 0)
    })

    //    words3.foreachRDD(rdd => rdd.foreach {
    //      case (k, v) => if (args(0).equals("x")) xJoin(k, v, m3, 2)
    //      else if (args(0).equals("m")) mJoin(k, v, m3, 2)
    //      else if (args(0).equals("am")) amJoin(k, v, m3, 2)
    //    })
    //
    //    words4.foreachRDD(rdd => rdd.foreach {
    //      case (k, v) => if (args(0).equals("x")) xJoin(k, v, m4, 3)
    //      else if (args(0).equals("m")) mJoin(k, v, m4, 3)
    //      else if (args(0).equals("am")) amJoin(k, v, m4, 3)
    //    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }


  def earlyHashJoin(key: String, value: String, hashTable: mutable.HashMap[String, mutable.ListBuffer[String]], ix: Int): Unit = {
    if (isFirst) {
      isFirst = false
      firstTime = System.currentTimeMillis()
    }

    if (hashTable.contains(key)) {
      hashTable(key) += value
    }
    else {
      hashTable(key) = mutable.ListBuffer(value)
    }

    // Biased Flushing Policy

    val hashTableSize = MemoryUtil.deepMemoryUsageOf(m1)
    //println(hashTableSize)
    // Flushing Phase
    if (hashTableSize >= MAX_HASH_TABLE_SIZE) {
      val fileName = System.currentTimeMillis().toString()
      hashTableCollectionR(fileName) = m1
      val fos = new FileOutputStream("C:\\Raw_Data_Source_For_Test\\StreamingAlgorithmsOutput\\" + fileName + ".ser")
      val oos = new ObjectOutputStream(fos)
      oos.writeObject(m2)
      oos.close()
      m2.clear()
      m1.clear()
      //      val fis = new FileInputStream("c://list.ser")
      //      val ois = new ObjectInputStream(fis)
      //      val anotherList = ois.readObject()
      //      ois.close()
      //      println(anotherList)
    }

    // Cleanup Phase

  }

  def xJoin(k: String, v: String, m: mutable.HashMap[String, mutable.ListBuffer[String]], ix: Int): Unit = {
    if (isFirst) {
      isFirst = false;
      firstTime = System.currentTimeMillis()
    }

    val startJoin = System.nanoTime()
    if (m.contains(k)) {
      m(k) += v
    }
    else {
      m(k) = mutable.ListBuffer(v)
    }
    // Insert into hash table
    val endHash = System.nanoTime()
    var isJoinable = false
    if (ix == 0) {
      // If match with m2 then hash in m12
      if (mFix(1).contains(k)) {
        if (m12.contains(k)) {
          m12(k)(ix) += v
        }
        else {
          m12(k) = Array(mutable.ListBuffer(v), mFix(1)(k))
        }
        val endHash12 = System.nanoTime()
        // If match with m3 then hash in m123
        if (mFix(2).contains(k)) {
          if (m123.contains(k)) {
            m123(k)(ix) += v
          }
          else {
            m123(k) = Array(mutable.ListBuffer(v), m12(k)(1), mFix(2)(k))
          }
          if (mFix(3).contains(k)) {
            isJoinable = true
          }
        }
      }
    } else if (ix == 1) {
      // If match with m1 then hash in m12
      if (mFix(0).contains(k)) {
        if (m12.contains(k)) {
          m12(k)(ix) += v
        }
        else {
          m12(k) = Array(mFix(0)(k), mutable.ListBuffer(v))
        }
        val endHash12 = System.nanoTime()
        // If match with m3 then hash in m123
        if (mFix(2).contains(k)) {
          if (m123.contains(k)) {
            m123(k)(ix) += v
          }
          else {
            m123(k) = Array(m12(k)(0), mutable.ListBuffer(v), mFix(2)(k))
          }
          if (mFix(3).contains(k)) {
            isJoinable = true
          }
        }
      }
    } else if (ix == 2) {
      // If match with m12 then hash in m123
      if (m12.contains(k)) {
        if (m123.contains(k)) {
          m123(k)(ix) += v
        }
        else {
          m123(k) = Array(m12(k)(0), m12(k)(1), mutable.ListBuffer(v))
        }
        if (mFix(3).contains(k)) {
          isJoinable = true
        }
      }
    } else {
      // Join with m12
      if (m123.contains(k)) {
        isJoinable = true
      }
    }
    println((System.nanoTime() - startJoin) + " " + (endHash - startJoin) + " " + isJoinable + " " + k + " " + ix + " " + (System.currentTimeMillis() - firstTime))
  }

  def mJoin(k: String, v: String, m: mutable.HashMap[String, mutable.ListBuffer[String]], ix: Int): Unit = {
    if (isFirst) {
      isFirst = false
      firstTime = System.currentTimeMillis()
    }
    val startJoin = System.nanoTime()
    if (m.contains(k)) {
      m(k) += v
    }
    else {
      m(k) = mutable.ListBuffer(v)
    }
    //Insert Into Hash Table
    val endHash = System.nanoTime()
    val tmpm = new mutable.HashMap[Int, mutable.ListBuffer[String]]
    var i = 0
    var isJoinable = true
    while (i < mFix.length && isJoinable) {
      if (i != ix) {
        if (mFix(i).contains(k)) tmpm(i) = mFix(i)(k) else isJoinable = false
      } else {
        tmpm(ix) = mutable.ListBuffer(v)
      }
      i = i + 1
    }
    println((System.nanoTime() - startJoin) + " " + (endHash - startJoin) + " " + isJoinable + " " + k + " " + ix + " " + (System.currentTimeMillis() - firstTime))
  }

  def amJoin(k: String, v: String, m: mutable.HashMap[String, mutable.ListBuffer[String]], ix: Int): Unit = {
    if (isFirst) {
      isFirst = false;
      firstTime = System.currentTimeMillis()
    }

    val startJoin = System.nanoTime()
    if (m.contains(k)) {
      m(k) += v
    }
    else {
      m(k) = mutable.ListBuffer(v)
    }
    //Insert into hash table
    val endHash = System.nanoTime()
    var isJoinable = true
    var endByBV = 1
    if (hbit.contains(k)) {
      hbit(k) += ix
      if ((hbit(k) & bittot).size != 3) isJoinable = false
    } else {
      hbit(k) = mutable.BitSet()
      hbit(k) += ix
      isJoinable = false
    }
    if (isJoinable) {
      endByBV = 0
      val tmpm = new mutable.HashMap[Int, mutable.ListBuffer[String]]
      var i = 0
      while (i < mFix.length && isJoinable) {
        if (i != ix) {
          if (mFix(i).contains(k)) tmpm(i) = mFix(i)(k) else isJoinable = false
        } else {
          tmpm(ix) = mutable.ListBuffer(v)
        }
        i = i + 1
      }
    }
    println((System.nanoTime() - startJoin) + " " + (endHash - startJoin)
      + " " + endByBV + " " + isJoinable + " " + k + " " + ix + " " + (System.currentTimeMillis() - firstTime))
  }

  def earlyHashJoinCleanupPhase(hashTableCollectionR: mutable.HashMap[String, mutable.HashMap[String, mutable.ListBuffer[String]]]): mutable.HashMap[String, mutable.ListBuffer[mutable.ListBuffer[String]]] = {
    val outputTable = new mutable.HashMap[String, mutable.ListBuffer[mutable.ListBuffer[String]]]()
    // for each partition Ri in memomry, it is probed against the matching on-disk partition Si
    hashTableCollectionR.foreach(hashTableR => {
      val fis = new FileInputStream("C:\\Raw_Data_Source_For_Test\\StreamingAlgorithmsOutput\\" + hashTableR._1 + ".ser")
      val ois = new ObjectInputStream(fis)
      val hashTableS = ois.readObject().asInstanceOf[mutable.HashMap[String, mutable.ListBuffer[String]]]
      ois.close()
      hashTableR._2.foreach(item => {
        if (hashTableS.contains(item._1)) {
          outputTable(item._1) = mutable.ListBuffer(item._2)
          outputTable(item._1) += hashTableS(item._1)
        }
      })

    })
    return outputTable
  }
}
