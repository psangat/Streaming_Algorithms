import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import com.javamex.classmexer.MemoryUtil
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object main {

  val hbit = new mutable.HashMap[String, mutable.BitSet]
  val m1 = new mutable.HashMap[String, mutable.ListBuffer[String]]
  val m2 = new mutable.HashMap[String, mutable.ListBuffer[String]]
  val m3 = new mutable.HashMap[String, mutable.ListBuffer[String]]
  val m4 = new mutable.HashMap[String, mutable.ListBuffer[String]]
  val m12 = new mutable.HashMap[String, Array[mutable.ListBuffer[String]]]
  val m123 = new mutable.HashMap[String, Array[mutable.ListBuffer[String]]]
  val mFix = List(m1, m2, m3, m4)
  val MAX_HASH_TABLE_SIZE = 1024000000
  // 1GB ??
  // bytes
  val hashTableR = new mutable.HashMap[String, mutable.ListBuffer[String]]
  val hashTableS = new mutable.HashMap[String, mutable.ListBuffer[String]]
  val hashTableT = new mutable.HashMap[String, mutable.ListBuffer[String]]
  val hashTableU = new mutable.HashMap[String, mutable.ListBuffer[String]]
  val hashTableCollectionR = new mutable.HashMap[String, mutable.HashMap[String, mutable.ListBuffer[String]]]

  val hashTableRSlice = new mutable.HashMap[String, mutable.ListBuffer[(String, String)]]
  val hashTableSSlice = new mutable.HashMap[String, mutable.ListBuffer[(String, String)]]
  val hashTableTSlice = new mutable.HashMap[String, mutable.ListBuffer[(String, String)]]
  val hashTableUSlice = new mutable.HashMap[String, mutable.ListBuffer[(String, String)]]
  val indirectPartitionMapper = new mutable.HashMap[String, mutable.ListBuffer[(String, String)]]
  // Global Variables
  var firstTime = 0L
  var y = new mutable.BitSet()
  var bittot = new mutable.BitSet(mFix.length)
  var kafkaServer = "118.138.244.164:9092"
  y += 1
  var isFirst = true
  for (i <- mFix.indices)
    bittot += i


  def main(args: Array[String]): Unit = {
    // Disabling the logs of type "org" and "akka"
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    if (args.length != 1) {
      println("Must have parameter x || m || am")
      sys.exit()
    }

    val master = "local[*]"
    // At least 2 required, 1 thread used to run receiver. local[n] n > number of receivers
    val ssc = new StreamingContext(master, "Kafka Streaming", Seconds(1))
    val kafkaParams = Map("metadata.broker.list" -> kafkaServer)
    val topics1 = List("stream1").toSet
    val topics2 = List("stream2").toSet
    val topics3 = List("stream3").toSet
    val topics4 = List("stream4").toSet

    val stream1 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics1)
    val stream2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics2)
    val stream3 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics3)
    val stream4 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics4)

    val words1 = stream1.map(_._2).map(x => (x.split(" ")(0), x.split(" ")(1)))
    val words2 = stream2.map(_._2).map(x => (x.split(" ")(0), x.split(" ")(1)))
    val words3 = stream3.map(_._2).map(x => (x.split(" ")(0), x.split(" ")(1)))
    val words4 = stream4.map(_._2).map(x => (x.split(" ")(0), x.split(" ")(1)))


    words1.foreachRDD(rdd => rdd.foreach {
      // stream for R
      case (k, v) => if (args(0).equals("x")) xJoin(k, v, m1, 0)
      else if (args(0).equals("m")) mJoin(k, v, m1, 0)
      else if (args(0).equals("am")) amJoin(k, v, m1, 0)
      else if (args(0).equals("ehj")) earlyHashJoin(k, v, "1M", "R")
      // CA = Common Attribute
      // DA = Distinct Attribute
      else if (args(0).equals("sj")) sliceJoin(k, v, "CA", "R")
    })

    words2.foreachRDD(rdd => rdd.foreach {
      // stream for S
      case (k, v) => if (args(0).equals("x")) xJoin(k, v, m2, 1)
      else if (args(0).equals("m")) mJoin(k, v, m2, 1)
      else if (args(0).equals("am")) amJoin(k, v, m2, 1)
      else if (args(0).equals("ehj")) earlyHashJoin(k, v, "1M", "S")
      else if (args(0).equals("sj")) sliceJoin(k, v, "CA", "S")
    })

    words3.foreachRDD(rdd => rdd.foreach {
      case (k, v) => if (args(0).equals("x")) xJoin(k, v, m3, 2)
      else if (args(0).equals("m")) mJoin(k, v, m3, 2)
      else if (args(0).equals("am")) amJoin(k, v, m3, 2)
      else if (args(0).equals("sj")) sliceJoin(k, v, "CA", "T")
    })

    words4.foreachRDD(rdd => rdd.foreach {
      case (k, v) => if (args(0).equals("x")) xJoin(k, v, m4, 3)
      else if (args(0).equals("m")) mJoin(k, v, m4, 3)
      else if (args(0).equals("am")) amJoin(k, v, m4, 3)
      else if (args(0).equals("sj")) sliceJoin(k, v, "CA", "U")
    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate

    // Clean Up phase is performed after streams are over
    //    val cleanUpPhaseStartTime = System.currentTimeMillis()
    //    val finalOutput = earlyHashJoinCleanupPhase(hashTableCollectionR)
    //    val cleanUpPhaseEndTime = System.currentTimeMillis()
    //    println("Time Taken for CleanUP Phase: {0}", cleanUpPhaseEndTime - cleanUpPhaseStartTime)
    //    finalOutput.foreach(println)
  }


  def earlyHashJoin(key: String, value: String, joinType: String, whichStream: String): Unit = {
    val phase1StartTime = System.currentTimeMillis()
    // One to Many Join
    if (joinType.equals("1M")) {
      whichStream match {
        case "R" => if (hashTableS.contains(key)) {
          println(key, value, hashTableS.get(key))
          hashTableR(key) = mutable.ListBuffer(value)
          hashTableS.remove(key)
        }
        else {
          hashTableR(key) = mutable.ListBuffer(value)
        }
        case "S" => if (hashTableR.contains(key)) {
          println(key, value, hashTableR.get(key))
        }
        else {
          hashTableS(key) = mutable.ListBuffer(value)
        }
      }
    }
    else {
      // Many to Many
      whichStream match {
        case "R" => if (hashTableS.contains(key)) {
          println(key, value, hashTableS.get(key))
          if (hashTableR.contains(key)) {
            hashTableR(key) += value
          }
          else {
            hashTableR(key) = mutable.ListBuffer(value)
          }
        }
        else {
          hashTableR(key) = mutable.ListBuffer(value)
        }
        case "S" => if (hashTableR.contains(key)) {
          println(key, value, hashTableR.get(key))
          if (hashTableS.contains(key)) {
            hashTableS(key) += value
          }
          else {
            hashTableS(key) = mutable.ListBuffer(value)
          }
        }
        else {
          hashTableS(key) = mutable.ListBuffer(value)
        }
      }
    }
    val phase1EndTime = System.currentTimeMillis()
    // println("Time Taken for Phase1: {0}", phase1EndTime - phase1StartTime)

    // Biased Flushing Policy
    val flushingPhaseStartTime = System.currentTimeMillis()
    val hashTableSize = MemoryUtil.deepMemoryUsageOf(hashTableS)
    //println(hashTableSize)
    // Flushing Phase
    if (hashTableSize >= MAX_HASH_TABLE_SIZE) {
      val flushTimeStamp = System.currentTimeMillis().toString()
      hashTableCollectionR(flushTimeStamp) = hashTableR
      val fos = new FileOutputStream("C:\\Raw_Data_Source_For_Test\\StreamingAlgorithmsOutput\\" + flushTimeStamp + ".ser")
      val oos = new ObjectOutputStream(fos)
      oos.writeObject(hashTableS)
      oos.close()
      hashTableR.clear()
      hashTableS.clear()
    }
    val flushingPhaseEndTime = System.currentTimeMillis()
    // println("Time Taken for Flushing Phase: {0}", flushingPhaseEndTime - flushingPhaseStartTime)
    // Cleanup Phase
  }

  def sliceJoin(key: String, value: String, joinType: String, whichStream: String): Unit = {
    if (joinType.equals("CA")) {
      whichStream match {
        // Common Attribute Join [Key is the common attribute]
        case "R" => if (hashTableRSlice.contains(key)) {
          hashTableRSlice(key) += ((key, value))
        } else {
          val listBuffer: mutable.ListBuffer[(String, String)] = ListBuffer()
          listBuffer += ((key, value))
          hashTableRSlice(key) = listBuffer
        }
          if (hashTableSSlice.contains(key) && hashTableTSlice.contains(key) && hashTableUSlice.contains(key)) {
            println(key, value, hashTableSSlice.get(key), hashTableTSlice.get(key), hashTableUSlice.get(key))
          }


        case "S" => if (hashTableSSlice.contains(key)) {
          hashTableSSlice(key) += ((key, value))
        } else {
          val listBuffer: mutable.ListBuffer[(String, String)] = ListBuffer()
          listBuffer += ((key, value))
          hashTableSSlice(key) = listBuffer
        }
          if (hashTableRSlice.contains(key) && hashTableTSlice.contains(key) && hashTableUSlice.contains(key)) {
            println(key, value, hashTableRSlice.get(key), hashTableTSlice.get(key), hashTableUSlice.get(key))
          }


        case "T" => if (hashTableTSlice.contains(key)) {
          hashTableTSlice(key) += ((key, value))
        } else {
          val listBuffer: mutable.ListBuffer[(String, String)] = ListBuffer()
          listBuffer += ((key, value))
          hashTableTSlice(key) = listBuffer
        }
          if (hashTableRSlice.contains(key) && hashTableSSlice.contains(key) && hashTableUSlice.contains(key)) {
            println(key, value, hashTableRSlice.get(key), hashTableSSlice.get(key), hashTableUSlice.get(key))
          }

        case "U" => if (hashTableUSlice.contains(key)) {
          hashTableUSlice(key) += ((key, value))
        } else {
          val listBuffer: mutable.ListBuffer[(String, String)] = ListBuffer()
          listBuffer += ((key, value))
          hashTableUSlice(key) = listBuffer
        }
          if (hashTableRSlice.contains(key) && hashTableSSlice.contains(key) && hashTableTSlice.contains(key)) {
            println(key, value, hashTableRSlice.get(key), hashTableSSlice.get(key), hashTableTSlice.get(key))
          }
      }
    }
    else {
      // Distinct Attribute Join using mapping function
      // R -> (a,b)
      // S -> (a,c)
      // T -> (c,d)
      // U -> (a,e)
      whichStream match {
        case "R" => if (hashTableRSlice.contains(key)) {
          hashTableRSlice(key) += ((key, value))
        } else {
          val listBuffer: mutable.ListBuffer[(String, String)] = ListBuffer()
          listBuffer += ((key, value))
          hashTableRSlice(key) = listBuffer
        }
          if (hashTableSSlice.contains(key) && hashTableUSlice.contains(key)) {
            // implementation of slice mapping
            val mappingList = hashTableSSlice.get(key).get // list buffer of key values
            mappingList.foreach { kvPair =>
              if (hashTableTSlice.contains(kvPair._2)) {
                println(key, value, hashTableTSlice.get(kvPair._2), hashTableUSlice.get(key))
              }
            }
            // implementation of slice mapping complete
          }
        case "S" =>
          if (hashTableSSlice.contains(key)) {
            hashTableSSlice(key) += ((key, value))
          } else {
            val listBuffer: mutable.ListBuffer[(String, String)] = ListBuffer()
            listBuffer += ((key, value))
            hashTableSSlice(key) = listBuffer
          }

          if (indirectPartitionMapper.contains(value)) {
            // used for mapping in stream T
            indirectPartitionMapper(value) += ((value, key))
          }
          else {
            val listBuffer: mutable.ListBuffer[(String, String)] = ListBuffer()
            listBuffer += ((value, key))
            indirectPartitionMapper(value) = listBuffer
          }

          if (hashTableRSlice.contains(key) && hashTableTSlice.contains(value) && hashTableUSlice.contains(key)) {
            // does not need slice mapping here
            println(key, value, hashTableRSlice.get(key), hashTableTSlice.get(value), hashTableUSlice.get(key))
          }


        case "T" => if (hashTableTSlice.contains(key)) {
          hashTableTSlice(key) += ((key, value))
        } else {
          val listBuffer: mutable.ListBuffer[(String, String)] = ListBuffer()
          listBuffer += ((key, value))
          hashTableTSlice(key) = listBuffer
        }
          if (indirectPartitionMapper.contains(key)) {
            val mappingList = indirectPartitionMapper.get(key).get
            mappingList.foreach { item =>
              val commonKey = item._2 // actually key of stream S
              if (hashTableRSlice.contains(commonKey) && hashTableUSlice.contains(commonKey)) {
                println(commonKey, hashTableRSlice.get(commonKey), item._1, value, hashTableUSlice.get(commonKey))
              }
            }
          }

        case "U" => if (hashTableUSlice.contains(key)) {
          hashTableUSlice(key) += ((key, value))
        } else {
          val listBuffer: mutable.ListBuffer[(String, String)] = ListBuffer()
          listBuffer += ((key, value))
          hashTableUSlice(key) = listBuffer
        }

          if (hashTableRSlice.contains(key) && hashTableSSlice.contains(key)) {
            // implementation of slice mapping
            val mappingList = hashTableSSlice.get(key).get // list buffer of key values
            mappingList.foreach { kvPair =>
              if (hashTableTSlice.contains(kvPair._2)) {
                println(key, hashTableRSlice.get(key), mappingList, hashTableTSlice.get(kvPair._2), value)
              }
            }
            // implementation of slice mapping complete
          }
      }
    }
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
