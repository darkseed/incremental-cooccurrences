package io.ssc.incrementalcooccurrences.experiments.datapreparation

import java.io.{BufferedWriter, FileInputStream, FileWriter}
import java.util.zip.GZIPInputStream

import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.io.Source

//https://archive.org/details/archiveteam-twitter-stream-2017-01
object PrepareHashtagStream extends App {


  val usersById = new Object2IntLinkedOpenHashMap[String](40000000)
  val hashTagsById = new Object2IntLinkedOpenHashMap[String](40000000)

  var userId = 0
  var tagId = 0

  Source.fromInputStream(new GZIPInputStream(
    new FileInputStream("/home/ssc/Entwicklung/datasets/twitterhashtags/hashtagstream-raw.tsv.gz")))
    .getLines//.take(1000)
    .foreach { line =>
      //println(line)
      val tokens = line.split("\t")
      val user = tokens(1)
      val tag = tokens(2)

      if (!usersById.containsKey(user)) {
        usersById.put(user, userId)
        userId += 1
      }

      if (!hashTagsById.containsKey(tag)) {
        hashTagsById.put(tag, tagId)
        tagId += 1
      }
    }


  val usersWriter = new BufferedWriter(new FileWriter("/home/ssc/Entwicklung/datasets/twitterhashtags/users.tsv"))
  val usersIterator = usersById.object2IntEntrySet().fastIterator()
  while (usersIterator.hasNext) {
    val entry = usersIterator.next()
    usersWriter.append(s"${entry.getIntValue}\t${entry.getKey}")
    usersWriter.newLine()
  }
  usersWriter.close()

  val tagsWriter = new BufferedWriter(new FileWriter("/home/ssc/Entwicklung/datasets/twitterhashtags/tags.tsv"))
  val tagsIterator = hashTagsById.object2IntEntrySet().fastIterator()
  while (tagsIterator.hasNext) {
    val entry = tagsIterator.next()
    tagsWriter.append(s"${entry.getIntValue}\t${entry.getKey}")
    tagsWriter.newLine()
  }
  tagsWriter.close()

  val interactionsWriter = new BufferedWriter(new FileWriter("/home/ssc/Entwicklung/datasets/twitterhashtags/interactions.tsv"))
  Source.fromInputStream(new GZIPInputStream(
    new FileInputStream("/home/ssc/Entwicklung/datasets/twitterhashtags/hashtagstream-raw.tsv.gz")))
    .getLines//.take(1000)
    .foreach { line =>
      //println(line)
      val tokens = line.split("\t")
      val user = tokens(1)
      val tag = tokens(2)

      val dateStr = tokens(0).substring(4).replaceAll("Jan", "01").replaceAll("Feb", "02")

      val milliseconds = DateTime.parse(dateStr, DateTimeFormat.forPattern("M d H:mm:ss +0000 yyyy")).toInstant.getMillis

      interactionsWriter.append(s"${milliseconds}\t${usersById.getInt(user)}\t${hashTagsById.getInt(tag)}")
      interactionsWriter.newLine()
    }
  interactionsWriter.close()
}
