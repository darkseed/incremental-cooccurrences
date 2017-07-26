package io.ssc.incrementalcooccurrences.experiments

import io.ssc.incrementalcooccurrences.{IncrementalCooccurrenceAnalysis, Interaction}

import scala.io.Source


object TwitterSampleStream extends App {

  val start = System.currentTimeMillis()

  val interactions = Source
    .fromFile("/home/ssc/Entwicklung/datasets/twitterhashtags/interactions.tsv").getLines
    .map { line =>
      val tokens = line.split("\t")
      Interaction(tokens(1).toInt, tokens(2).toInt)
    }

  val batchSize = 10000
  println("Batching up input data...")
  val batches = interactions.toArray.grouped(batchSize)

  val analysis = new IncrementalCooccurrenceAnalysis(
    numUsers = 8094909,
    numItems = 3070055,
    fMax = 500,
    kMax = 500,
    k = 10,
    seed = 0xcafeb
  )

  try {
    while (batches.hasNext) {
      val (durationForBatch, numChanges) = analysis.process(batches.next(), batchSize)
      println(s"\tbatchSize=${batchSize}, " +
        s"duration=${durationForBatch}, " +
        s"numChanges=${numChanges}, " +
        s"throughput=${(batchSize.toDouble / durationForBatch * 1000.0).toInt}/s")
    }
  } finally {
    analysis.close()
  }
}
