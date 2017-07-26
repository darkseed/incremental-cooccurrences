package io.ssc.incrementalcooccurrences.experiments

import io.ssc.incrementalcooccurrences.{IncrementalCooccurrenceAnalysis, Interaction}

import scala.io.Source

object Movielens1M extends App {

  val start = System.currentTimeMillis()

  val stream = getClass.getResourceAsStream("/ml1m-shuffled.csv")

  val interactions = Source
      .fromInputStream(stream).getLines
      .map { line =>
        val tokens = line.split(",")
        Interaction(tokens(0).toInt - 1, tokens(1).toInt - 1)
      }

  val batchSize = 10000
  val batches = interactions.toArray.grouped(batchSize)

  val analysis = new IncrementalCooccurrenceAnalysis(
    numUsers = 9746,
    numItems = 6040,
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
