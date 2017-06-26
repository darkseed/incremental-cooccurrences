package io.ssc.incrementalcooccurrences

import scala.io.Source

object Run extends App {

  val start = System.currentTimeMillis()

  val stream = getClass.getResourceAsStream("/ml1m-shuffled.csv")

  val interactions = Source
      .fromInputStream(stream).getLines
      //.filter { line => !line.startsWith("%") }
      .map { line =>
        val tokens = line.split(",")
        Interaction(tokens(0).toInt - 1, tokens(1).toInt - 1)
      }

  val analysis = new IncrementalCooccurrenceAnalysis(
    interactions = interactions,
    numUsers = 9746,
    numItems = 6040,
    fMax = 500,
    kMax = 500,
    k = 10,
    seed = 0xcafe
  )

  analysis.process()

}
