package io.ssc.incrementalcooccurrences

import java.util.concurrent.{Callable, Executors}

import it.unimi.dsi.fastutil.ints._

class IncrementalCooccurrenceAnalysis(
    numUsers: Int,
    numItems: Int,
    fMax: Int,
    kMax: Int,
    k: Int,
    seed: Int) extends AutoCloseable {

  val executorService = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors)

  val random = new scala.util.Random(0xcafe)

  /* sampled user histories */
  //TODO whats a good estimate for the average number of interactions per user?
  val samplesOfA = Array.fill[IntArrayList](numUsers) { new IntArrayList(10) }
  val userNonSampledInteractionCounts = Array.ofDim[Int](numUsers)
  val userInteractionCounts = Array.ofDim[Int](numUsers)
  val itemInteractionCounts = Array.ofDim[Int](numItems)

  /* cooccurrence matrix */
  //TODO whats a good estimate for the average number of cooccurrences per item?
  val C = Array.fill[Int2ShortOpenHashMap](numItems) { new Int2ShortOpenHashMap(10) }
  /* rows sums of the cooccurrence matrix */
  val rowSumsOfC = Array.ofDim[Int](numItems)

  val indicators = Array.fill[PriorityQueue[(Int, Double)]](numItems) {
    new PriorityQueue[(Int, Double)](k) {
      override protected def lessThan(a: (Int, Double), b: (Int, Double)): Boolean = { a._2 < b._2 }
    }
  }

  var numInteractionsObserved = 0
  var numInteractionsSampled = 0
  var numCooccurrencesObserved = 0L


  override def close() {
    executorService.shutdownNow()
  }

  def process(interactions: TraversableOnce[Interaction], batchSize: Int): (Long, Int) = {

    val start = System.currentTimeMillis()

    val interactionsIterator = interactions.toIterator

    val itemsToRescore = new IntArraySet(batchSize)


    while (interactionsIterator.hasNext) {

      val interaction = interactionsIterator.next()

      val user = interaction.user
      val item = interaction.item

      userNonSampledInteractionCounts(user) += 1
      numInteractionsObserved += 1

      if (itemInteractionCounts(item) < fMax) {

        val userHistory = samplesOfA(user).elements()
        val numItemsInUserHistory = samplesOfA(user).size()

        if (userInteractionCounts(user) < kMax) {

          var n = 0
          while (n < numItemsInUserHistory) {
            val otherItem = userHistory(n)

            C(item).addTo(otherItem, 1)
            C(otherItem).addTo(item, 1)

            rowSumsOfC(otherItem) += 1

            n += 1
          }

          rowSumsOfC(item) += numItemsInUserHistory
          numCooccurrencesObserved += 2 * numItemsInUserHistory

          samplesOfA(user).add(item)

          userInteractionCounts(user) += 1
          itemInteractionCounts(item) += 1
          numInteractionsSampled += 1

          itemsToRescore.add(item)
        } else {

          val k = random.nextInt(userNonSampledInteractionCounts(user) + 1)

          if (k < numItemsInUserHistory) {

            val previousItem = samplesOfA(user).getInt(k)
            //TODO we could check for previousItem == newItem

            var n = 0
            while (n < numItemsInUserHistory) {
              val otherItem = userHistory(n)

              if (n != k) {
                C(item).addTo(otherItem, 1)
                C(otherItem).addTo(item, 1)

                C(previousItem).addTo(otherItem, -1)
                C(otherItem).addTo(previousItem, -1)
              }
              n += 1
            }

            rowSumsOfC(item) += numItemsInUserHistory - 1
            rowSumsOfC(previousItem) -= numItemsInUserHistory - 1

            samplesOfA(user).set(k, item)

            itemInteractionCounts(item) += 1
            itemInteractionCounts(previousItem) -= 1
            numInteractionsSampled += 1

            itemsToRescore.add(item)
          }

        }
      }
    }

    val tasks = new java.util.ArrayList[Callable[Unit]](itemsToRescore.size())
    val itemsToRescoreIterator = itemsToRescore.iterator()
    while (itemsToRescoreIterator.hasNext) {
      tasks.add(new Rescorer(itemsToRescoreIterator.nextInt()))
    }

    val results = executorService.invokeAll(tasks)

    val resultsIterator = results.iterator()
    while (resultsIterator.hasNext) {
      resultsIterator.next().get()
    }

    val duration = System.currentTimeMillis() - start
    (duration, itemsToRescore.size())
  }

  class Rescorer(item: Int) extends Callable[Unit] {

    override def call(): Unit = {

      val indicatorsForItem = indicators(item)

      indicatorsForItem.clear()

      val cooccurrencesOfItem = C(item).int2ShortEntrySet()
      val particularCooccurrences = cooccurrencesOfItem.fastIterator()

      while (particularCooccurrences.hasNext) {
        val entry = particularCooccurrences.next()
        val otherItem = entry.getIntKey

        val k11 = entry.getShortValue.toLong
        val k12 = rowSumsOfC(item).toLong - k11
        val k21 = rowSumsOfC(otherItem) - k11
        val k22 = numCooccurrencesObserved + k11 - k12 - k21

        val score = Loglikelihood.logLikelihoodRatio(k11, k12, k21, k22)

        if (indicatorsForItem.size < k) {
          indicatorsForItem.add(otherItem -> score)
        } else if (score > indicatorsForItem.top()._2) {
          indicatorsForItem.updateTop(otherItem -> score)
        }
      }
    }
  }

}
