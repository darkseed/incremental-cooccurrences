package io.ssc.incrementalcooccurrences

import java.util.concurrent.{Callable, Executors}

import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, IntArrayList, IntArraySet}

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
  val userInteractionCounts = Array.ofDim[Int](numUsers)
  val itemInteractionCounts = Array.ofDim[Int](numItems)

  /* cooccurrence matrix */
  //TODO whats a good estimate for the average number of cooccurrences per item?
  //TODO needs to be volatile?
  val C = Array.fill[Int2IntOpenHashMap](numItems) { new Int2IntOpenHashMap(10) }
  /* rows sums of the cooccurrence matrix */
  //TODO needs to be volatile?
  val rowSumsOfC = Array.ofDim[Int](numItems)

  //TODO needs to be volatile?
  val indicators = Array.fill[IntArraySet](numItems) { new IntArraySet(k) }

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

      numInteractionsObserved += 1

      if (itemInteractionCounts(item) < fMax) {

        val historyView = samplesOfA(user).elements()
        val numItemsInHistoryView = samplesOfA(user).size()

        if (userInteractionCounts(user) < kMax) {

          var n = 0
          while (n < numItemsInHistoryView) {
            val otherItem = historyView(n)

            C(item).addTo(otherItem, 1)
            C(otherItem).addTo(item, 1)

            rowSumsOfC(otherItem) += 1

            n += 1
          }

          rowSumsOfC(item) += numItemsInHistoryView
          numCooccurrencesObserved += 2 * numItemsInHistoryView

          samplesOfA(user).add(item)

          userInteractionCounts(user) += 1
          itemInteractionCounts(item) += 1
          numInteractionsSampled += 1

          itemsToRescore.add(item)
        } else {

          val k = random.nextInt(userInteractionCounts(user) + 1)

          if (k < kMax) {

            val previousItem = samplesOfA(user).getInt(k)

            var n = 0
            while (n < numItemsInHistoryView) {
              val otherItem = historyView(n)

              if (n != k) {
                C(item).addTo(otherItem, 1)
                C(otherItem).addTo(item, 1)

                C(previousItem).addTo(otherItem, -1)
                C(otherItem).addTo(previousItem, -1)
              }
              n += 1
            }

            rowSumsOfC(item) += numItemsInHistoryView - 1
            rowSumsOfC(previousItem) -= numItemsInHistoryView - 1

            samplesOfA(user).set(k, item)

            itemInteractionCounts(item) += 1
            itemInteractionCounts(previousItem) -= 1
            numInteractionsSampled += 1

            itemsToRescore.add(item)
          }

        }
      }
    }

    val tasks = new java.util.ArrayList[Callable[Boolean]](itemsToRescore.size())
    val itemsToRescoreIterator = itemsToRescore.iterator()
    while (itemsToRescoreIterator.hasNext) {
      tasks.add(new Rescorer(itemsToRescoreIterator.nextInt()))
    }

    val results = executorService.invokeAll(tasks)

    var numChanges = 0
    val resultsIterator = results.iterator()
    while (resultsIterator.hasNext) {
      if (resultsIterator.next().get) {
        numChanges += 1
      }
    }

    val duration = System.currentTimeMillis() - start
    (duration, numChanges)
  }

  class Rescorer(item: Int) extends Callable[Boolean] {

    override def call(): Boolean = {

      //TODO can we reuse the queue?
      val queue = new PriorityQueue[(Int, Double)](k) {
        override protected def lessThan(a: (Int, Double), b: (Int, Double)): Boolean = { a._2 < b._2 }
      }

      val cooccurrencesOfItem = C(item).int2IntEntrySet()
      val particularCooccurrences = cooccurrencesOfItem.fastIterator()

      while (particularCooccurrences.hasNext) {
        val entry = particularCooccurrences.next()
        val otherItem = entry.getIntKey

        val k11 = entry.getIntValue.toLong
        val k12 = rowSumsOfC(item).toLong - k11
        val k21 = rowSumsOfC(otherItem) - k11
        val k22 = numCooccurrencesObserved + k11 - k12 - k21

        val score = Loglikelihood.logLikelihoodRatio(k11, k12, k21, k22)

        if (queue.size < k) {
          queue.add(otherItem -> score)
        } else if (score > queue.top()._2) {
          queue.updateTop(otherItem -> score)
        }
      }

      val previousTopK = indicators(item)

      var changeDetected = false

      if (queue.size != previousTopK.size) {
        changeDetected = true
      } else {

        val topKIterator = queue.iterator()
        while (topKIterator.hasNext && !changeDetected) {
          val (otherItem, _) = topKIterator.next()
          if (previousTopK.contains(otherItem)) {
            changeDetected = true
          }
        }
      }

      if (changeDetected) {
        indicators(item).clear()
        val topKIterator = queue.iterator()
        while (topKIterator.hasNext) {
          indicators(item).add(topKIterator.next()._1)
        }
      }

      changeDetected
    }
  }

}
