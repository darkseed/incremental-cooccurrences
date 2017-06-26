package io.ssc.incrementalcooccurrences

import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, IntArrayList, IntArraySet}

class IncrementalCooccurrenceAnalysis(
    interactions: TraversableOnce[Interaction],
    numUsers: Int,
    numItems: Int,
    fMax: Int,
    kMax: Int,
    k: Int,
    seed: Int) {

  val random = new scala.util.Random(0xcafe)

  /* sampled user histories */
  val samplesOfA = Array.fill[IntArrayList](numUsers) { new IntArrayList(10) }
  val userInteractionCounts = Array.ofDim[Int](numUsers)
  val itemInteractionCounts = Array.ofDim[Int](numItems)

  /* cooccurrence matrix */
  val C = Array.fill[Int2IntOpenHashMap](numItems) { new Int2IntOpenHashMap(10) }
  /* rows sums of the cooccurrence matrix */
  val rowSumsOfC = Array.ofDim[Int](numItems)


  val indicators = Array.fill[IntArraySet](numItems) { new IntArraySet(k) }

  implicit val COMPARE_SCORES = new Ordering[(Int, Double)] {
    override def compare(x: (Int, Double), y: (Int, Double)): Int = {
      x._2.compareTo(y._2)
    }
  }

  var numInteractionsObserved = 0
  var numInteractionsSampled = 0
  var numCooccurrencesObserved = 0L

  def process(): Unit = {

    val start = System.currentTimeMillis()

    interactions.foreach { interaction =>

      val user = interaction.user
      val item = interaction.item

      numInteractionsObserved += 1

      if (numInteractionsObserved % 10000 == 0) {
        val duration = System.currentTimeMillis() - start

        println(s"Processed ${numInteractionsObserved} interactions... " +
          s"interactions per ms: ${numInteractionsObserved.toDouble / duration}")
      }

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

          rescore(item)
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

            rescore(item)
          }

        }
      }
    }

  }

  private[this] def rescore(item: Int) {

    val queue = new BoundedPriorityQueue[(Int, Double)](k)(COMPARE_SCORES)

    val particularCooccurrences = C(item).int2IntEntrySet().fastIterator()

    while (particularCooccurrences.hasNext) {
      val entry = particularCooccurrences.next()
      val otherItem = entry.getIntKey

      val k11 = entry.getIntValue.toLong
      val k12 = rowSumsOfC(item).toLong - k11
      val k21 = rowSumsOfC(otherItem) - k11
      val k22 = numCooccurrencesObserved + k11 - k12 - k21

      val score = Loglikelihood.logLikelihoodRatio(k11, k12, k21, k22)

      queue += otherItem -> score
    }

    val topK = new IntArraySet(k)

    queue.view.foreach { case  (otherItem, _) => topK.add(otherItem) }

    val previousTopK = indicators(item)

    if (topK.size() != previousTopK.size() || !topK.retainAll(previousTopK) ) {
      //println(s"Change for ${item}")
    } else {
      //println(s"No Change for ${item}")
    }
  }

}
