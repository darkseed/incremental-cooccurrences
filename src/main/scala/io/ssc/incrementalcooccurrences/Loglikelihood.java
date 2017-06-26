package io.ssc.incrementalcooccurrences;

//import com.google.common.base.Preconditions;

public class Loglikelihood {

  public static double logLikelihoodRatio(long k11, long k12, long k21, long k22) {
    //Preconditions.checkArgument(k11 >= 0 && k12 >= 0 && k21 >= 0 && k22 >= 0,
    //    //TODO this string should not be built every time
    //    "k11: " + k11 + ", k12: " + k12 + ", k21: " + k21 + ", k22: " + k22);
    // note that we have counts here, not probabilities, and that the entropy is not normalized.
    double rowEntropy = entropy(k11 + k12, k21 + k22);
    double columnEntropy = entropy(k11 + k21, k12 + k22);
    double matrixEntropy = entropy(k11, k12, k21, k22);
    if (rowEntropy + columnEntropy < matrixEntropy) {
      // round off error
      return 0.0;
    }
    return 2.0 * (rowEntropy + columnEntropy - matrixEntropy);
  }

  private static double xLogX(long x) {
    return x == 0 ? 0.0 : x * Math.log(x);
  }

  private static double entropy(long a, long b) {
    return xLogX(a + b) - xLogX(a) - xLogX(b);
  }

  private static double entropy(long a, long b, long c, long d) {
    return xLogX(a + b + c + d) - xLogX(a) - xLogX(b) - xLogX(c) - xLogX(d);
  }
}
