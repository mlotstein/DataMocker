package datamocker

/** Helpers for scale-invariant performance checks: machine speed cancels out of a fitted exponent. */
object Scaling {

  private def seconds(f: () => Any): Double = {
    val s = System.nanoTime(); f(); (System.nanoTime() - s) / 1e9
  }

  /**
   * Fits time ~ size^k by least squares on log-log data and returns k (about 1 for linear code, 2 for
   * quadratic). `setup(n)` builds the input for size n and returns the work to time, so setup is excluded.
   */
  def exponent(sizes: Seq[Int], reps: Int = 3)(setup: Int => () => Any): Double = {
    val warm = setup(sizes.head)
    (0 until 3).foreach(_ => warm())
    val points = sizes.map { n =>
      val work = setup(n)
      System.gc()
      (math.log(n.toDouble), math.log(math.max((0 until reps).map(_ => seconds(work)).min, 1e-6)))
    }
    val n = points.size.toDouble
    val (sx, sy) = (points.map(_._1).sum, points.map(_._2).sum)
    val sxy = points.map(p => p._1 * p._2).sum
    val sxx = points.map(p => p._1 * p._1).sum
    (n * sxy - sx * sy) / (n * sxx - sx * sx)
  }
}
