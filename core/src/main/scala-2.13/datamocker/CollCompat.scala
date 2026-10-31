package datamocker

import scala.jdk.CollectionConverters._

private[datamocker] object CollCompat {
  def toJava[A](s: Seq[A]): java.util.List[A] = s.asJava
}
