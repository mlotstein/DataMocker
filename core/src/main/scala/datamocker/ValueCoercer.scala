package datamocker

import java.math.{BigDecimal => JBigDecimal, RoundingMode}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/** Thrown for a value that does not fit its target type; collected into [[MockSpecException]]. */
private[datamocker] final class CoercionException(msg: String) extends RuntimeException(msg, null, false, false)

/**
 * Validates and normalizes user-supplied values against a Spark type, without relying on Spark casts
 * (whose behavior depends on ANSI mode and so differs between versions).
 *
 * Leaf types without a rule here (variant, intervals, UDTs, ...) pass through unchanged and Spark decides.
 */
private[datamocker] object ValueCoercer {

  private def fail(dt: DataType, v: Any): Nothing =
    throw new CoercionException(s"expected ${dt.simpleString} but got ${v.getClass.getName} ($v)")

  private def integral(dt: DataType, v: Any, min: Long, max: Long): Long = {
    val l = v match {
      case b: Byte  => b.toLong
      case s: Short => s.toLong
      case i: Int   => i.toLong
      case l: Long  => l
      case _        => fail(dt, v)
    }
    if (l < min || l > max) throw new CoercionException(s"value $l is out of range for ${dt.simpleString}")
    l
  }

  def coerce(dt: DataType, v: Any): Any =
    if (v == null) null
    else dt match {
      case ByteType    => integral(dt, v, Byte.MinValue, Byte.MaxValue).toByte
      case ShortType   => integral(dt, v, Short.MinValue, Short.MaxValue).toShort
      case IntegerType => integral(dt, v, Int.MinValue, Int.MaxValue).toInt
      case LongType    => integral(dt, v, Long.MinValue, Long.MaxValue)

      case FloatType => v match {
        case f: Float => f
        case b: Byte  => b.toFloat
        case s: Short => s.toFloat
        case _        => fail(dt, v)
      }
      case DoubleType => v match {
        case d: Double => d
        case f: Float  => f.toDouble
        case b: Byte   => b.toDouble
        case s: Short  => s.toDouble
        case i: Int    => i.toDouble
        case _         => fail(dt, v)
      }

      case d: DecimalType =>
        val bd: JBigDecimal = v match {
          case b: JBigDecimal      => b
          case b: BigDecimal       => b.bigDecimal
          case b: Byte             => JBigDecimal.valueOf(b.toLong)
          case s: Short            => JBigDecimal.valueOf(s.toLong)
          case i: Int              => JBigDecimal.valueOf(i.toLong)
          case l: Long             => JBigDecimal.valueOf(l)
          case _                   => fail(dt, v)
        }
        val scaled =
          try bd.setScale(d.scale, RoundingMode.UNNECESSARY)
          catch { case _: ArithmeticException => throw new CoercionException(s"$bd cannot be represented as ${d.simpleString} without rounding") }
        if (scaled.precision > d.precision) throw new CoercionException(s"$bd does not fit ${d.simpleString}")
        scaled

      case _: StringType => v match { case s: String => s; case _ => fail(dt, v) }
      case BooleanType   => v match { case b: Boolean => b; case _ => fail(dt, v) }
      case BinaryType    => v match { case b: Array[Byte] => b; case _ => fail(dt, v) }

      case DateType => v match {
        case d: java.sql.Date    => d
        case d: java.time.LocalDate => d
        case _                   => fail(dt, v)
      }
      case TimestampType => v match {
        case t: java.sql.Timestamp => t
        case t: java.time.Instant  => t
        case _                     => fail(dt, v)
      }
      case TimestampNTZType => v match { case t: java.time.LocalDateTime => t; case _ => fail(dt, v) }

      case st: StructType => coerceStruct(st, v)
      case at: ArrayType  => coerceArray(at, v)
      case mt: MapType    => coerceMap(mt, v)

      case _ => v
    }

  /** O(1) after the first call (Spark caches the name index per StructType); -1 if absent. */
  def fieldIndexOf(st: StructType, name: String): Int =
    try st.fieldIndex(name)
    catch { case _: IllegalArgumentException => -1 }

  private def coerceStruct(st: StructType, v: Any): Row = v match {
    case r: Row =>
      if (r.length != st.length) throw new CoercionException(s"expected a Row of ${st.length} fields but got ${r.length}")
      Row.fromSeq(Vector.tabulate(st.length)(i => within(st(i).name, coerce(st(i).dataType, r.get(i)))))
    case m: scala.collection.Map[_, _] =>
      val out = new Array[Any](st.length)
      m.foreach { case (k, value) =>
        val name = k.toString
        val i = fieldIndexOf(st, name)
        if (i < 0) throw new CoercionException(s"no field '$name' in ${st.simpleString}")
        out(i) = within(name, coerce(st(i).dataType, value))
      }
      Row.fromSeq(out.toVector)
    case _ => fail(st, v)
  }

  private def coerceArray(at: ArrayType, v: Any): Vector[Any] = {
    val seq: scala.collection.Seq[Any] = v match {
      case s: scala.collection.Seq[_] => s.asInstanceOf[scala.collection.Seq[Any]]
      case a: Array[_]                => a.toSeq.asInstanceOf[scala.collection.Seq[Any]]
      case _                          => fail(at, v)
    }
    var i = -1
    seq.iterator.map { e => i += 1; within(s"[$i]", coerce(at.elementType, e)) }.toVector
  }

  private def coerceMap(mt: MapType, v: Any): Map[Any, Any] = v match {
    case m: scala.collection.Map[_, _] =>
      m.iterator.map { case (k, value) =>
        if (k == null) throw new CoercionException("map keys cannot be null")
        val ck = coerce(mt.keyType, k)
        (ck, within(s"['$k']", coerce(mt.valueType, value)))
      }.toMap
    case _ => fail(mt, v)
  }

  // Error context for nested values is added lazily, only when a failure actually occurs.
  private def within[A](where: String, body: => A): A =
    try body
    catch { case e: CoercionException => throw new CoercionException(s"at $where: ${e.getMessage}") }

  /** Converts a path name segment into a map key of the given type. */
  def keyFromName(kt: DataType, name: String): Any = kt match {
    case _: StringType => name
    case ByteType | ShortType | IntegerType | LongType =>
      val l = try name.toLong catch { case _: NumberFormatException => throw new CoercionException(s"map key '$name' is not a valid ${kt.simpleString}") }
      coerce(kt, l)
    case BooleanType =>
      name match {
        case "true"  => true
        case "false" => false
        case _       => throw new CoercionException(s"map key '$name' is not a valid boolean")
      }
    case _ => throw new CoercionException(s"paths cannot address maps with ${kt.simpleString} keys; assign the whole map instead")
  }
}
