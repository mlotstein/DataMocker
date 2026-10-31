package datamocker

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

/** Guards against complexity regressions. Exponent ~1 is linear; these fail well before quadratic (~2). */
class PerformanceSpec extends AnyFunSuite {

  private val MaxExponent = 1.5
  private val sizes = Seq(20000, 40000, 80000, 160000, 320000)

  private def nested(n: Int) = StructType((0 until n).map(i =>
    StructField(s"f$i", StructType(Seq(StructField("a", StructType(Seq(
      StructField("b", IntegerType, false), StructField("c", ArrayType(IntegerType, false), false))), false))), false)))

  test("the detector itself flags quadratic work") {
    val k = Scaling.exponent(Seq(1000, 2000, 4000, 8000, 16000)) { n =>
      val a = Array.fill(n)(1); () => { var t = 0L; var i = 0; while (i < n) { var j = 0; while (j < n) { t += a(i) * a(j); j += 1 }; i += 1 }; t }
    }
    info(f"quadratic control exponent: $k%.2f")
    assert(k > 1.7)
  }

  test("toRows scales linearly with the number of paths and schema width") {
    val k = Scaling.exponent(sizes) { n =>
      val s = nested(n)
      val entries = (0 until n).flatMap(i => Seq(s"$$.f$i.a.b" -> i, s"$$.f$i.a.c[3]" -> i))
      () => DataMocker(s).row(entries: _*).toRows
    }
    info(f"toRows exponent: $k%.2f")
    assert(k < MaxExponent)
  }

  test("relaxing a schema scales linearly") {
    val k = Scaling.exponent(sizes) { n => val s = nested(n); () => SchemaRelaxer.relax(s) }
    info(f"relax exponent: $k%.2f")
    assert(k < MaxExponent)
  }

  test("one very wide flat struct scales linearly") {
    val k = Scaling.exponent(sizes) { n =>
      val s = StructType((0 until n).map(i => StructField(s"c$i", LongType, false)))
      val entries = (0 until n).map(i => s"$$.c$i" -> i)
      () => DataMocker(s).row(entries: _*).toRows
    }
    info(f"flat exponent: $k%.2f")
    assert(k < MaxExponent)
  }

  test("many rows scales linearly") {
    val s = nested(10)
    val k = Scaling.exponent(Seq(20000, 40000, 80000, 160000)) { n =>
      val m = (0 until n).foldLeft(DataMocker(s))((m, i) => m.row("$.f1.a.b" -> i))
      () => m.toRows
    }
    info(f"rows exponent: $k%.2f")
    assert(k < MaxExponent)
  }

  test("cost does not depend on the size of parts of the schema the paths never touch") {
    val k = Scaling.exponent(Seq(10000, 20000, 40000, 80000, 160000)) { n =>
      val s = StructType(Seq(
        StructField("target", StructType(Seq(StructField("x", IntegerType, false))), false),
        StructField("other", nested(n), false)))
      () => DataMocker(s).row("$.target.x" -> 1).toRows
    }
    info(f"untouched-schema exponent: $k%.2f")
    assert(k < 0.5)
  }

  test("path insertion and row building are not limited by nesting depth") {
    val depth = 50000
    val s = (0 until depth).foldLeft(StructType(Seq(StructField("leaf", IntegerType, false)))) { (inner, _) =>
      StructType(Seq(StructField("n", inner, false)))
    }
    val errors = scala.collection.mutable.Buffer.empty[String]
    val row = RowAssembler.assemble(s, Seq(("$" + ".n" * depth + ".leaf") -> 1), "row 0", errors)
    assert(errors.isEmpty && row.length == 1)
  }

  test("absurdly deep nesting fails with a clear error instead of a raw StackOverflowError") {
    val depth = 2000000
    val s = (0 until depth).foldLeft(StructType(Seq(StructField("leaf", IntegerType, false)))) { (inner, _) =>
      StructType(Seq(StructField("n", inner, false)))
    }
    val ex = intercept[IllegalArgumentException](DataMocker(s).relaxedSchema)
    assert(ex.getMessage.contains("too deep"))
  }
}
