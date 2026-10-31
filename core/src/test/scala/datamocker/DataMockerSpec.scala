package datamocker

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class DataMockerSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark = SparkSession.builder()
    .master("local[1]")
    .appName("data-mocker-spec")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private def struct(fields: (String, DataType)*) =
    StructType(fields.map { case (n, t) => StructField(n, t, nullable = false) })

  private val schema = struct(
    "id" -> LongType,
    "small" -> ShortType,
    "price" -> DecimalType(10, 2),
    "address" -> struct("zip" -> StringType, "geo" -> struct("lat" -> DoubleType, "lon" -> DoubleType)),
    "items" -> ArrayType(struct("sku" -> StringType, "tags" -> ArrayType(StringType, containsNull = false)), containsNull = false),
    "props" -> MapType(StringType, struct("id" -> IntegerType, "vals" -> ArrayType(IntegerType)), valueContainsNull = false),
    "byInt" -> MapType(IntegerType, StringType, valueContainsNull = false),
    "when" -> TimestampType,
    "day" -> DateType
  )

  private def rows(entries: (String, Any)*): Row = DataMocker(schema).row(entries: _*).toRows.head
  private def errorsOf(rowsOfEntries: Seq[(String, Any)]*): Seq[String] = {
    val ex = intercept[MockSpecException] {
      rowsOfEntries.foldLeft(DataMocker(schema))((m, e) => m.row(e: _*)).toRows
    }
    ex.errors
  }

  test("unmentioned fields are null") {
    val r = rows()
    assert(r.length == schema.length)
    assert((0 until r.length).forall(r.isNullAt))
  }

  test("scalar fields, with Int widened to Long") {
    val r = rows("$.id" -> 5, "$.small" -> 7)
    assert(r.getLong(0) == 5L)
    assert(r.getShort(1) == 7.toShort)
  }

  test("a deep path creates its ancestors and leaves siblings null") {
    val r = rows("$.address.geo.lat" -> 1.5)
    val geo = r.getStruct(3).getStruct(1)
    assert(geo.getDouble(0) == 1.5)
    assert(geo.isNullAt(1))
    assert(r.getStruct(3).isNullAt(0))
  }

  test("array indexes pad with nulls, including in nested arrays") {
    val r = rows("$.items[2].sku" -> "c", "$.items[0].tags[1]" -> "t")
    val items = r.getSeq[Row](4)
    assert(items.length == 3)
    assert(items(1) == null)
    assert(items(2).getString(0) == "c")
    assert(items(0).getSeq[String](1) == Seq(null, "t"))
  }

  test("map values by string key and by integer key") {
    val r = rows("$.props['a'].id" -> 1, "$.props['b'].vals[1]" -> 9, "$.byInt['42']" -> "x", "$.byInt[\"7\"]" -> "y")
    val props = r.getMap[String, Row](5)
    assert(props("a").getInt(0) == 1)
    assert(props("b").getSeq[Int](1) == Seq(null, 9))
    assert(r.getMap[Int, String](6) == Map(42 -> "x", 7 -> "y"))
  }

  test("whole structs, arrays and maps can be assigned and are validated recursively") {
    val r = rows(
      "$.address" -> Row("94110", Row(1.0, 2.0)),
      "$.items" -> Seq(Map("sku" -> "a"), Row("b", Seq("t"))),
      "$.props" -> Map("k" -> Row(1, Seq(1, 2)))
    )
    assert(r.getStruct(3).getString(0) == "94110")
    assert(r.getSeq[Row](4)(0).getString(0) == "a")
    assert(r.getSeq[Row](4)(1).getSeq[String](1) == Seq("t"))
    assert(r.getMap[String, Row](5)("k").getSeq[Int](1) == Seq(1, 2))
  }

  test("explicit null is allowed anywhere") {
    val r = rows("$.id" -> null, "$.address" -> null)
    assert(r.isNullAt(0) && r.isNullAt(3))
  }

  test("decimals accept exact values and reject rounding or overflow") {
    assert(rows("$.price" -> BigDecimal("1.5")).getDecimal(2).toPlainString == "1.50")
    assert(rows("$.price" -> 3).getDecimal(2).toPlainString == "3.00")
    assert(errorsOf(Seq("$.price" -> BigDecimal("1.234"))).head.contains("without rounding"))
    assert(errorsOf(Seq("$.price" -> BigDecimal("123456789.00"))).head.contains("does not fit"))
    assert(errorsOf(Seq("$.price" -> 1.5)).head.contains("expected decimal"))
  }

  test("dates and timestamps accept java.sql and java.time values") {
    val r = rows(
      "$.when" -> java.time.Instant.parse("2020-01-01T00:00:00Z"),
      "$.day" -> java.time.LocalDate.of(2020, 1, 2)
    )
    assert(r.get(7) != null && r.get(8) != null)
    val r2 = rows("$.when" -> new java.sql.Timestamp(0L), "$.day" -> java.sql.Date.valueOf("2020-01-02"))
    assert(r2.get(7) != null && r2.get(8) != null)
  }

  test("type and range mismatches are rejected") {
    assert(errorsOf(Seq("$.id" -> "x")).head.contains("expected bigint"))
    assert(errorsOf(Seq("$.small" -> 70000)).head.contains("out of range"))
    assert(errorsOf(Seq("$.address.zip" -> 1)).head.contains("expected string"))
    assert(errorsOf(Seq("$.items" -> Seq(Map("nope" -> 1)))).head.contains("no field 'nope'"))
  }

  test("path problems against the schema") {
    assert(errorsOf(Seq("$.nope" -> 1)).head.contains("no field 'nope'"))
    assert(errorsOf(Seq("$.address.nope" -> 1)).head.contains("no field 'nope' at $['address']"))
    assert(errorsOf(Seq("$.id.x" -> 1)).head.contains("cannot descend into bigint"))
    assert(errorsOf(Seq("$.id[0]" -> 1)).head.contains("cannot descend into bigint"))
    assert(errorsOf(Seq("$.items.sku" -> "a")).head.contains("cannot select 'sku' on array"))
    assert(errorsOf(Seq("$.props[0]" -> 1)).head.contains("cannot index [0] on map"))
    assert(errorsOf(Seq("$.byInt['abc']" -> "x")).head.contains("not a valid int"))
    assert(errorsOf(Seq("$" -> 1)).head.contains("root"))
    assert(errorsOf(Seq("$.a.*" -> 1)).head.contains("wildcards"))
  }

  test("conflicting and duplicate writes are rejected") {
    assert(errorsOf(Seq("$.id" -> 1, "$.id" -> 2)).head.contains("conflicts"))
    assert(errorsOf(Seq("$.address" -> Row("z", Row(1.0, 2.0)), "$.address.zip" -> "q")).head.contains("conflicts"))
    assert(errorsOf(Seq("$.address.zip" -> "q", "$.address" -> Row("z", Row(1.0, 2.0)))).head.contains("conflicts"))
  }

  test("all problems across all rows are reported together, labelled by row") {
    val errs = errorsOf(Seq("$.nope" -> 1, "$.id" -> "x"), Seq("$.id" -> 1), Seq("$.small" -> 70000))
    assert(errs.size == 3)
    assert(errs.exists(_.startsWith("row 0")) && errs.exists(_.startsWith("row 2")))
    assert(!errs.exists(_.startsWith("row 1")))
  }

  test("the exception message lists the problems") {
    val ex = intercept[MockSpecException](DataMocker(schema).row("$.nope" -> 1, "$.id" -> "x").toRows)
    assert(ex.getMessage.contains("2 problems"))
  }

  test("DataFrame has exactly the relaxed schema and the expected data") {
    val df = DataMocker(schema)
      .row("$.id" -> 1, "$.address.geo.lat" -> 1.5, "$.items[1].sku" -> "b", "$.props['k'].id" -> 3)
      .row("$.id" -> 2)
      .build(spark)
    assert(df.schema == SchemaRelaxer.relax(schema))
    val out = df.collect()
    assert(out.length == 2)
    assert(out(0).getSeq[Row](4)(0) == null)
    assert(out(0).getSeq[Row](4)(1).getString(0) == "b")
    assert(out(0).getMap[String, Row](5)("k").getInt(0) == 3)
    assert(out(1).getLong(0) == 2L)
  }

  test("zero rows gives an empty DataFrame with the relaxed schema") {
    val df = DataMocker(schema).build(spark)
    assert(df.schema == SchemaRelaxer.relax(schema) && df.count() == 0)
  }

  test("scales to a very wide, deep schema and many paths") {
    val width = 20000
    val big = StructType((0 until width).map { i =>
      StructField(s"f$i", struct("a" -> struct("b" -> IntegerType, "c" -> ArrayType(IntegerType))), nullable = false)
    })
    val entries = (0 until width).flatMap(i => Seq(s"$$.f$i.a.b" -> i, s"$$.f$i.a.c[3]" -> i))
    val start = System.nanoTime()
    val r = DataMocker(big).row(entries: _*).toRows.head
    val secs = (System.nanoTime() - start) / 1e9
    assert(r.length == width)
    assert(r.getStruct(width - 1).getStruct(0).getInt(0) == width - 1)
    assert(secs < 10.0, s"took $secs s")
  }
}
