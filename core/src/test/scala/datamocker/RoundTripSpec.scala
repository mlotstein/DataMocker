package datamocker

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import datamocker.syntax._

class RoundTripSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark = SparkSession.builder()
    .master("local[1]")
    .appName("round-trip")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private def field(n: String, t: DataType) = StructField(n, t, nullable = false)

  private val schema = StructType(Seq(
    field("b", ByteType), field("s", ShortType), field("i", IntegerType), field("l", LongType),
    field("f", FloatType), field("d", DoubleType), field("dec", DecimalType(10, 2)),
    field("str", StringType), field("bool", BooleanType), field("bin", BinaryType),
    field("date", DateType), field("ts", TimestampType),
    field("ntz", TimestampNTZType),
    field("nested", StructType(Seq(field("arr", ArrayType(DecimalType(5, 1), false)), field("m", MapType(StringType, DateType, false)))))
  ))

  test("every supported leaf type survives a createDataFrame/collect round trip") {
    val row = Map[String, Any](
      "$.b" -> 1.toByte, "$.s" -> 2.toShort, "$.i" -> 3, "$.l" -> 4,
      "$.f" -> 1.5f, "$.d" -> 2.5, "$.dec" -> BigDecimal("12.30"),
      "$.str" -> "x", "$.bool" -> true, "$.bin" -> Array[Byte](1, 2),
      "$.date" -> java.time.LocalDate.of(2020, 1, 2),
      "$.ts" -> java.time.Instant.parse("2020-01-01T00:00:00Z"),
      "$.ntz" -> java.time.LocalDateTime.of(2020, 1, 1, 12, 0),
      "$.nested.arr[1]" -> BigDecimal("1.5"),
      "$.nested.m['k']" -> java.sql.Date.valueOf("2021-03-04")
    )
    val df = schema.mockDF(spark)(row)
    assert(df.schema == SchemaRelaxer.relax(schema))
    val r = df.collect().head
    assert(r.getByte(0) == 1 && r.getShort(1) == 2 && r.getInt(2) == 3 && r.getLong(3) == 4L)
    assert(r.getFloat(4) == 1.5f && r.getDouble(5) == 2.5)
    assert(r.getDecimal(6).toPlainString == "12.30")
    assert(r.getString(7) == "x" && r.getBoolean(8) && r.getAs[Array[Byte]](9).toSeq == Seq[Byte](1, 2))
    assert(r.getDate(10).toString == "2020-01-02")
    assert(r.getTimestamp(11).toInstant == java.time.Instant.parse("2020-01-01T00:00:00Z"))
    assert(r.get(12).toString == "2020-01-01T12:00")
    val nested = r.getStruct(13)
    assert(nested.getSeq[java.math.BigDecimal](0)(1).toPlainString == "1.5")
    assert(nested.getSeq[java.math.BigDecimal](0)(0) == null)
    assert(nested.getMap[String, java.sql.Date](1)("k").toString == "2021-03-04")
  }

  test("a mocked DataFrame can be used in real Spark queries") {
    val s = StructType(Seq(field("id", LongType), field("tags", ArrayType(StringType, false))))
    val df = s.mockDF(spark)(Map("$.id" -> 1, "$.tags[0]" -> "a"), Map("$.id" -> 2), Map.empty[String, Any])
    df.createOrReplaceTempView("t")
    assert(spark.sql("select count(*) from t where id is null").collect().head.getLong(0) == 1)
    assert(spark.sql("select sum(id) from t").collect().head.getLong(0) == 3)
    assert(spark.sql("select explode(tags) from t").count() == 1)
  }

  test("syntax: mocker returns a builder for the same schema") {
    assert(schema.mocker.schema eq schema)
  }

  test("syntax: errors surface through mockDF") {
    val ex = intercept[MockSpecException](schema.mockDF(spark)(Map("$.nope" -> 1)))
    assert(ex.errors.size == 1)
  }
}
