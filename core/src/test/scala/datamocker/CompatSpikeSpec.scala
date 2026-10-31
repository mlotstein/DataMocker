package datamocker

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class CompatSpikeSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark = SparkSession.builder()
    .master("local[1]")
    .appName("compat-spike")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private val schema = StructType(Seq(
    StructField("id", LongType, nullable = false),
    StructField("address", StructType(Seq(
      StructField("zip", StringType, nullable = false),
      StructField("geo", StructType(Seq(StructField("lat", DoubleType, nullable = false))), nullable = false)
    )), nullable = false),
    StructField("items", ArrayType(StructType(Seq(StructField("sku", StringType, nullable = false))), containsNull = false), nullable = false),
    StructField("props", MapType(StringType, StructType(Seq(StructField("id", IntegerType, nullable = false))), valueContainsNull = false), nullable = false)
  ))

  private def allNullable(dt: DataType): Boolean = dt match {
    case s: StructType => s.fields.forall(f => f.nullable && allNullable(f.dataType))
    case a: ArrayType  => a.containsNull && allNullable(a.elementType)
    case m: MapType    => m.valueContainsNull && allNullable(m.keyType) && allNullable(m.valueType)
    case _             => true
  }

  test("spark version matches the source directory line") {
    assert(spark.version.startsWith(SparkLine.line))
  }

  test("relaxer makes every level nullable and keeps names/types") {
    val relaxed = SchemaRelaxer.relax(schema)
    assert(allNullable(relaxed))
    assert(relaxed.fieldNames.toSeq == schema.fieldNames.toSeq)
  }

  test("built DataFrame has exactly the relaxed schema, with sparse nested nulls") {
    val rows = Seq(
      Row(1L, null, Seq(null, Row("a")), Map("k" -> null)),
      Row(null, Row(null, Row(1.5)), null, Map("x" -> Row(7)))
    )
    val df = DataMocker.build(spark, schema, rows)
    assert(df.schema == SchemaRelaxer.relax(schema))
    val out = df.collect()
    assert(out.length == 2)
    assert(out(0).isNullAt(1))
    assert(out(0).getSeq[Row](2)(0) == null)
    assert(out(0).getSeq[Row](2)(1).getString(0) == "a")
    assert(out(1).getStruct(1).getStruct(1).getDouble(0) == 1.5)
  }

  test("strict schema rejects a null in a non-nullable field") {
    val rows = Seq(Row(null, null, null, null))
    intercept[Exception] {
      DataMocker.buildStrict(spark, schema, rows).collect()
    }
  }
}
