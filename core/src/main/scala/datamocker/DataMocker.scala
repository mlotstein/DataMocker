package datamocker

import scala.collection.mutable

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * Builds DataFrames for tests from a schema and sparse `(JSONPath -> value)` entries.
 * Everything not mentioned is null; the resulting DataFrame has the schema with all nullability relaxed.
 *
 * {{{
 *   DataMocker(schema)
 *     .row("$.id" -> 1L, "$.items[1].sku" -> "a")
 *     .row("$.id" -> 2L)
 *     .build(spark)
 * }}}
 */
final class DataMocker private (val schema: StructType, entries: Vector[Seq[(String, Any)]]) {

  /** Adds a row. Paths and values are validated when the rows are built, and all problems reported at once. */
  def row(values: (String, Any)*): DataMocker = new DataMocker(schema, entries :+ values)

  /** The schema of the resulting DataFrame: the input schema with every level nullable. */
  lazy val relaxedSchema: StructType = DataMocker.guardDepth(SchemaRelaxer.relax(schema))

  /** Validates and assembles the rows. Throws [[MockSpecException]] listing every problem found. */
  def toRows: Seq[Row] = {
    val errors = mutable.ArrayBuffer.empty[String]
    val rows = DataMocker.guardDepth {
      entries.zipWithIndex.map { case (e, i) => RowAssembler.assemble(schema, e, s"row $i", errors) }
    }
    if (errors.nonEmpty) throw new MockSpecException(errors.toVector)
    rows
  }

  def build(spark: SparkSession): DataFrame = DataMocker.build(spark, relaxedSchema, toRows)
}

object DataMocker {
  def apply(schema: StructType): DataMocker = new DataMocker(schema, Vector.empty)

  /** Turns a stack overflow from extremely deep nesting into an actionable error. */
  private[datamocker] def guardDepth[A](body: => A): A =
    try body
    catch {
      case _: StackOverflowError =>
        throw new IllegalArgumentException("Schema or value nesting is too deep to process (stack overflow)")
    }

  /** Version-sensitive shim: the only place that touches SparkSession. */
  def build(spark: SparkSession, schema: StructType, rows: Seq[Row]): DataFrame =
    spark.createDataFrame(CollCompat.toJava(rows), SchemaRelaxer.relax(schema))

  /** Same, but with the schema untouched (used to document why relaxing is needed). */
  private[datamocker] def buildStrict(spark: SparkSession, schema: StructType, rows: Seq[Row]): DataFrame =
    spark.createDataFrame(CollCompat.toJava(rows), schema)
}
