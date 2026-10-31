package datamocker

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

/** Opt-in extension methods: `import datamocker.syntax._`. */
object syntax {
  implicit class StructTypeMockOps(private val schema: StructType) extends AnyVal {

    /** Starts a [[DataMocker]] for this schema. */
    def mocker: DataMocker = DataMocker(schema)

    /**
     * Builds a DataFrame with one row per argument. Each row is a collection of `path -> value` pairs,
     * e.g. a `Map` or a `Seq` of tuples.
     * {{{
     *   schema.mockDF(spark)(Map("$.id" -> 1L), Map("$.id" -> 2L))
     * }}}
     */
    def mockDF(spark: SparkSession)(rows: Iterable[(String, Any)]*): DataFrame =
      rows.foldLeft(DataMocker(schema))((m, r) => m.row(r.toSeq: _*)).build(spark)
  }
}
