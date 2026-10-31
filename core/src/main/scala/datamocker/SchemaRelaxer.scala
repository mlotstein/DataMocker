package datamocker

import org.apache.spark.sql.types._

object SchemaRelaxer {

  /** Only the three container types are special; every other type is a leaf and passes through. */
  def relax(dt: DataType): DataType = dt match {
    case s: StructType => relax(s)
    case a: ArrayType  => ArrayType(relax(a.elementType), containsNull = true)
    case m: MapType    => MapType(relax(m.keyType), relax(m.valueType), valueContainsNull = true)
    case other         => other
  }

  def relax(s: StructType): StructType =
    StructType(s.fields.map(f => f.copy(dataType = relax(f.dataType), nullable = true)))

  // Probe: a type pattern (not `case StringType`) so collated strings in 4.x also match.
  def isString(dt: DataType): Boolean = dt match {
    case _: StringType => true
    case _             => false
  }
}
