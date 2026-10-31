package datamocker

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class VersionSpecificSpec extends AnyFunSuite {
  test("4.x: collated string matches isString via type pattern") {
    assert(SchemaRelaxer.isString(StringType("UTF8_LCASE")))
  }
  test("4.x: VariantType is treated as a leaf") {
    assert(SchemaRelaxer.relax(VariantType) == VariantType)
  }
}
