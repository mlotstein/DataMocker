package datamocker

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class VersionSpecificSpec extends AnyFunSuite {
  test("3.5: string is a leaf and matches isString") {
    assert(SchemaRelaxer.isString(StringType))
  }
}
