package datamocker

import datamocker.PathSegment._
import org.scalatest.funsuite.AnyFunSuite

class JsonPathSpec extends AnyFunSuite {

  private def segs(p: String) = JsonPath.parse(p).segments

  test("root only") {
    assert(segs("$").isEmpty)
  }

  test("dotted fields") {
    assert(segs("$.a.b.c") == Vector(Name("a"), Name("b"), Name("c")))
  }

  test("array indexes") {
    assert(segs("$.items[0].sku") == Vector(Name("items"), Index(0), Name("sku")))
    assert(segs("$[3]") == Vector(Index(3)))
    assert(segs("$.a[1][2]") == Vector(Name("a"), Index(1), Index(2)))
  }

  test("quoted names with either quote style") {
    assert(segs("$.props['k'].id") == Vector(Name("props"), Name("k"), Name("id")))
    assert(segs("""$["k"]""") == Vector(Name("k")))
  }

  test("quoted names may contain dots, brackets and spaces") {
    assert(segs("$['a.b']['c[0]']['d e']") == Vector(Name("a.b"), Name("c[0]"), Name("d e")))
  }

  test("escapes inside quoted names") {
    assert(segs("""$['it\'s']""") == Vector(Name("it's")))
    assert(segs("""$['back\\slash']""") == Vector(Name("back\\slash")))
    assert(segs("""$["say \"hi\""]""") == Vector(Name("say \"hi\"")))
  }

  test("empty quoted name is allowed (empty map key)") {
    assert(segs("$['']") == Vector(Name("")))
  }

  test("toString round-trips") {
    val paths = Seq("$", "$.a.b", "$.items[2].sku", "$['a.b'][0]", """$['it\'s']""")
    paths.foreach { p =>
      val parsed = JsonPath.parse(p)
      assert(JsonPath.parse(parsed.toString) == parsed)
    }
  }

  private def err(p: String): PathParseException = intercept[PathParseException](JsonPath.parse(p))

  test("rejects missing root") {
    assert(err("a.b").position == 0)
    assert(err("").position == 0)
  }

  test("rejects unsupported JSONPath features with a clear message") {
    assert(err("$.a.*").getMessage.contains("wildcards"))
    assert(err("$.a[*]").getMessage.contains("wildcards"))
    assert(err("$..a").getMessage.contains("recursive descent"))
    assert(err("$.a[-1]").getMessage.contains("negative"))
  }

  test("rejects malformed paths and reports position") {
    assert(err("$.").position == 2)
    assert(err("$.a.").position == 4)
    assert(err("$.a[").position == 4)
    assert(err("$.a[0").position == 5)
    assert(err("$.a['x'").position == 7)
    assert(err("$.a['x").getMessage.contains("unterminated"))
    assert(err("$.a b[").position == 6)
    assert(err("$a").position == 1)
    assert(err("$.a[x]").position == 4)
    assert(err("""$['a\n']""").getMessage.contains("escape"))
  }

  test("rejects an index that overflows Int") {
    assert(err("$.a[99999999999]").getMessage.contains("too large"))
  }

  test("parsing is linear: a very long path parses quickly") {
    val long = "$" + (0 until 200000).map(i => s".f$i").mkString
    val start = System.nanoTime()
    assert(JsonPath.parse(long).segments.size == 200000)
    assert((System.nanoTime() - start) / 1e9 < 5.0)
  }
}
