package datamocker

/** One step of a path. `Name` is resolved against the schema: a struct field or a map key. */
sealed trait PathSegment
object PathSegment {
  final case class Name(value: String) extends PathSegment
  final case class Index(value: Int) extends PathSegment
}

final case class JsonPath(segments: Vector[PathSegment]) {
  override def toString: String = segments.foldLeft("$") {
    case (acc, PathSegment.Name(n))  => acc + "['" + n.replace("\\", "\\\\").replace("'", "\\'") + "']"
    case (acc, PathSegment.Index(i)) => acc + "[" + i + "]"
  }
}

final class PathParseException(val path: String, val position: Int, reason: String)
    extends IllegalArgumentException(s"Invalid path '$path' at position $position: $reason")

/**
 * Parses the supported JSONPath subset:
 * {{{
 *   path    := '$' step*
 *   step    := '.' name | '[' quoted ']' | '[' digits ']'
 *   name    := any characters except '.' and '['
 *   quoted  := ''' chars ''' | '"' chars '"'      (backslash escapes the quote and itself)
 * }}}
 * Wildcards, negative indexes, slices, filters and recursive descent are rejected with a clear error.
 * Single pass, no backtracking: O(path length).
 */
object JsonPath {
  import PathSegment._

  def parse(path: String): JsonPath = {
    def fail(pos: Int, reason: String): Nothing = throw new PathParseException(path, pos, reason)

    if (path.isEmpty || path.charAt(0) != '$') fail(0, "path must start with '$'")

    val out = Vector.newBuilder[PathSegment]
    var i = 1
    val n = path.length

    while (i < n) {
      path.charAt(i) match {
        case '.' =>
          i += 1
          if (i < n && path.charAt(i) == '.') fail(i - 1, "recursive descent '..' is not supported")
          if (i < n && path.charAt(i) == '*') fail(i, "wildcards are not supported")
          val start = i
          while (i < n && path.charAt(i) != '.' && path.charAt(i) != '[') i += 1
          if (i == start) fail(start, "expected a field name after '.'")
          out += Name(path.substring(start, i))

        case '[' =>
          i += 1
          if (i >= n) fail(i, "unterminated '['")
          path.charAt(i) match {
            case q @ ('\'' | '"') =>
              i += 1
              val sb = new java.lang.StringBuilder
              var closed = false
              while (!closed) {
                if (i >= n) fail(i, s"unterminated quoted name, expected closing $q")
                val c = path.charAt(i)
                if (c == '\\') {
                  if (i + 1 >= n) fail(i, "dangling escape")
                  val e = path.charAt(i + 1)
                  if (e != q && e != '\\') fail(i, s"unsupported escape '\\$e'")
                  sb.append(e)
                  i += 2
                } else if (c == q) {
                  closed = true
                  i += 1
                } else {
                  sb.append(c)
                  i += 1
                }
              }
              if (i >= n || path.charAt(i) != ']') fail(i, "expected ']' after quoted name")
              i += 1
              out += Name(sb.toString)

            case '*' => fail(i, "wildcards are not supported")
            case '-' => fail(i, "negative indexes are not supported")
            case c if c >= '0' && c <= '9' =>
              val start = i
              while (i < n && path.charAt(i) >= '0' && path.charAt(i) <= '9') i += 1
              if (i >= n || path.charAt(i) != ']') fail(i, "expected ']' after index")
              val idx =
                try path.substring(start, i).toInt
                catch { case _: NumberFormatException => fail(start, "index is too large") }
              i += 1
              out += Index(idx)

            case _ => fail(i, "expected an index or a quoted name after '['")
          }

        case _ => fail(i, "expected '.' or '['")
      }
    }
    JsonPath(out.result())
  }
}
