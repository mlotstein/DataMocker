# DataMocker

Build Spark DataFrames for **tests** from a `StructType` and a few `JSONPath -> value` pairs.
Everything you don't mention is `null`, at every nesting level.

```scala
import datamocker.syntax._

val df = schema.mockDF(spark)(
  Map("$.id" -> 1L, "$.address.zip" -> "94110", "$.items[2].sku" -> "a", "$.props['k'].id" -> 7),
  Map("$.id" -> 2L)
)
```

Or without the implicit syntax:

```scala
DataMocker(schema)
  .row("$.id" -> 1L, "$.items[2].sku" -> "a")
  .row("$.id" -> 2L)
  .build(spark)
```

## Nullability

The resulting DataFrame has the input schema's names and types, but **every level is nullable**: struct
fields, array elements (`containsNull`) and map values (`valueContainsNull`). Map keys stay non-null, as Spark
requires. This is what lets unmentioned fields be null even if the original schema says they can't be, so use
this for tests only. `DataMocker(schema).relaxedSchema` shows the schema that will be used.

## Paths

A subset of JSONPath: `$`, `.name`, `['name']` / `["name"]`, `[n]`.

| Target | Path |
|---|---|
| struct field | `$.address.zip` |
| array element | `$.items[2].sku` (earlier indexes are padded with `null`) |
| map value | `$.props['k'].id` (key text is converted to the map's key type: string, integral, boolean) |
| name with dots/spaces | `$['a.b']` (escape quotes and backslashes with `\`) |

Not supported, and rejected with an error: wildcards, `..`, negative indexes, slices, filters, and assigning to `$`.
Maps whose keys are not string/integral/boolean can only be assigned as a whole map.

Naming a deep path creates every struct, array and map above it automatically.

## Values

You can assign a leaf, or a whole struct (`Row` or `Map` of field name to value), array (`Seq`) or map (`Map`);
whole values are validated recursively. `null` is allowed everywhere. Values are checked without using Spark
casts, so behavior doesn't change with ANSI mode:

- Integrals: any integral type that fits the target range (`Int` into `Long` is fine; `70000` into `Short` is not).
- `Float`/`Double`: lossless widening only.
- `Decimal`: `BigDecimal`/integrals that fit the precision and scale without rounding.
- Dates/timestamps: `java.sql.Date`/`Timestamp` or `java.time.LocalDate`/`Instant`/`LocalDateTime` (NTZ).
- Types with no rule here (e.g. variant, intervals) are passed through to Spark.

## Errors

All problems across all rows are collected and thrown together as a `MockSpecException`, with the row number and
path on each. Unknown fields, type or range mismatches, bad paths, and conflicting writes (the same path twice, or a
path inside a value you already assigned) are all errors; nothing is silently ignored.

## Performance

Cost is linear in the number of paths plus the size of the values you assign. The schema is never copied or
walked per path (it is only walked once to relax nullability), so sparse mocks over huge schemas are cheap.
`PerformanceSpec` fits a scaling exponent for several operations and fails the build if one stops being linear.

Nesting depth is not limited by the stack for paths and rows. Extremely deep schemas fail with a clear error.
Note that Spark itself gets slow on very wide, deeply nested schemas (`collect()` on thousands of nested struct
fields takes seconds, and tens of thousands can exhaust the default heap); that cost is Spark's, not this library's.

## Building

sbt with `sbt-projectmatrix`, cross-built per Spark line:

| Project | Spark | Scala |
|---|---|---|
| `core_spark352_12` | 3.5.9 | 2.12 |
| `core_spark35` | 3.5.9 | 2.13 |
| `core_spark40` | 4.0.5 | 2.13 |
| `core_spark43` | 4.3.0 | 2.13 |

`sbt test` runs everything. Use JDK 17 or 21 to build (Spark 4.x needs 17+). The 3.5 builds target Java 8
bytecode and the suite passes on Corretto 8 (run through ScalaTest's runner directly, because sbt's forked test
agent doesn't start on Java 8). Spark is a `provided` dependency, so you bring your own.

Version-specific code lives in `core/src/main/scala-spark-{3.5,4}/` and `scala-{2.12,2.13}/`.

## Not done yet

- `preserveNullability` option (keep the original nullability and fail on missing non-null fields).
- Spark 4.1 / 4.2 are untested (4.0 and 4.3 pass). Spark Connect is untested.
- Publishing and CI.
