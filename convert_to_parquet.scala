import is.hail.HailContext
import is.hail.backend.spark.SparkBackend
import is.hail.expr.ir._
import is.hail.variant.Locus
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

val backend = SparkBackend(tmpdir = "/tmp", localTmpdir = "file:///tmp")
val hc = HailContext(backend, logFile = "/tmp/hail.log")

val tir = TableRead.native(backend.fs, "/home/leo/gnomad_popmax_af.ht")
val tv = ExecuteContext.scoped() { ctx => Interpret(tir, ctx) }

val mapped = tv.rdd.map(row => {
    val locus = row.get(0).asInstanceOf[Locus]
    Row(locus.contig, locus.position, row.get(2))
})

val schema = new StructType()
  .add(StructField("contig", StringType, false))
  .add(StructField("position", IntegerType, false))
  .add(StructField("AF", DoubleType, true))

val df = backend.sparkSession.createDataFrame(mapped, schema)

df.write.parquet("/home/leo/gnomad_popmax_af.parquet")
