import is.hail.HailContext
import is.hail.backend.spark.SparkBackend
import is.hail.expr.ir._
import is.hail.variant.Locus
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator

val backend = SparkBackend(tmpdir = "/tmp", localTmpdir = "file:///tmp")
val hc = HailContext(backend, logFile = "/tmp/hail.log")

val tir = TableRead.native(backend.fs, "/home/leo/gnomad_popmax_af.ht")
val tv = ExecuteContext.scoped() { ctx => Interpret(tir, ctx) }

val doubles = tv.rdd.map(row => {
  if (row.isNullAt(2)) {
    Double.NaN
  } else {
    row.getDouble(2)
  }
})

val cached = doubles.persist(StorageLevel.MEMORY_ONLY)
println("Caching...")
println("Count: " + cached.count())
println("Size estimate: " + SizeEstimator.estimate(cached))

println("Filtering...")
(1 to 9).foreach(t => {
  val threshold = t * 1e-4
  val t0 = System.nanoTime()
  println(threshold + " " + cached.filter(x => !x.isNaN && x < threshold).count)
  val t1 = System.nanoTime()
  println((t1 - t0) * 1e-9 + " s")
})
