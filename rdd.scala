import is.hail.HailSuite
import is.hail.expr.ir._
import org.apache.spark.storage.StorageLevel

val hs = new HailSuite
val tir = TableRead.native(hs.fs, "/Users/leo/tmp/gnomad_popmax_af_head.ht")
val tv = ExecuteContext.scoped() { ctx => Interpret(tir, ctx) }
val cached = tv.rdd.persist(StorageLevel.MEMORY_ONLY)
println("Caching...")
cached.count()

println("Filtering...")
(1 to 9).foreach(t => {
  val threshold = t * 1e-4
  val t0 = System.nanoTime()
  println(cached.filter(row => !row.isNullAt(2) && row.getDouble(2) < threshold).count)
  val t1 = System.nanoTime()
  println((t1 - t0) * 1e9 + " s")
})
