import is.hail.HailSuite
import is.hail.expr.ir._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import is.hail.variant.Locus

val hs = new HailSuite
val tir = TableRead.native(hs.fs, "/home/leo/gnomad_popmax_af.ht")
val tv = ExecuteContext.scoped() { ctx => Interpret(tir, ctx) }

val floats = tv.rdd.map(row => {
  var f = 0.0f
  
  if (!row.isNullAt(2))
      f = row.getDouble(2).toFloat

  val locus = row.get(0).asInstanceOf[Locus]
  (locus.position, f)
})
val cached = floats.persist(StorageLevel.MEMORY_ONLY)
println("Caching...")
println("Count: " + cached.count())
println("Size estimate: " + SizeEstimator.estimate(cached))

println("Filtering...")
(1 to 9).foreach(t => {
  val threshold = t * 1e-4
  val t0 = System.nanoTime()
  println(cached.filter(x => x._2 < threshold).count)
  val t1 = System.nanoTime()
  println((t1 - t0) * 1e-9 + " s")
})
