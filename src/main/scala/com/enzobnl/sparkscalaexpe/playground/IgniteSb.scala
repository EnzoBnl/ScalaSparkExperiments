//package com.enzobnl.sparkscalaexpe.playground
//
//import com.enzobnl.sparkscalaexpe.util.{QuickSparkSessionFactory, Utils}
//import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
//import org.apache.ignite.{IgniteCache, Ignition}
//import org.apache.ignite.configuration.IgniteConfiguration
//import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
//import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
//import java.util
//
//import org.apache.parquet.example.data.simple.NanoTime
//import org.apache.spark.sql.{DataFrameReader, SparkSession}
////object Memoizer {
////  def memo[R, A1](function: A1 => R): A1 => R = new Memoizer[R, A1, Any]().doMemoize(function)
////  def memo[R, A1, A2](function: (A1, A2) => R): (A1, A2) => R = new Memoizer[R, A1, A2]().doMemoize(function)
////}
////
////class Memoizer[R, A1, A2] {
////  lazy val cache = {
////    println("cache created"); scala.collection.mutable.Map[Int, R]()
////  }
////
////  private def doMemoize(function: A1 => R): A1 => R = {
////    input => cache.getOrElseUpdate(input.hashCode(), {
////      println("Computed"); function(input)
////    })
////  }
////
////  private def doMemoize(function: (A1, A2) => R): (A1, A2) => R = {
////    (a1: A1, a2: A2) => {
////      cache.getOrElseUpdate(a1.hashCode() + a2.hashCode(), {
////        println("Computed"); function(a1, a2)
////      })
////    }
////  }
////}
////TRAITS
//trait MemoizationCache{
//  def put(key: Long, value: Any): Unit
//  def get(key: Long): Any
//  def getOrElseUpdate(key: Long, value: => Any): Any
//  def close(): Unit
//}
//
//trait Memoizer{
//  def memo[I, R](f: I => R): I => R
//  def memo[I1, I2, R](f: (I1, I2) => R): (I1, I2) => R
//  private var nextId = 0L
//  def getFunctionId(): Long = {
//    nextId += 1
//    nextId
//  }
//}
//trait CacheMemoizer extends Memoizer{
//  val memoizationCache: MemoizationCache
//}
//
//
//
//trait NaiveCacheMemoizer extends CacheMemoizer{
//  override def memo[I, R](f: I => R): I => R = {
//    new Function1[I, R]{
//      val sharedMemoizationCache: MemoizationCache = memoizationCache
//      val id = getFunctionId()  // reference agnostic
//      override def apply(v1: I): R = sharedMemoizationCache.getOrElseUpdate((id, v1).hashCode(), f.apply(v1)).asInstanceOf[R]
//
//      override def finalize(): Unit = {
//        sharedMemoizationCache.close()
//        super.finalize()
//      }
//    }
//  }
//  override def memo[I1, I2, R](f: (I1, I2) => R): (I1, I2) => R = {
//    new Function2[I1, I2, R]{
//      val sharedMemoizationCache: MemoizationCache = memoizationCache
//      val id = getFunctionId()
//      override def apply(v1: I1, v2: I2): R = sharedMemoizationCache.getOrElseUpdate((id, v1, v2).hashCode(), f.apply(v1, v2)).asInstanceOf[R]
//      override def finalize(): Unit = {
//        println("memoized function finalyzed" + sharedMemoizationCache.getClass)
//        sharedMemoizationCache.close()
//        super.finalize()
//      }
//    }
//  }
//}
//// PURE SCALA
//class WrappedMapMemoizationCache extends MemoizationCache{  // Adapter pattern
//  lazy val map = {println("created MAP");scala.collection.mutable.Map[Long, Any]()}
//  override def put(key: Long, value: Any): Unit = map.put(key, value)
//  override def get(key: Long): Any = map.get(key)
//  override def getOrElseUpdate(key: Long, value: => Any): Any = map.getOrElseUpdate(key, {println("Computed"); value})
//  override def close(): Unit = {println("closed WrappedMapMemoizationCache")}
//}
//object PureScalaMemoizer extends NaiveCacheMemoizer{ // TODO replace this by implicit or DI
//  override val memoizationCache: MemoizationCache = new WrappedMapMemoizationCache()
//}
//// Ignite
//class IgniteBasedMemoizationCache extends MemoizationCache{
//  val icf = new IgniteConfiguration().setCacheConfiguration(new CacheConfiguration("ignite").setOnheapCacheEnabled(false))
//  lazy val ignite = Ignition.getOrStart(icf)
//  lazy val igniteCache: IgniteCache[Long, Any] = ignite.getOrCreateCache[Long, Any]("ignite")
//  override def put(key: Long, value: Any): Unit = igniteCache.put(key, value)
//  override def get(key: Long): Any = igniteCache.get(key)
//  override def getOrElseUpdate(key: Long, value: => Any): Any = {
//    igniteCache.get(key) match{
//      case v: Any => v
//      case _ => {
//        val v = value
//        println("Computed,ig")
//        igniteCache.put(key, v)
//        v
//      }
//    }
//  }
//  override def close(): Unit = {println("closed Ignite");ignite.close()}
//}
//object IgniteBasedMemoizer extends NaiveCacheMemoizer{
//  override val memoizationCache: MemoizationCache = new IgniteBasedMemoizationCache()
//}
//
//
//object IgniteSb extends Runnable {
//
//  lazy val spark: SparkSession = QuickSparkSessionFactory.getOrCreate()
//  lazy val sc = spark.sparkContext
//  lazy val df = spark.createDataFrame(
//    Seq(("Thin", "Cell", 6000, 1),
//      ("Normal", "Tablet", 1500, 1),
//      ("Mini", "Tablet", 5500, 1),
//      ("Ultra thin", "Cell", 5000, 1),
//      ("Very thin", "Cell", 6000, 1),
//      ("Big", "Tablet", 2500, 2),
//      ("Bendable", "Cell", 3000, 2),
//      ("Foldable", "Cell", 3000, 2),
//      ("Pro", "Tablet", 4500, 2),
//      ("Pro2", "Tablet", 6500, 2))).toDF("product", "category", "revenue", "un")
//
//
//  override def run(): Unit = {
//    val f = (i: Int, s: String) => s.substring(i, i + 1)
//    val g = (i: Int, s: String) => s.substring(i - 1, i)
//    import IgniteBasedMemoizer.memo
////    lazy val facto: Int => Int = memo((n: Int) => if(n<=1) 1 else n*memo(facto)(n-1))
////    println(facto(6))
//    var i = 0
//    lazy val fibo: Int => Int = n => {i += 1; println(f"fibo no memo run$i");n match {
//      case 0 => 0
//      case 1 => 1
//      case _ => fibo(n-1) + fibo(n-2)}}
//    Utils.time(println(fibo(20)))
////    i = 0
////    lazy val mfibo2: Int => Int = memo(fibo)
////    Utils.time(println(mfibo2(30)))
//    i = 0
//    lazy val mfibo: Int => Int = memo(n => {i += 1; println(f"fibo IGNITE run$i"); n match {
//      case 0 => 1
//      case 1 => 1
//      case _ => mfibo(n-1) + mfibo(n-2)}})
//    Utils.time(println(mfibo(1)))
//    Utils.time(println(mfibo(20)))
//    i = 0
//    lazy val mfibo2: Int => Int = PureScalaMemoizer.memo(n => {i += 1; println(f"fibo PureScalaMemoizer run$i"); n match {
//      case 0 => 0
//      case 1 => 1
//      case _ => mfibo2(n-1) + mfibo2(n-2)}})
//    Utils.time(println(mfibo2(20)))
//
//    //    Utils.time(println(mfibo(100)))
////    i = 0
////    lazy val memoizedFib: Int => Int = Memo.mutableHashMapMemo { n => i += 1; println(f"fibo SCALAZ run$i"); n match{
////      case 0 => 0
////      case 1 => 1
////      case _ => memoizedFib(n - 2) + memoizedFib(n - 1)}
////    }
////    Utils.time(println(memoizedFib(20)))
//
//
////    System.exit(0)
//    spark.udf.register("f", mfibo2)
//    Utils.time {spark.createDataFrame(for(i <- 1 to 21) yield Tuple1(i)).toDF("n").selectExpr("f(n)").show()}
//    Utils.time {spark.createDataFrame(for(i <- 1 to 21) yield Tuple1(i)).toDF("n").selectExpr("f(n)").show()}
//
//    //    val im = new IgniteBasedMemoizer()
//    spark.udf.register("f", mfibo)
//    Utils.time {spark.createDataFrame(for(i <- 1 to 21) yield Tuple1(i)).toDF("n").selectExpr("f(n)").show()}
//    spark.stop()
//    System.gc()
//  }
//}
///*
//def bench(cache: IgniteCache[Int, String], cache2: scala.collection.mutable.Map[Int, String])= {
//
//    for (i <- 1 to 10) {
//      println(Utils.time {cache.put(i, s"value-$i")})
//      println(Utils.time {cache2.put(i, s"value-$i")})
//    }
//    println(s"From cache 7 -> ${cache.get(7)}")
//    println(s"Not exists 20 -> ${cache.get(20)}")
//    println("IGNITE GRID")
//    println(Utils.time {(1 to 100).foreach((i: Int) => cache.get(i))})
//    println(Utils.time {(1 to 100).foreach((i: Int) => cache.get(7))})
//    println(Utils.time {(1 to 100).foreach((i: Int) => cache.get(77))})
//    println("SCALA MAP")
//    println(Utils.time {(1 to 100).foreach((i: Int) => cache2.get(i))})
//    println(Utils.time {(1 to 100).foreach((i: Int) => cache2.get(7))})
//    println(Utils.time {(1 to 100).foreach((i: Int) => cache2.get(77))})
//    println("OTHERS")
//    println(Utils.time {
//      for(i <- 8 to 1000000){
//        if(7%i == 0){
//          print(1)
//        }
//      }
//    })
//    println(Utils.time {
//      for(i <- 8 to 100000){
//        if(7%i == 0){
//          print(1)
//        }
//      }
//    })
//    println(Utils.time {
//      for(i <- 8 to 10000){
//        if(7%i == 0){
//          print(1)
//        }
//      }
//    })
//    println(Utils.time {1})
//
//    val printTime = () => System.nanoTime()
//    import org.apache.spark.sql.functions.{udf, col}
//    spark.udf.register("pt", printTime)
//    df.selectExpr("hash(revenue+un)", "pt()").show()
//  }
//*/
//
////    val icf = new IgniteConfiguration().setCacheConfiguration(new CacheConfiguration("ignite").setOnheapCacheEnabled(false))
////    val ignite = Ignition.getOrStart(icf)
////    println("A")
////    val ignite2 = Utils.time {Ignition.getOrStart(icf)}
////    println("B")
////
////    val cache: IgniteCache[Int, Any] = ignite.getOrCreateCache[Int, Any]("ignite")
////    cache.put(1, Seq("e","7"))
////    println(Utils.time {cache.get(1)}.asInstanceOf[Seq[String]])
////    println(Utils.time {cache.get(1)})
////
////    val cache2: scala.collection.mutable.Map[Int, String] = scala.collection.mutable.Map[Int, String]()
//
//
////
////
////
////    val spi = new TcpDiscoverySpi
////    val ipFinder = new TcpDiscoveryVmIpFinder
////    // Set initial IP addresses.
////    // Note that you can optionally specify a port or a port range.
////    ipFinder.setAddresses(util.Arrays.asList("127.0.0.1"))
////    spi.setIpFinder(ipFinder)
////    val cfg = new IgniteConfiguration
////    // Override default discovery SPI.
////    cfg.setDiscoverySpi(spi)
////    // Start Ignite node.
////    val ic = new IgniteContext(sc, () => cfg, false)
////    val sharedRDD: IgniteRDD[Int, Int] = ic.fromCache("sharedRDD")
////    sharedRDD.savePairs(sc.parallelize(1 to 1000, 10).map(i => (i, i)))
////    ic.close(true)
////    spark.close()
//
////
////object RDDWriter extends App {
////  val conf = new SparkConf().setAppName("RDDWriter")
////  val sc = new SparkContext(conf)
////  val ic = new IgniteContext(sc, "/path_to_ignite_home/examples/config/spark/example-shared-rdd.xml")
////  val sharedRDD: IgniteRDD[Int, Int] = ic.fromCache("sharedRDD")
////  sharedRDD.savePairs(sc.parallelize(1 to 1000, 10).map(i => (i, i)))
////  ic.close(true)
////  sc.stop()
////}
////
////object RDDReader extends App {
////  val conf = new SparkConf().setAppName("RDDReader")
////  val sc = new SparkContext(conf)
////  val ic = new IgniteContext(sc, "/path_to_ignite_home/examples/config/spark/example-shared-rdd.xml")
////  val sharedRDD: IgniteRDD[Int, Int] = ic.fromCache("sharedRDD")
////  val greaterThanFiveHundred = sharedRDD.filter(_._2 > 500)
////  println("The count is " + greaterThanFiveHundred.count())
////  ic.close(true)
////  sc.stop()
////}