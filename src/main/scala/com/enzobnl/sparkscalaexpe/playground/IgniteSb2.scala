package com.enzobnl.sparkscalaexpe.playground


import com.enzobnl.sparkscalaexpe.playground
import com.enzobnl.sparkscalaexpe.playground.IgniteMemoCache.FIFO_EVICTION
import org.apache.ignite.cache.eviction.AbstractEvictionPolicy
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy
import org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicy
import org.apache.ignite.{IgniteCache, Ignition}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}

import scala.collection.mutable

// Policy
//sealed trait Policy
// CACHE
trait MemoCache{
  val evictionPolicy: EvictionPolicy
  var retrieves = 0L
  var defaults = 0L
  def getOrElseUpdate(key: Long, value: => Any): Any
  def close(): Unit = {}
}

trait EvictionPolicy

sealed class IgniteMemoCacheEvictionPolicy extends EvictionPolicy

object IgniteMemoCache{
  case object LRU_EVICTION extends  IgniteMemoCacheEvictionPolicy
  case object FIFO_EVICTION extends  IgniteMemoCacheEvictionPolicy
  case object SORTED_EVICTION extends  IgniteMemoCacheEvictionPolicy
}

class IgniteMemoCache(override val evictionPolicy: IgniteMemoCacheEvictionPolicy) extends MemoCache {
  val CACHE_NAME = "ignite"

  val cacheConfig = new CacheConfiguration(CACHE_NAME)
    .setEvictionPolicy(evictionPolicy match{
      case IgniteMemoCache.LRU_EVICTION => new LruEvictionPolicy()
      case IgniteMemoCache.FIFO_EVICTION => new FifoEvictionPolicy()
      case IgniteMemoCache.SORTED_EVICTION => new SortedEvictionPolicy()
    })

  val icf = new IgniteConfiguration()
    .setCacheConfiguration(new CacheConfiguration(CACHE_NAME)
    .setOnheapCacheEnabled(false))


  lazy val ignite = Ignition.getOrStart(icf)

  lazy val igniteCache: IgniteCache[Long, Any] = ignite.getOrCreateCache[Long, Any](CACHE_NAME)

  override def getOrElseUpdate(key: Long, value: => Any): Any = {
    igniteCache.get(key) match{
      case v: Any =>
        retrieves += 1
        v
      case _ =>
        defaults += 1
        val v = value
        igniteCache.put(key, v)
        v
    }
  }

  override def close(): Unit = {
    println("closed Ignite")
    ignite.close()
  }
}

class MapAdapter(map: mutable.Map[Long, Any], override val evictionPolicy: EvictionPolicy) extends MemoCache {

  override def getOrElseUpdate(hash: Long, value: => Any): Any = {
    var computed = false
    val result = map.getOrElseUpdate(hash, {computed = true; value})
    if(computed) defaults += 1
    else retrieves += 1
    result
  }
}



// MEMO
trait TMemo{
  val memoCache: MemoCache
  def apply[I, R](f: I => R): I => R
  def apply[I1, I2, R](f: (I1, I2) => R): (I1, I2) => R
}
class Memo(override val memoCache: MemoCache) extends TMemo {
  override def apply[I, R](f: I => R): I => R = {
    new Function1[I, R]{
      val sharedMemoizationCache: MemoCache = memoCache
      override def apply(v1: I): R =
        sharedMemoizationCache.getOrElseUpdate((f.hashCode(), v1).hashCode(), f.apply(v1)).asInstanceOf[R]

      override def finalize(): Unit = {
        sharedMemoizationCache.close()
        super.finalize()
      }
    }
  }
  override def apply[I1, I2, R](f: (I1, I2) => R): (I1, I2) => R = {
    new Function2[I1, I2, R]{
      val sharedMemoizationCache: MemoCache = memoCache
      override def apply(v1: I1, v2: I2): R =
        sharedMemoizationCache.getOrElseUpdate((f.hashCode(), v1, v2).hashCode(), f.apply(v1, v2)).asInstanceOf[R]
      override def finalize(): Unit = {
        println("memoized function finalyzed" + sharedMemoizationCache.getClass)
        sharedMemoizationCache.close()
        super.finalize()
      }
    }
  }
}

object IgniteSb2 extends Runnable{

  override def run(): Unit = {
    val memoCache = new IgniteMemoCache(FIFO_EVICTION)
    val memo = new Memo(memoCache)
    val f = (i: Int) => i*4
    val memoizedf = memo(f)
    for(i <- 1 to 10) println(memoizedf(i % 3))
    println(memoCache.defaults, memoCache.retrieves) // 3 7

    val memo2 = new Memo(memoCache)
    val memoizedf2 = memo2(f)
    for(i <- 1 to 10) println(memoizedf2(i % 3))
    println(memoCache.defaults, memoCache.retrieves)  // 3 17

    val memoCacheMap = new MapAdapter(mutable.Map[Long, Any](1L -> "e"), null)
    val memo3 = new Memo(memoCacheMap)
//
    for(i <- 1 to 10) println(memo3(f)(i % 3))
    println(memoCacheMap.defaults, memoCacheMap.retrieves)  // 3 7

    val memoized3 = memo3(f)
    for(i <- 1 to 10) println(memoized3(i % 3))
    println(memoCacheMap.defaults, memoCacheMap.retrieves) // 3,17

  }
}