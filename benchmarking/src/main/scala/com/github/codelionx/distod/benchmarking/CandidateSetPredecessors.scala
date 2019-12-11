package com.github.codelionx.distod.benchmarking

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import scala.collection.immutable.BitSet
import scala.collection.mutable


@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 50, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 100, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
class CandidateSetPredecessors {

  @Param(Array("1", "5", "10", "15", "20", "50"))
  var size: Int = _

  var bitset: BitSet = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    bitset = BitSet.fromSpecific(0 until size)
  }

  @Benchmark
  def functional(): Set[BitSet] = {
    bitset.unsorted.map(elem =>
      bitset - elem
    )
  }

  @Benchmark
  def mutableSet(): Set[BitSet] = {
    val x = mutable.Set.empty[BitSet]
    bitset.unsorted.foreach(elem =>
      x.add(bitset - elem)
    )
    x.toSet
  }

  @Benchmark
  def generational(): Set[BitSet] = {
    bitset
      .toSeq
      .combinations(size - 1)
      .map(BitSet.fromSpecific)
      .toSet
  }
}
