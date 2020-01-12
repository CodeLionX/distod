package com.github.codelionx.distod.benchmarking

import java.util.concurrent.TimeUnit

import com.github.codelionx.distod.types.CandidateSet
import org.openjdk.jmh.annotations._

import scala.util.Random


@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 50, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 100, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
class CandidateSetEquals {

  private val compareTo: CandidateSet = CandidateSet.from(0, 243, 2, 413, 13, 1202, 4, 9)

  @Param(Array("1", "10", "100", "1000", "10000"))
  var size: Int = _

  var bitset: CandidateSet = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    val bits = for(_ <- 0 until 100) yield Random.nextInt(size)
    bitset = CandidateSet.fromSpecific(bits :+ size)
  }

  @Benchmark
  def baseline: Boolean = {
    bitset.equals_super(compareTo)
  }

  @Benchmark
  def xor: Boolean = {
    bitset.equals_xor(compareTo)
  }

  @Benchmark
  def hash: Boolean = {
    bitset.equals_hash(compareTo)
  }
}
