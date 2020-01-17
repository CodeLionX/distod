package com.github.codelionx.util

import scala.annotation.tailrec


object Math {

//  @tailrec
  def binomialCoefficient(n: Int, k: Int): Int = {
    if(n < 0 || k < 0)
      throw new IllegalArgumentException(s"One of the parameters was negative (n=$n, k=$k)")
    else
      (n, k) match {
        case (n, k) if k > n =>
          throw new IllegalArgumentException(s"k ($k) was greater than n ($n)")
        case (_, 0) =>
          1
        case (n, k) if k > n / 2 =>
          binomialCoefficient(n, n - k)
        case (n, k) =>
          n * binomialCoefficient(n - 1, k - 1) / k
      }
  }
}
