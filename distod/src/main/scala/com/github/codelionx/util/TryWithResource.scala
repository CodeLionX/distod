package com.github.codelionx.util

import scala.util.control.NonFatal


object TryWithResource {

  /**
   * Try-With-Resource scala implementation.
   *
   * @example {{{
   * val result: Int = withResource(new FileReader("path")) { reader =>
   *   val fileContent = reader.read();
   *   1 // pseudo return
   * }
   * }}}
   * @see https://medium.com/@dkomanov/scala-try-with-resources-735baad0fd7d
   * @param r resource that implements the [[java.lang.AutoCloseable]] interface
   * @param f processing function that is guarded by this try-with-resource block
   * @tparam T type of the resource
   * @tparam V type of the processed result
   * @return the result of the processing function `f`
   */
  def withResource[T <: AutoCloseable, V](r: => T)(f: T => V): V = {
    val resource: T = r
    require(resource != null, "resource is null")

    var exception: Throwable = null
    try {
      f(r)
    } catch {
      case NonFatal(e) =>
        exception = e
        throw e
    } finally {
      closeAndAddSuppressed(exception, resource)
    }
  }

  private def closeAndAddSuppressed(e: Throwable, resource: AutoCloseable): Unit = {
    if (e != null) {
      try {
        resource.close()
      } catch {
        case NonFatal(suppressed) =>
          e.addSuppressed(suppressed)
      }
    } else {
      resource.close()
    }
  }
}
