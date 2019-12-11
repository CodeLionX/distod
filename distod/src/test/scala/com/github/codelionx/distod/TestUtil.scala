package com.github.codelionx.distod

object TestUtil {

  /**
   * Returns the path to a file in the resources folder.
   */
  def findResource(filename: String): String = this.getClass.getClassLoader.getResource(filename).getPath
}
