package com.github.codelionx.distod.types

case class Table(name: String, headers: Array[String], columns: Array[Array[String]]) {

  override def toString: String = toStringCapped()

  def toStringCapped(maxLines: Int = 20): String = {
    val builder = new StringBuilder("\n")
    builder.append("TABLE: ").append(name).append("\n")
    builder.append("===================================================================\n")
    builder.append(headers.mkString(",")).append("\n")
    builder.append("-------------------------------------------------------------------\n")
    if (columns.head.length > maxLines) {
      for (i <- columns.head.indices.take(maxLines)) {
        builder.append(columns.map(_ (i)).mkString(",")).append("\n")
      }
      builder.append("...").append("\n")
      builder.append(columns.map(_ (columns.head.length - 1)).mkString(",")).append("\n")
    } else {
      for (i <- columns.head.indices) {
        builder.append(columns.map(_ (i)).mkString(",")).append("\n")
      }
    }
    builder.append("===================================================================\n")
    builder.result()
  }
}
