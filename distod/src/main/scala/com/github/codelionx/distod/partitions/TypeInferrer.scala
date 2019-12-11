package com.github.codelionx.distod.partitions

import com.github.codelionx.distod.types._


object TypeInferrer {

  /**
   * Infers the type of the value and returns it as an instance of [[com.github.codelionx.distod.types.DataType]].
   *
   * @note The value is not transformed! You can do this by using [[com.github.codelionx.distod.types.DataType#parse]].
   */
  def inferType(value: String): DataType[_] = {
    val maybeDate = DateTimeType.getDateTimeType(value)
    maybeDate match {
      case Some(tpe) =>
        tpe
      case None =>
        if (DoubleType.isDouble(value))
          DoubleType
        else
          StringType
    }
  }

  /**
   * Infers the type of a column by looking at some samples of it. Returns the most refined type according to the
   * ordering in [[com.github.codelionx.distod.types.DataType#orderMapping]].
   */
  def inferTypeForColumn(column: Array[String]): DataType[_] = {
    val start = column.take(2)
    val mid = column.slice((column.length / 2) - 2, (column.length / 2) + 2)
    val end = column.takeRight(2)

    var tpe: DataType[Any] = NullType.broaden
    for (value <- start ++ mid ++ end) {
      val newTpe = inferType(value)
      if (tpe != newTpe && tpe < newTpe) {
        tpe = newTpe.broaden
      }
    }
    tpe
  }
}
