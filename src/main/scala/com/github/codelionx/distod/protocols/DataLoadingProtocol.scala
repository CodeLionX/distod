package com.github.codelionx.distod.protocols

import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.types.PartitionedTable


object DataLoadingProtocol {

  sealed trait DataLoadingEvent extends CborSerializable

  final case class DataLoaded(table: PartitionedTable) extends DataLoadingEvent

}
