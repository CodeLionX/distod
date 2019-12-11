package com.github.codelionx.distod


object Serialization {

  /**
   * Marker trait for messages that can be serialized with Jackson to JSON
   */
  trait JsonSerializable

  /**
   * Marker trait for messages that can be serialized with Jackson to CBOR
   */
  trait CborSerializable

}
