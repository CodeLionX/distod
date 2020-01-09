package com.github.codelionx.distod

import java.lang.reflect.Type

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, JsonToken}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonNode, KeyDeserializer, SerializerProvider}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.github.codelionx.distod.actors.master.JobType
import com.github.codelionx.distod.types.CandidateSet

import scala.collection.mutable


object Serialization {

  /**
   * Marker trait for messages that can be serialized with Jackson to JSON
   */
  trait JsonSerializable

  /**
   * Marker trait for messages that can be serialized with Jackson to CBOR
   */
  trait CborSerializable

  class CandidateSetSerializer extends StdSerializer[CandidateSet](classOf[CandidateSet]) {

    override def serialize(value: CandidateSet, gen: JsonGenerator, provider: SerializerProvider): Unit = {
      val bitmask = value.toBitMask
      gen.writeArray(bitmask, 0, bitmask.length)
    }

    override def getSchema(provider: SerializerProvider, typeHint: Type): JsonNode =
      createSchemaNode("CandidateSet")
  }

  class CandidateSetKeySerializer extends StdSerializer[CandidateSet](classOf[CandidateSet]) {

    override def serialize(value: CandidateSet, gen: JsonGenerator, provider: SerializerProvider): Unit = {
      // candidate set is a field name
      gen.writeFieldName(value.toBitMask.mkString(","))
    }

    override def getSchema(provider: SerializerProvider, typeHint: Type): JsonNode =
      createSchemaNode("CandidateSetKey")
  }

  class CandidateSetDeserializer extends StdDeserializer[CandidateSet](classOf[CandidateSet]) {

    override def deserialize(p: JsonParser, ctxt: DeserializationContext): CandidateSet =
      if (p.hasToken(JsonToken.START_ARRAY)) {
        p.nextToken()
        // optimization using mutable collection and while loop
        val longs = mutable.ArrayBuilder.make[Long]
        do {
          val item = _parseLongPrimitive(p, ctxt)
          longs.addOne(item)
        } while (p.nextToken() != null && !p.hasToken(JsonToken.END_ARRAY))
        CandidateSet.fromBitMask(longs.result())
      } else {
        throw ctxt.wrongTokenException(p, _valueClass, JsonToken.START_ARRAY, "")
      }
  }

  class CandidateSetKeyDeserializer extends KeyDeserializer {

    override def deserializeKey(key: String, ctxt: DeserializationContext): AnyRef = {
      val longs = key.split(",").map(_.toLong)
      CandidateSet.fromBitMask(longs)
    }
  }

  class JobTypeSerializer extends StdSerializer[JobType.JobType](classOf[JobType.JobType]) {

    override def serialize(value: JobType.JobType, gen: JsonGenerator, provider: SerializerProvider): Unit = {
      val stringValue = value match {
        case JobType.Split => "JobType.Split"
        case JobType.Swap => "JobType.Swap"
      }
      gen.writeString(stringValue)
    }
  }

  class JobTypeDeserializer extends StdDeserializer[JobType.JobType](classOf[JobType.JobType]) {

    override def deserialize(p: JsonParser, ctxt: DeserializationContext): JobType.JobType = {
      p.getText match {
        case "JobType.Split" => JobType.Split
        case "JobType.Swap" => JobType.Swap
      }
    }
  }
}
