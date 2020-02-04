package com.github.codelionx.distod.actors.partitionMgmt.channel

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import akka.actor.typed.ActorSystem
import akka.util.ByteString
import com.github.codelionx.util.TryWithResource


trait DataMessageWriteObjectSerialization {

  def serialize(m: DataMessage)(implicit system: ActorSystem[_]): ByteString = {
    val bytes =
      TryWithResource.withResource(new ByteArrayOutputStream()) { baos =>
        TryWithResource.withResource(new ObjectOutputStream(baos)) { out =>
          out.writeObject(m)
          out.flush()
        }
        baos.toByteArray
      }
    ByteString(bytes).compact
  }

  def deserialize(s: ByteString)(implicit system: ActorSystem[_]): DataMessage = {
    val obj = TryWithResource.withResource(new ObjectInputStream(new ByteArrayInputStream(s.toArray[Byte]))) { in =>
      in.readObject()
    }
    obj.asInstanceOf[DataMessage]
  }
}
