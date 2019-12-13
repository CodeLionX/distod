package com.github.codelionx.distod.actors.master

import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.github.codelionx.distod.Serialization.{JobTypeDeserializer, JobTypeSerializer}


/**
 * Marker traits for distinguishing worker jobs that test candidates. They either test swap candidates, split
 * candidates, or they generate the next candidates.
 */
object JobType {
  @JsonSerialize(using = classOf[JobTypeSerializer])
  @JsonDeserialize(using = classOf[JobTypeDeserializer])
  sealed trait JobType
  case object Split extends JobType
  case object Swap extends JobType
  case object Generation extends JobType
}
