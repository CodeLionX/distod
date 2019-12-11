package com.github.codelionx.distod.actors.master


/**
 * Marker traits for distinguishing worker jobs that test candidates. They either test swap candidates or split
 * candidates.
 */
object JobType {
  sealed trait JobType
  case object Split extends JobType
  case object Swap extends JobType
}
