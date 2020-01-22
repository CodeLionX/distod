package com.github.codelionx.distod.actors.master

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo, JsonTypeName}
import com.github.codelionx.distod.actors.master.CandidateState.{NewSplitCandidates, NewSwapCandidates, SplitChecked, SwapChecked}
import com.github.codelionx.distod.types.CandidateSet


object CandidateState {

  def apply(id: CandidateSet): CandidateState = InitialCandidateState(id)

  def forL0(id: CandidateSet, splitCandidates: CandidateSet): CandidateState =
    // we do not need to check for splits and swaps in level 0 (empty set)
    FullyCheckedCandidateState(
      id = id,
      splitCandidates = splitCandidates,
      swapCandidates = Seq.empty
    )

  def forL1(id: CandidateSet, splitCandidate: CandidateSet): CandidateState = L1CandidateState(id, splitCandidate)

  // we do not need to check for swaps in level 1 (single attribute nodes)
  case class L1CandidateState private(id: CandidateSet, splitCandidates: CandidateSet) extends CandidateState {

    override val isPruned: Boolean = false
    override val splitChecked: Boolean = false
    override val swapChecked: Boolean = true
    override val swapCandidates: Seq[(Int, Int)] = Seq.empty

    override def isReadyToCheck(jobType: JobType.JobType): Boolean = jobType match {
      case JobType.Split => true
      case JobType.Swap => false
    }

    override protected def incSplitPreconditions: CandidateState = this

    override protected def incSwapPreconditions: CandidateState = this

    override def updated(delta: Delta): CandidateState = delta match {
      case SplitChecked(removedCandidates) => FullyCheckedCandidateState(
        id = this.id,
        splitCandidates = this.splitCandidates -- removedCandidates,
        swapCandidates = this.swapCandidates
      )
      case m => throw new UnsupportedOperationException(s"L1 Candidate State can not be updated by $m")
    }
  }

  def initForL2(id: CandidateSet): CandidateState = L2CandidateState(id)

  // predecessors (L1) do not perform swap checks, so the preconditions for the L2 swap checks are already met
  case class L2CandidateState private(
      id: CandidateSet,
      splitCandidates: CandidateSet = CandidateSet.empty,
      swapCandidates: Seq[(Int, Int)] = Seq.empty,
      private val splitPreconditions: Int = 0,
      private val receivedUpdates: Int = 0
  ) extends CandidateState {

    override val isPruned: Boolean = false
    override val splitChecked: Boolean = false
    override val swapChecked: Boolean = false

    override def isReadyToCheck(jobType: JobType.JobType): Boolean = jobType match {
      case JobType.Split => id.size == splitPreconditions
      case JobType.Swap => id.size == splitPreconditions
    }

    override protected def incSplitPreconditions: CandidateState = this.copy(
      splitPreconditions = this.splitPreconditions + 1
    )

    override protected def incSwapPreconditions: CandidateState = this

    private def alreadyOneUpdateReceived: Boolean = receivedUpdates == 1

    override def updated(delta: Delta): CandidateState = delta match {
      case NewSplitCandidates(splitCandidates) if isReadyToCheck(JobType.Split) && !alreadyOneUpdateReceived =>
        this.copy(
          splitCandidates = splitCandidates,
          receivedUpdates = this.receivedUpdates + 1
        )
      case NewSplitCandidates(splitCandidates) if isReadyToCheck(JobType.Split) && alreadyOneUpdateReceived =>
        ReadyCandidateState(
          id = id,
          splitCandidates = splitCandidates,
          swapCandidates = this.swapCandidates
        )
      case NewSwapCandidates(swapCandidates) if isReadyToCheck(JobType.Swap) && !alreadyOneUpdateReceived =>
        this.copy(
          swapCandidates = swapCandidates,
          receivedUpdates = this.receivedUpdates + 1
        )
      case NewSwapCandidates(swapCandidates) if isReadyToCheck(JobType.Swap) && alreadyOneUpdateReceived =>
        ReadyCandidateState(
          id = id,
          splitCandidates = this.splitCandidates,
          swapCandidates = swapCandidates
        )
      case SwapChecked(_) =>
        throw new UnsupportedOperationException("L2CandidateState can not update the swap candidates")
      case m => throw new UnsupportedOperationException(s"L2CandidateState can not be updated by $m: $this")
    }
  }

  def pruned(id: CandidateSet): CandidateState = PrunedCandidateState(id)

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[NewSplitCandidates]),
    new JsonSubTypes.Type(value = classOf[NewSwapCandidates]),
    new JsonSubTypes.Type(value = classOf[SplitChecked]),
    new JsonSubTypes.Type(value = classOf[SwapChecked]),
  ))
  sealed trait Delta extends Ordered[Delta] {

    protected def orderingId: Int

    override def compare(that: Delta): Int = this.orderingId - that.orderingId
  }

  @JsonTypeName("NewSplitCandidates")
  final case class NewSplitCandidates(splitCandidates: CandidateSet) extends Delta {

    override protected def orderingId: Int = 0
  }

  @JsonTypeName("NewSwapCandidates")
  final case class NewSwapCandidates(swapCandidates: Seq[(Int, Int)]) extends Delta {

    override protected def orderingId: Int = 1
  }

  @JsonTypeName("SplitChecked")
  final case class SplitChecked(removedCandidates: CandidateSet) extends Delta {

    override protected def orderingId: Int = 2
  }

  @JsonTypeName("SwapChecked")
  final case class SwapChecked(removedCandidates: Seq[(Int, Int)]) extends Delta {

    override protected def orderingId: Int = 3
  }

}


sealed trait CandidateState {

  // state
  def id: CandidateSet

  def splitCandidates: CandidateSet

  def swapCandidates: Seq[(Int, Int)]

  def splitChecked: Boolean

  def swapChecked: Boolean

  def isPruned: Boolean

  // state transitions
  def isReadyToCheck(jobType: JobType.JobType): Boolean

  def updated(delta: CandidateState.Delta): CandidateState

  protected def incSplitPreconditions: CandidateState

  protected def incSwapPreconditions: CandidateState

  // helper methods -----------------------------------------------------
  // testing functions
  def isFullyChecked: Boolean = splitChecked && swapChecked

  def isNotPruned: Boolean = !isPruned

  def notReadyToCheck(jobType: JobType.JobType): Boolean = !isReadyToCheck(jobType)

  // state transitions
  def prune: CandidateState = CandidateState.pruned(id)

  def pruneIfConditionsAreMet: CandidateState =
    if (!isPruned && shouldBePruned) prune
    else this

  protected def shouldBePruned: Boolean = isFullyChecked && splitCandidates.isEmpty && swapCandidates.isEmpty

  def incAndTestReadyToCheck(jobType: JobType.JobType): (CandidateState, Boolean) = {
    val newState = incPreconditions(jobType)
    (newState, newState.isReadyToCheck(jobType))
  }

  def incPreconditions(jobType: JobType.JobType): CandidateState = jobType match {
    case JobType.Split => this.incSplitPreconditions
    case JobType.Swap => this.incSwapPreconditions
  }

  def updatedAll(deltas: Iterable[CandidateState.Delta]): CandidateState =
    deltas
      .toSeq
      .sorted
      .foldLeft(this) { case (state, delta) =>
        state.updated(delta)
      }
}

case class PrunedCandidateState(id: CandidateSet) extends CandidateState {

  override val splitChecked: Boolean = true
  override val swapChecked: Boolean = true
  override val isPruned: Boolean = true
  override val splitCandidates: CandidateSet = CandidateSet.empty
  override val swapCandidates: Seq[(Int, Int)] = Seq.empty

  override def isReadyToCheck(jobType: JobType.JobType): Boolean = false

  override def updated(delta: CandidateState.Delta): CandidateState =
    // silently ignore updates
    this

  override protected def incSplitPreconditions: CandidateState = this

  override protected def incSwapPreconditions: CandidateState = this
}

case class InitialCandidateState(
    id: CandidateSet,
    private val splitPreconditions: Int = 0,
    private val swapPreconditions: Int = 0
) extends CandidateState {

  override val splitCandidates: CandidateSet = CandidateSet.empty
  override val swapCandidates: Seq[(Int, Int)] = Seq.empty
  override val splitChecked: Boolean = false
  override val swapChecked: Boolean = false
  override val isPruned: Boolean = false

  override def isReadyToCheck(jobType: JobType.JobType): Boolean = jobType match {
    case JobType.Split => id.size == splitPreconditions
    case JobType.Swap => id.size == splitPreconditions && id.size == swapPreconditions
  }

  override protected def incSplitPreconditions: CandidateState = this.copy(
    splitPreconditions = this.splitPreconditions + 1
  )

  override protected def incSwapPreconditions: CandidateState = this.copy(
    swapPreconditions = this.swapPreconditions + 1
  )

  override def updated(delta: CandidateState.Delta): CandidateState = delta match {
    case NewSplitCandidates(splitCandidates) if isReadyToCheck(JobType.Split) => SplitReadyCandidateState(
      id = id,
      splitCandidates = splitCandidates,
      swapPreconditions = swapPreconditions
    )
    case NewSwapCandidates(_) if isReadyToCheck(JobType.Swap) =>
      SwapReadyCandidateState(
        id = id,
        swapCandidates = swapCandidates
      )
    case m => throw new UnsupportedOperationException(s"InitialCandidateState can not be updated by $m")
  }
}

case class SplitReadyCandidateState(
    id: CandidateSet,
    splitCandidates: CandidateSet,
    swapPreconditions: Int,
    splitChecked: Boolean = false
) extends CandidateState {

  override val swapChecked: Boolean = false
  override val swapCandidates: Seq[(Int, Int)] = Seq.empty
  override val isPruned: Boolean = false

  override def isReadyToCheck(jobType: JobType.JobType): Boolean = jobType match {
    case JobType.Split => !splitChecked
    case JobType.Swap => id.size == swapPreconditions
  }

  override protected def incSplitPreconditions: CandidateState = this

  override protected def incSwapPreconditions: CandidateState = this.copy(
    swapPreconditions = this.swapPreconditions + 1
  )

  override def updated(delta: CandidateState.Delta): CandidateState = delta match {
    case NewSwapCandidates(swapCandidates) if isReadyToCheck(JobType.Swap) && this.splitChecked =>
      SplitCheckedCandidateState(
        id = id,
        splitCandidates = this.splitCandidates,
        swapCandidates = swapCandidates
      )
    case NewSwapCandidates(swapCandidates) if isReadyToCheck(JobType.Swap) && !this.splitChecked =>
      ReadyCandidateState(
        id = id,
        splitCandidates = this.splitCandidates,
        swapCandidates = swapCandidates
      )
    case SplitChecked(removedCandidates) => this.copy(
      splitCandidates = this.splitCandidates -- removedCandidates,
      splitChecked = true
    )
    case SwapChecked(_) =>
      throw new UnsupportedOperationException("SplitReadyCandidateState can not update the swap candidates")
    case m => throw new UnsupportedOperationException(s"SplitReadyCandidateState can not be updated by $m")
  }
}

case class SwapReadyCandidateState(
    id: CandidateSet,
    swapCandidates: Seq[(Int, Int)],
    swapChecked: Boolean = false
) extends CandidateState {

  override val splitChecked: Boolean = false
  override val splitCandidates: CandidateSet = CandidateSet.empty
  override val isPruned: Boolean = false

  override def isReadyToCheck(jobType: JobType.JobType): Boolean = jobType match {
    case JobType.Split => true
    case JobType.Swap => !swapChecked
  }

  override protected def incSplitPreconditions: CandidateState = this

  override protected def incSwapPreconditions: CandidateState = this

  override def updated(delta: CandidateState.Delta): CandidateState = delta match {
    case NewSplitCandidates(splitCandidates) if this.swapChecked =>
      SwapCheckedCandidateState(
        id = id,
        splitCandidates = splitCandidates,
        swapCandidates = this.swapCandidates
      )
    case NewSplitCandidates(splitCandidates) if !this.swapChecked =>
      ReadyCandidateState(
        id = id,
        splitCandidates = splitCandidates,
        swapCandidates = this.swapCandidates
      )
    case SwapChecked(removedCandidates) => this.copy(
      swapCandidates = this.swapCandidates.filterNot(removedCandidates.contains),
      swapChecked = true
    )
    case SplitChecked(_) =>
      throw new UnsupportedOperationException("SwapReadyCandidateState can not update the split candidates")
    case m => throw new UnsupportedOperationException(s"SwapReadyCandidateState can not be updated by $m")
  }
}

case class ReadyCandidateState(
    id: CandidateSet,
    splitCandidates: CandidateSet,
    swapCandidates: Seq[(Int, Int)]
) extends CandidateState {

  override val splitChecked: Boolean = false
  override val swapChecked: Boolean = false
  override val isPruned: Boolean = false

  override def isReadyToCheck(jobType: JobType.JobType): Boolean = jobType match {
    case JobType.Split => !splitChecked
    case JobType.Swap => !swapChecked
  }

  override protected def incSplitPreconditions: CandidateState = this

  override protected def incSwapPreconditions: CandidateState = this

  override def updated(delta: CandidateState.Delta): CandidateState = delta match {
    case SplitChecked(removedCandidates) => SplitCheckedCandidateState(
      id = id,
      splitCandidates = this.splitCandidates -- removedCandidates,
      swapCandidates = this.swapCandidates
    )
    case SwapChecked(removedCandidates) => SwapCheckedCandidateState(
      id = id,
      splitCandidates = this.splitCandidates,
      swapCandidates = this.swapCandidates.filterNot(removedCandidates.contains)
    )
    case m => throw new UnsupportedOperationException(s"SwapReadyCandidateState can not be updated by $m")
  }
}

case class SplitCheckedCandidateState(
    id: CandidateSet,
    splitCandidates: CandidateSet,
    swapCandidates: Seq[(Int, Int)]
) extends CandidateState {

  override val splitChecked: Boolean = true
  override val swapChecked: Boolean = false
  override val isPruned: Boolean = false

  override def isReadyToCheck(jobType: JobType.JobType): Boolean = jobType match {
    case JobType.Split => false
    case JobType.Swap => true
  }

  override protected def incSplitPreconditions: CandidateState = this

  override protected def incSwapPreconditions: CandidateState = this

  override def updated(delta: CandidateState.Delta): CandidateState = delta match {
    case SwapChecked(removedCandidates) => FullyCheckedCandidateState(
      id = this.id,
      splitCandidates = this.splitCandidates,
      swapCandidates = this.swapCandidates.filterNot(removedCandidates.contains)
    )
    case m => throw new UnsupportedOperationException(s"SplitCheckedCandidateState can not be updated by $m")
  }
}

case class SwapCheckedCandidateState(
    id: CandidateSet,
    splitCandidates: CandidateSet,
    swapCandidates: Seq[(Int, Int)]
) extends CandidateState {

  override val splitChecked: Boolean = false
  override val swapChecked: Boolean = true
  override val isPruned: Boolean = false

  override def isReadyToCheck(jobType: JobType.JobType): Boolean = jobType match {
    case JobType.Split => true
    case JobType.Swap => false
  }

  override protected def incSplitPreconditions: CandidateState = this

  override protected def incSwapPreconditions: CandidateState = this

  override def updated(delta: CandidateState.Delta): CandidateState = delta match {
    case SplitChecked(removedCandidates) => FullyCheckedCandidateState(
      id = this.id,
      splitCandidates = this.splitCandidates -- removedCandidates,
      swapCandidates = this.swapCandidates
    )
    case m => throw new UnsupportedOperationException(s"SwapCheckedCandidateState can not be updated by $m")
  }
}

case class FullyCheckedCandidateState(id: CandidateSet, splitCandidates: CandidateSet, swapCandidates: Seq[(Int, Int)])
  extends CandidateState {

  override val splitChecked: Boolean = true
  override val swapChecked: Boolean = true
  override val isPruned: Boolean = false

  override def isReadyToCheck(jobType: JobType.JobType): Boolean = false

  override protected def incSplitPreconditions: CandidateState = this

  override protected def incSwapPreconditions: CandidateState = this

  override def updated(delta: CandidateState.Delta): CandidateState =
    throw new UnsupportedOperationException(s"FullyCheckedCandidateState can not be updated")
}
