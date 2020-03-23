package com.github.codelionx.distod.actors.master

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo, JsonTypeName}
import com.github.codelionx.distod.Serialization.CborSerializable
import com.github.codelionx.distod.actors.master.CandidateState._
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
  case class L1CandidateState private(id: CandidateSet, override val splitCandidates: CandidateSet) extends CandidateState {

    override val splitChecked: Boolean = false
    override val swapChecked: Boolean = true
    override val isFullyGenerated: Boolean = true
    override val swapCandidates: Seq[(Int, Int)] = Seq.empty

    override def isReadyToGenerate(jobType: JobType.JobType): Boolean = jobType match {
      case JobType.Split => true
      case JobType.Swap => false
    }

    override def updated(delta: Delta): CandidateState = delta match {
      case Prune() =>
        this.prune
      case IncPrecondition(_) =>
        this
      case SplitChecked(removedCandidates) => FullyCheckedCandidateState(
        id = this.id,
        splitCandidates = this.splitCandidates -- removedCandidates,
        swapCandidates = this.swapCandidates
      )
      case m => throw new UnsupportedOperationException(s"L1 Candidate State can not be updated by $m")
    }
  }

  // predecessors (L1) do not perform swap checks, so the preconditions for the L2 swap checks are already met
  def initForL2(id: CandidateSet): CandidateState = InitialCandidateState(id, swapPreconditions = id.size)

  def pruned(id: CandidateSet): CandidateState = PrunedCandidateState(id)

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(Array(
    new JsonSubTypes.Type(value = classOf[Prune], name = "Prune"),
    new JsonSubTypes.Type(value = classOf[IncPrecondition], name = "IncPrecondition"),
    new JsonSubTypes.Type(value = classOf[NewSplitCandidates], name = "NewSplitCandidates"),
    new JsonSubTypes.Type(value = classOf[NewSwapCandidates], name = "NewSwapCandidates"),
    new JsonSubTypes.Type(value = classOf[SplitChecked], name = "SplitChecked"),
    new JsonSubTypes.Type(value = classOf[SwapChecked], name = "SwapChecked"),
  ))
  sealed trait Delta extends Ordered[Delta] with CborSerializable {

    protected def orderingId: Int

    override def compare(that: Delta): Int = this.orderingId - that.orderingId
  }

  // must be a case class (instead of object), because of Jackson (de-)serialization support
  @JsonTypeName("Prune")
  final case class Prune() extends Delta {

    override protected val orderingId: Int = -1
  }

  @JsonTypeName("IncPrecondition")
  final case class IncPrecondition(jobType: JobType.JobType) extends Delta {

    override protected val orderingId: Int = 0
  }

  @JsonTypeName("NewSplitCandidates")
  final case class NewSplitCandidates(splitCandidates: CandidateSet) extends Delta {

    override protected val orderingId: Int = 1
  }

  @JsonTypeName("NewSwapCandidates")
  final case class NewSwapCandidates(swapCandidates: Seq[(Int, Int)]) extends Delta {

    override protected val orderingId: Int = 2
  }

  @JsonTypeName("SplitChecked")
  final case class SplitChecked(removedCandidates: CandidateSet) extends Delta {

    override protected val orderingId: Int = 3
  }

  @JsonTypeName("SwapChecked")
  final case class SwapChecked(removedCandidates: Seq[(Int, Int)]) extends Delta {

    override protected val orderingId: Int = 4
  }

}


sealed trait CandidateState {

  // state
  def id: CandidateSet

  def splitCandidates: CandidateSet = throw new IllegalAccessException("Split candidates are not yet ready!")

  def swapCandidates: Seq[(Int, Int)] = throw new IllegalAccessException("Swap candidates are not yet ready!")

  def isFullyGenerated: Boolean

  def splitChecked: Boolean

  def swapChecked: Boolean

  // state transitions
  def isReadyToGenerate(jobType: JobType.JobType): Boolean

  def updated(delta: CandidateState.Delta): CandidateState

  // helper methods -----------------------------------------------------
  // testing functions
  def isFullyChecked: Boolean = splitChecked && swapChecked

  def isPruned: Boolean = false

  def isNotPruned: Boolean = !isPruned

  def notReadyToCheck(jobType: JobType.JobType): Boolean = !isReadyToGenerate(jobType)

  def shouldBePruned: Boolean = isFullyChecked && splitCandidates.isEmpty && swapCandidates.isEmpty

  protected def prune: CandidateState = CandidateState.pruned(id)

  // state transitions
  def updatedAll(deltas: Iterable[CandidateState.Delta]): CandidateState = {
    val sortedDeltas = deltas.toSeq.sorted

    // avoid calling update multiple times on a pruned state (pruning will be the first step)
    if(sortedDeltas.headOption.contains(Prune())) {
      this.prune
    } else {
      sortedDeltas.foldLeft(this) { case (state, delta) =>
        state.updated(delta)
      }
    }
  }
}

case class PrunedCandidateState(id: CandidateSet) extends CandidateState {

  override val splitChecked: Boolean = true
  override val swapChecked: Boolean = true
  override val isPruned: Boolean = true
  override val isFullyGenerated: Boolean = true
  override val splitCandidates: CandidateSet = CandidateSet.empty
  override val swapCandidates: Seq[(Int, Int)] = Seq.empty

  override def prune: CandidateState = this

  override def isReadyToGenerate(jobType: JobType.JobType): Boolean = false

  override def updated(delta: CandidateState.Delta): CandidateState =
    // silently ignore updates
    this
}

case class InitialCandidateState(
    id: CandidateSet,
    private val splitPreconditions: Int = 0,
    private val swapPreconditions: Int = 0
) extends CandidateState {

  override val splitChecked: Boolean = false
  override val swapChecked: Boolean = false
  override val isFullyGenerated: Boolean = false

  override def isReadyToGenerate(jobType: JobType.JobType): Boolean = jobType match {
    case JobType.Split => id.size == splitPreconditions
    case JobType.Swap => id.size == splitPreconditions && id.size == swapPreconditions
  }

  override def updated(delta: CandidateState.Delta): CandidateState = delta match {
    case Prune() =>
      this.prune
    case IncPrecondition(JobType.Split) =>
      this.copy(splitPreconditions = this.splitPreconditions + 1)
    case IncPrecondition(JobType.Swap) =>
      this.copy(swapPreconditions = this.swapPreconditions + 1)
    case NewSplitCandidates(splitCandidates) if isReadyToGenerate(JobType.Split) =>
      SplitReadyCandidateState(
        id = id,
        splitCandidates = splitCandidates,
        swapPreconditions = swapPreconditions
      )
    case NewSwapCandidates(swapCandidates) if isReadyToGenerate(JobType.Swap) =>
      SwapReadyCandidateState(
        id = id,
        swapCandidates = swapCandidates
      )
    case m => throw new UnsupportedOperationException(s"InitialCandidateState can not be updated by $m")
  }
}

case class SplitReadyCandidateState(
    id: CandidateSet,
    override val splitCandidates: CandidateSet,
    swapPreconditions: Int,
    splitChecked: Boolean = false
) extends CandidateState {

  override val swapChecked: Boolean = false
  override val isFullyGenerated: Boolean = false

  override def isReadyToGenerate(jobType: JobType.JobType): Boolean = jobType match {
    case JobType.Split => !splitChecked
    case JobType.Swap => id.size == swapPreconditions
  }

  override def updated(delta: CandidateState.Delta): CandidateState = delta match {
    case Prune() =>
      this.prune
    case IncPrecondition(JobType.Split) =>
      this
    case IncPrecondition(JobType.Swap) =>
      this.copy(swapPreconditions = this.swapPreconditions + 1)
    case NewSwapCandidates(swapCandidates) if isReadyToGenerate(JobType.Swap) && this.splitChecked =>
      SplitCheckedCandidateState(
        id = id,
        splitCandidates = this.splitCandidates,
        swapCandidates = swapCandidates
      )
    case NewSwapCandidates(swapCandidates) if isReadyToGenerate(JobType.Swap) && !this.splitChecked =>
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
    override val swapCandidates: Seq[(Int, Int)],
    swapChecked: Boolean = false
) extends CandidateState {

  override val splitChecked: Boolean = false
  override val isFullyGenerated: Boolean = false

  override def isReadyToGenerate(jobType: JobType.JobType): Boolean = jobType match {
    case JobType.Split => true
    case JobType.Swap => !swapChecked
  }

  override def updated(delta: CandidateState.Delta): CandidateState = delta match {
    case Prune() =>
      this.prune
    case IncPrecondition(_) =>
      this
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
    override val splitCandidates: CandidateSet,
    override val swapCandidates: Seq[(Int, Int)]
) extends CandidateState {

  override val splitChecked: Boolean = false
  override val swapChecked: Boolean = false
  override val isFullyGenerated: Boolean = true

  override def isReadyToGenerate(jobType: JobType.JobType): Boolean = jobType match {
    case JobType.Split => !splitChecked
    case JobType.Swap => !swapChecked
  }

  override def updated(delta: CandidateState.Delta): CandidateState = delta match {
    case Prune() =>
      this.prune
    case IncPrecondition(_) =>
      this
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
    override val splitCandidates: CandidateSet,
    override val swapCandidates: Seq[(Int, Int)]
) extends CandidateState {

  override val splitChecked: Boolean = true
  override val swapChecked: Boolean = false
  override val isFullyGenerated: Boolean = true

  override def isReadyToGenerate(jobType: JobType.JobType): Boolean = jobType match {
    case JobType.Split => false
    case JobType.Swap => true
  }

  override def updated(delta: CandidateState.Delta): CandidateState = delta match {
    case Prune() =>
      this.prune
    case IncPrecondition(_) =>
      this
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
    override val splitCandidates: CandidateSet,
    override val swapCandidates: Seq[(Int, Int)]
) extends CandidateState {

  override val splitChecked: Boolean = false
  override val swapChecked: Boolean = true
  override val isFullyGenerated: Boolean = true

  override def isReadyToGenerate(jobType: JobType.JobType): Boolean = jobType match {
    case JobType.Split => true
    case JobType.Swap => false
  }

  override def updated(delta: CandidateState.Delta): CandidateState = delta match {
    case Prune() =>
      this.prune
    case IncPrecondition(_) =>
      this
    case SplitChecked(removedCandidates) => FullyCheckedCandidateState(
      id = this.id,
      splitCandidates = this.splitCandidates -- removedCandidates,
      swapCandidates = this.swapCandidates
    )
    case m => throw new UnsupportedOperationException(s"SwapCheckedCandidateState can not be updated by $m")
  }
}

case class FullyCheckedCandidateState(
      id: CandidateSet,
      override val splitCandidates: CandidateSet,
      override val swapCandidates: Seq[(Int, Int)]
  ) extends CandidateState {

  override val splitChecked: Boolean = true
  override val swapChecked: Boolean = true
  override val isFullyGenerated: Boolean = true

  override def isReadyToGenerate(jobType: JobType.JobType): Boolean = false

  override def updated(delta: CandidateState.Delta): CandidateState = delta match {
    case Prune() => this.prune
    case _ => throw new UnsupportedOperationException(s"FullyCheckedCandidateState can not be updated")
  }
}
