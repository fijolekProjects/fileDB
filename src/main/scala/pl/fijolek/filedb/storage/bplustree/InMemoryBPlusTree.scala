package pl.fijolek.filedb.storage.bplustree

import java.util.concurrent.atomic.AtomicLong

import InMemoryBPlusTree.RefId

import scala.annotation.tailrec
import scala.collection.IterableLike
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.TreeMap
import CollectionImplicits._

case class InMemoryBPlusTree private(root: RootNodeAbstract, degree: Int, nodes: Map[RefId, InnerNode], rootRefId: RefId) {

    def insert(record: Record): InMemoryBPlusTree = {
      root match {
        case SingleNodeTree(records) =>
          val newRecords = (record :: records).sortBy(_.key)
          val rootWithNewRecord = SingleNodeTree(newRecords)
          val result = growTree(rootWithNewRecord, RootRef, List.empty, Map.empty)
          this.copy(root = result.root, nodes = result.nodes)
        case _: Root =>
          val (_, ref, parents) = searchWithRef(record.key)
          val node = findById[Leaf](ref.internalId)
          val newRecords = (record :: node.records).sortBy(_.key)
          val leaf = node.copy(records = newRecords)
          val result = growTree(leaf, ref, parents, Map.empty)
          this.copy(root = result.root, nodes = result.nodes)
      }
    }

  case class GrowTreeResult(root: RootNodeAbstract, nodes: Map[RefId, InnerNode])

  @tailrec
  private def growTree(node: Node, currentNodeRef: Ref, parents: List[(Ref, ParentNode)], newNodes: Map[RefId, InnerNode]): GrowTreeResult = {
    node match {
      case innerNode: InnerNode =>
        if (innerNode.size == degree) {
          val (parentRef, parentNode) :: otherParents = parents
          val splitResult = split(innerNode, currentNodeRef.internalId)
          val newRefs = (splitResult.newRefs ++ parentNode.refs).distinctBy(_.key).sortBy(_.key)
          val newKeys = (splitResult.keyToPromote :: parentNode.keys).sorted
          val newParent = parentNode.withValues(keys = newKeys, refs = newRefs)
          growTree(newParent, parentRef, otherParents, newNodes ++ splitResult.nodeIdPairs)
        } else {
          GrowTreeResult(root, this.nodes ++ newNodes ++ Map(currentNodeRef.internalId -> innerNode))
        }
      case r: RootNodeAbstract =>
        if (r.size == degree) {
          val rootSplitResult = split(r, rootRefId)
          val newRoot = Root(keys = List(rootSplitResult.keyToPromote), refs = rootSplitResult.newRefs)
          growTree(newRoot, RootRef, List.empty, newNodes ++ rootSplitResult.nodeIdPairs)
        } else {
          GrowTreeResult(r, nodes = this.nodes ++ newNodes)
        }
    }
  }

  case class SplitResult(keyToPromote: Long, leftNode: InnerNode, leftRefId: RefId, rightNode: InnerNode, rightRefId: RefId) {
    def key(n: InnerNode) = n match {
      case Internal(keys, _) => keys.head
      case Leaf(records) => records.head.key
    }
    val newRefs = List(Ref(key(leftNode), leftRefId), Ref(key(rightNode), rightRefId))
    val nodeIdPairs = Map((leftRefId, leftNode), (rightRefId, rightNode))
  }

  private def split(node: Node, refId: RefId): SplitResult = {
    def splitRecords(records: List[Record], allocateSpaceForBothNewNodes: Boolean): SplitResult = {
      val (leftRecords, rightRecords) = records.splitInHalves
      val (leftRefId, rightRefId) = (nextRefId(), if (allocateSpaceForBothNewNodes) nextRefId() else refId)
      val keyToPromote = rightRecords.head.key
      SplitResult(keyToPromote, Leaf(leftRecords), leftRefId, Leaf(rightRecords), rightRefId)
    }
    def splitKeyRefs(keys: List[Long], refs: List[Ref], allocateSpaceForBothNewNodes: Boolean): SplitResult = {
      val medianKeyIndex = keys.size / 2
      val medianKey = keys(medianKeyIndex)
      val keysWithoutMedianValue = keys.zipWithIndex.filter(_._2 != medianKeyIndex).map(_._1)
      val (leftKeys, rightKeys) = keysWithoutMedianValue.splitInHalves
      val (leftRefs, rightRefs) = refs.partition(_.key < medianKey)
      val (leftRefId, rightRefId) = (nextRefId(), if (allocateSpaceForBothNewNodes) nextRefId() else refId)
      SplitResult(medianKey, Internal(leftKeys, leftRefs), leftRefId, Internal(rightKeys, rightRefs), rightRefId)
    }
    node match {
      case Leaf(records) =>
        splitRecords(records, allocateSpaceForBothNewNodes = false)
      case SingleNodeTree(records) =>
        splitRecords(records, allocateSpaceForBothNewNodes = true)
      case Internal(keys, refs) =>
        splitKeyRefs(keys, refs, allocateSpaceForBothNewNodes = false)
      case Root(keys, refs) =>
        splitKeyRefs(keys, refs, allocateSpaceForBothNewNodes = true)
    }
  }

  def search(key: Long): Option[Record]  = {
    searchWithRef(key)._1
  }

  private def searchWithRef(key: Long): (Option[Record], Ref, List[(Ref, ParentNode)])  = {
    @tailrec
    def findLeaf(key: Long, keys: List[Long], refs: List[Ref], parents: List[(Ref, ParentNode)]): (Option[Record], Ref, List[(Ref, ParentNode)]) = {
      val refIdx = keys.toIterator.zipWithIndex.find(_._1 > key).map(_._2).getOrElse(keys.size)
      val ref = refs(refIdx)
      findById[InnerNode](ref.internalId) match {
        case Leaf(records) =>
          (records.find(_.key == key), ref, parents)
        case internal@Internal(keys, refs) =>
          findLeaf(key, keys, refs, (ref, internal) :: parents)
      }
    }

    root match {
      case SingleNodeTree(records) =>
        (records.find(_.key == key), RootRef, List.empty)
      case root@Root(keys, refs) =>
        findLeaf(key, keys, refs, List((RootRef, root)))
    }
  }

  def prettyPrint(): PrettyTree = {
    new PrettyPrinter(root, nodes).prettyPrint()
  }

  private def findById[N <: InnerNode](id: RefId): N = {
    nodes(id).asInstanceOf[N]
  }

  private def nextRefId(): RefId = {
    InMemoryBPlusTree.nextId()
  }


}

object InMemoryBPlusTree {
  type RefId = Long
  def apply(degree: Int): InMemoryBPlusTree = {
    new InMemoryBPlusTree(SingleNodeTree(List.empty), degree, Map.empty, RootRef.internalId)
  }

  private val idGenerator = new AtomicLong(0)
  def nextId(): Long = idGenerator.incrementAndGet()
}

sealed trait Node {
  val size: Int = {
    this match {
      case r: Root => r.keys.size
      case r: SingleNodeTree => r.records.size
      case i: Internal => i.keys.size
      case l: Leaf => l.records.size
    }
  }

}
sealed trait RootNodeAbstract extends Node

sealed trait InnerNode extends Node
sealed trait ParentNode extends Node {
  val keys: List[Long]
  val refs: List[Ref]

  def withValues(keys: List[Long], refs: List[Ref]): ParentNode = {
    this match {
      case r: Root => r.copy(keys = keys, refs = refs)
      case i: Internal => i.copy(keys = keys, refs = refs)
    }
  }
}
case class SingleNodeTree(records: List[Record]) extends RootNodeAbstract
case class Root(keys: List[Long], refs: List[Ref]) extends RootNodeAbstract with ParentNode
case class Internal(keys: List[Long], refs: List[Ref]) extends InnerNode with ParentNode
case class Leaf(records: List[Record]) extends InnerNode
case class Record(key: Long, value: Any)

case class Ref(key: Long, internalId: RefId)
object RootRef extends Ref(-1, -1) //TODO what about this key?

case class PrettyTree(root: RootNodeAbstract, internals: List[List[Internal]], leafs: List[Leaf], nodes: Map[RefId, InnerNode]) {
  override def toString: String = {
    s"""
      |Tree:
      |$root
      |${internals.mkString("\n")}
      |$leafs
      |nodes: ${TreeMap.apply(nodes.toSeq: _*)}
    """.stripMargin
  }
}


class PrettyPrinter(root: RootNodeAbstract, nodes: Map[RefId, InnerNode]) {
  def prettyPrint(): PrettyTree = {
    val leafs = printLeafs()
    val internals = printInternals()
    PrettyTree(root, internals, leafs, nodes)
  }

  private def printLeafs(): List[Leaf] = {
    loopThroughNodes[Leaf] {
      case (Internal(_, refs), thisFun) =>
        val innerRefs = refs.map(ref => nodes(ref.internalId))
        innerRefs.flatMap(ref => thisFun(ref))
      case (leaf@Leaf(_), _) =>
        List(leaf)
    }.flatten
  }

  private def printInternals(): List[List[Internal]] = {
    loopThroughNodes[Internal] {
      case (internal@Internal(_, refs), thisFun) =>
        val innerRefs = refs.map(ref => nodes(ref.internalId))
        internal :: innerRefs.flatMap(in => thisFun(in))
      case (Leaf(_), _) =>
        List()
    }
  }

  private def loopThroughNodes[T <: InnerNode](collectFun: (InnerNode, InnerNode => List[T]) => List[T]): List[List[T]] = {
    def print(innerNode: InnerNode): List[T] = {
      collectFun(innerNode, print _)
    }
    root match {
      case _: SingleNodeTree =>
        List()
      case Root(_, refs) =>
        val innerNodes = refs.map(ref => nodes(ref.internalId))
        innerNodes.map(n => print(n))
    }
  }
}

object CollectionImplicits {

  implicit class RichCollection[A, Repr](xs: IterableLike[A, Repr]) {
    def distinctBy[B, That](f: A => B)(implicit cbf: CanBuildFrom[Repr, A, That]): That = {
      val builder = cbf(xs.repr)
      val i = xs.iterator
      var set = Set[B]()
      while (i.hasNext) {
        val o = i.next
        val b = f(o)
        if (!set(b)) {
          set += b
          builder += o
        }
      }
      builder.result
    }

    def splitInHalves: (Repr, Repr) = {
      val size = xs.size
      val (left, right) = (xs.take(size / 2), xs.drop(size / 2))
      (left, right)
    }
  }
}
