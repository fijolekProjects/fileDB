package pl.fijolek.filedb.storage.bplustree

import java.util.concurrent.atomic.AtomicLong

import InMemoryBPlusTree.{RefId, RefIdNode, RefNode}

import scala.annotation.tailrec
import scala.collection.IterableLike
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.TreeMap
import CollectionImplicits._

case class InMemoryBPlusTree private(degree: Int, root: RefIdNode[RootNodeAbstract], nodes: Map[RefId, InnerNode]) {
  import InMemoryBPlusTree.inMemoryBPlusTreeModification

  def insert(record: Record): InMemoryBPlusTree = {
    root.node match {
      case SingleNodeTree(records) =>
        val newRecords = (record :: records).sortBy(_.key)
        val rootWithNewRecord = SingleNodeTree(newRecords)
        val result = inMemoryBPlusTreeModification.growTree(degree, root, RefIdNode(RootRef.internalId, rootWithNewRecord), List.empty)
        this.copy(root = result.root, nodes = this.nodes ++ result.changedNodes)
      case _: Root =>
        val (_, ref, parents) = searchWithRef(record.key)
        val node = findById[Leaf](ref.internalId)
        val newRecords = (record :: node.records).sortBy(_.key)
        val leaf = node.copy(records = newRecords)
        val result = inMemoryBPlusTreeModification.growTree(degree, root, RefIdNode(ref.internalId, leaf), parents)
        this.copy(root = result.root, nodes = this.nodes ++ result.changedNodes)
    }
  }

  def search(key: Long): Option[Record] = {
    searchWithRef(key)._1
  }

  private def searchWithRef(key: Long): (Option[Record], Ref, List[RefNode[ParentNode]]) = {
    InMemoryBPlusTree.searchWithRef(root.node, key)(refId => findById[InnerNode](refId))
  }

  def prettyPrint(): PrettyTree = {
    new PrettyPrinter(root.node, nodes).prettyPrint()
  }

  private def findById[N <: InnerNode](id: RefId): N = {
    nodes(id).asInstanceOf[N]
  }

}

object InMemoryBPlusTree {

  type RefId = Long
  private val idGenerator = new AtomicLong(0)
  private def nextRefId(): RefId = {
    idGenerator.incrementAndGet()
  }

  case class RefIdNode[N <: Node](refId: RefId, node: N)
  case class RefNode[N <: Node](ref: Ref, node: N)

  val inMemoryBPlusTreeModification = new InMemoryBPlusTreeModification(() => nextRefId())

  def apply(degree: Int): InMemoryBPlusTree = {
    new InMemoryBPlusTree(degree, RefIdNode(RootRef.internalId, SingleNodeTree(List.empty)), Map.empty)
  }

  def searchWithRef(root: RootNodeAbstract, key: Long)(findById: RefId => InnerNode): (Option[Record], Ref, List[RefNode[ParentNode]])  = {

    @tailrec
    def findLeaf(key: Long, keys: List[Long], refs: List[Ref], parents: List[RefNode[ParentNode]]): (Option[Record], Ref, List[RefNode[ParentNode]]) = {
      val refIdx = keys.toIterator.zipWithIndex.find(_._1 > key).map(_._2).getOrElse(keys.size)
      val ref = refs(refIdx)
      findById(ref.internalId) match {
        case Leaf(records) =>
          (records.find(_.key == key), ref, parents)
        case internal@Internal(internalKeys, internalRefs) =>
          findLeaf(key, internalKeys, internalRefs, RefNode[ParentNode](ref, internal) :: parents)
      }
    }

    root match {
      case SingleNodeTree(records) =>
        (records.find(_.key == key), RootRef, List.empty)
      case root@Root(keys, refs) =>
        findLeaf(key, keys, refs, List(RefNode[ParentNode](RootRef, root)))
    }
  }

}

class InMemoryBPlusTreeModification(nextRefId: () => RefId) {
  case class GrowTreeResult(root: RefIdNode[RootNodeAbstract], changedNodes: Map[RefId, InnerNode])

  case class SplitResult(keyToPromote: Long, leftNode: RefIdNode[InnerNode], rightNode: RefIdNode[InnerNode]) {
    def key(n: InnerNode) = n match {
      case Internal(keys, _) => keys.head
      case Leaf(records) => records.head.key
    }
    val newRefs = List(Ref(key(leftNode.node), leftNode.refId), Ref(key(rightNode.node), rightNode.refId))
    val nodeIdPairs = Map((leftNode.refId, leftNode.node), (rightNode.refId, rightNode.node))
  }

  def growTree(degree: Int, root: RefIdNode[RootNodeAbstract], currentNode: RefIdNode[Node], parents: List[RefNode[ParentNode]]): GrowTreeResult = {
    innerGrowTree(degree, root, currentNode, parents, Map.empty)
  }

  @tailrec
  private def innerGrowTree(degree: Int, root: RefIdNode[RootNodeAbstract], currentNode: RefIdNode[Node], parents: List[RefNode[ParentNode]], newNodes: Map[RefId, InnerNode]): GrowTreeResult = {
    currentNode.node match {
      case innerNode: InnerNode =>
        if (innerNode.size == degree) {
          val RefNode(parentRef, parentNode) :: otherParents = parents
          val splitResult = split(currentNode)
          val newRefs = (splitResult.newRefs ++ parentNode.refs).distinctBy(_.key).sortBy(_.key)
          val newKeys = (splitResult.keyToPromote :: parentNode.keys).sorted
          val newParent = parentNode.withValues(keys = newKeys, refs = newRefs)
          innerGrowTree(degree, root, RefIdNode(parentRef.internalId, newParent), otherParents, newNodes ++ splitResult.nodeIdPairs)
        } else {
          GrowTreeResult(root, newNodes ++ Map(currentNode.refId -> innerNode))
        }
      case r: RootNodeAbstract =>
        if (r.size == degree) {
          val rootSplitResult = split(currentNode)
          val newRoot = Root(keys = List(rootSplitResult.keyToPromote), refs = rootSplitResult.newRefs)
          innerGrowTree(degree, root, RefIdNode(RootRef.internalId, newRoot), List.empty, newNodes ++ rootSplitResult.nodeIdPairs)
        } else {
          GrowTreeResult(root.copy(node = r), changedNodes = newNodes)
        }
    }
  }

  private def split(node: RefIdNode[Node]): SplitResult = {
    def splitRecords(records: List[Record], allocateSpaceForBothNewNodes: Boolean): SplitResult = {
      val (leftRecords, rightRecords) = records.splitInHalves
      val (leftRefId, rightRefId) = (nextRefId(), if (allocateSpaceForBothNewNodes) nextRefId() else node.refId)
      val keyToPromote = rightRecords.head.key
      SplitResult(keyToPromote, RefIdNode(leftRefId, Leaf(leftRecords)), RefIdNode(rightRefId, Leaf(rightRecords)))
    }
    def splitKeyRefs(keys: List[Long], refs: List[Ref], allocateSpaceForBothNewNodes: Boolean): SplitResult = {
      val medianKeyIndex = keys.size / 2
      val medianKey = keys(medianKeyIndex)
      val keysWithoutMedianValue = keys.zipWithIndex.filter(_._2 != medianKeyIndex).map(_._1)
      val (leftKeys, rightKeys) = keysWithoutMedianValue.splitInHalves
      val (leftRefs, rightRefs) = refs.partition(_.key < medianKey)
      val (leftRefId, rightRefId) = (nextRefId(), if (allocateSpaceForBothNewNodes) nextRefId() else node.refId)
      SplitResult(medianKey, RefIdNode(leftRefId, Internal(leftKeys, leftRefs)), RefIdNode(rightRefId, Internal(rightKeys, rightRefs)) )
    }
    node.node match {
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
