package pl.fijolek.filedb.storage.bplustree

import pl.fijolek.filedb.storage.bplustree.BPlusTree.RefId
import pl.fijolek.filedb.storage.{DbConstants, Page}

object BPlusTreeProtocol {

  import java.nio.ByteBuffer

  def recordToBytes(r: Record): Array[Byte] = {
    ByteBuffer.allocate(2 * java.lang.Long.BYTES)
      .putLong(r.key)
      .putLong(r.value)
      .array()
  }

  def bytesToRecord(b: ByteBuffer): Record = {
    val key = b.getLong()
    val value = b.getLong()
    Record(key, value)
  }

  def bytesToRef(b: ByteBuffer): Ref = {
    val key = b.getLong()
    val internalId = b.getLong()
    Ref(key, internalId)
  }

  def bytesToKey(b: ByteBuffer): Long = {
    val key = b.getLong()
    key
  }

  def keyToBytes(key: Long): Array[Byte] = {
    ByteBuffer.allocate(java.lang.Long.BYTES)
      .putLong(key)
      .array()
  }

  def refToBytes(ref: Ref): Array[Byte] = {
    ByteBuffer.allocate(2 * java.lang.Long.BYTES)
      .putLong(ref.key)
      .putLong(ref.internalId)
      .array()
  }

  def nodeTypeToChar(n: Node): Char = {
    val c = n match {
      case _: SingleNodeTree =>
        's'
      case _: Root =>
        'r'
      case _: Internal =>
        'i'
      case _: Leaf =>
        'l'
    }
    c
  }

  def charTyNodeType(c: Char): String = {
    c match {
      case 's' =>
        SingleNodeTree.getClass.getSimpleName.init
      case 'r' =>
        Root.getClass.getSimpleName.init
      case 'i' =>
        Internal.getClass.getSimpleName.init
      case 'l' =>
        Leaf.getClass.getSimpleName.init
    }
  }

  def nodeToPage(n: Node, refId: RefId, fileId: Long): Page = {
    val offset = computeOffset(refId)
    val page = Page.newPage(fileId, offset)
    val nodeTypeEnum = nodeTypeToChar(n)
    val bytesToWrite = n match {
      case SingleNodeTree(degree, records) =>
        val recordBytes = records.toArray.flatMap(rec => recordToBytes(rec))
        ByteBuffer.allocate(DbConstants.pageDataSize)
          .putChar(nodeTypeEnum)
          .putInt(degree)
          .putInt(records.size)
          .put(recordBytes)
          .array()
      case Root(degree, keys, refs) =>
        val keysBytes = keys.toArray.flatMap(k => keyToBytes(k))
        val refsBytes = refs.toArray.flatMap(r => refToBytes(r))
        ByteBuffer.allocate(DbConstants.pageDataSize)
          .putChar(nodeTypeEnum)
          .putInt(degree)
          .putInt(keys.size)
          .put(keysBytes)
          .putInt(refs.size)
          .put(refsBytes)
          .array()
      case Internal(keys, refs) =>
        val keysBytes = keys.toArray.flatMap(k => keyToBytes(k))
        val refsBytes = refs.toArray.flatMap(r => refToBytes(r))
        ByteBuffer.allocate(DbConstants.pageDataSize)
          .putChar(nodeTypeEnum)
          .putInt(keys.size)
          .put(keysBytes)
          .putInt(refs.size)
          .put(refsBytes)
          .array()
      case Leaf(records) =>
        val recordBytes = records.toArray.flatMap(rec => recordToBytes(rec))
        ByteBuffer.allocate(DbConstants.pageDataSize)
          .putChar(nodeTypeEnum)
          .putInt(records.size)
          .put(recordBytes)
          .array()
    }
    page.add(bytesToWrite)
  }

  def pageToNode(page: Page): Node = {
    val pageToRead = ByteBuffer.wrap(page.dataBytes)
    charTyNodeType(pageToRead.getChar) match {
      case "SingleNodeTree" =>
        val degree = pageToRead.getInt
        val recordsSize = pageToRead.getInt
        val records = (0 until recordsSize).map { _ =>
          val buffer = new Array[Byte](2 * java.lang.Long.BYTES)
          pageToRead.get(buffer)
          bytesToRecord(ByteBuffer.wrap(buffer))
        }.toList
        SingleNodeTree(degree, records)
      case "Root" =>
        val degree = pageToRead.getInt
        val keysSize = pageToRead.getInt
        val keys = (0 until keysSize).map { _ =>
          val buffer = new Array[Byte](java.lang.Long.BYTES)
          pageToRead.get(buffer)
          bytesToKey(ByteBuffer.wrap(buffer))
        }.toList
        val refsSize = pageToRead.getInt
        val refs = (0 until refsSize).map { _ =>
          val buffer = new Array[Byte](2 * java.lang.Long.BYTES)
          pageToRead.get(buffer)
          bytesToRef(ByteBuffer.wrap(buffer))
        }.toList
        Root(degree, keys, refs)
      case "Internal" =>
        val keysSize = pageToRead.getInt
        val keys = (0 until keysSize).map { _ =>
          val buffer = new Array[Byte](java.lang.Long.BYTES)
          pageToRead.get(buffer)
          bytesToKey(ByteBuffer.wrap(buffer))
        }.toList
        val refsSize = pageToRead.getInt
        val refs = (0 until refsSize).map { _ =>
          val buffer = new Array[Byte](2 * java.lang.Long.BYTES)
          pageToRead.get(buffer)
          bytesToRef(ByteBuffer.wrap(buffer))
        }.toList
        Internal(keys, refs)
      case "Leaf" =>
        val recordsSize = pageToRead.getInt
        val records = (0 until recordsSize).map { _ =>
          val buffer = new Array[Byte](2 * java.lang.Long.BYTES)
          pageToRead.get(buffer)
          bytesToRecord(ByteBuffer.wrap(buffer))
        }.toList
        Leaf(records)
    }
  }

  def computeOffset(refId: RefId): Long = {
    refId * DbConstants.pageSize
  }
}
