package pl.fijolek.filedb.storage.bplustree

import java.nio.file.{Files, Paths}

import com.github.ghik.silencer.silent
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, Matchers}
import pl.fijolek.filedb.storage.{FileIdMapper, FileUtils, PageIO, PageId}

class DiskBasedBPlusTreeTest extends FeatureSpec with Matchers with BeforeAndAfterEach {
  val indexFileId = 10L
  val basePath = "/tmp/filedb"
  val hardcodedFilePath = s"$basePath/index$indexFileId"

  //TODO get rid of FileIdMapper stub here
  val fileIdMapper = new FileIdMapper(basePath) {
    override def path(fileId: Long): String = {
      FileUtils.touchFile(new java.io.File(hardcodedFilePath)) //because travis fails
      hardcodedFilePath
    }
  }
  val pageIO = new PageIO(fileIdMapper)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    cleanTables()
  }
  override protected def afterEach(): Unit = {
    super.afterEach()
    cleanTables()
  }

  @silent
  private def cleanTables(): Unit = {
    Files.deleteIfExists(Paths.get(hardcodedFilePath))
  }

  feature("disk based b plus tree") {
    scenario("should insert/search on tree with size < degree") {
      DiskBasedBPlusTree(degree = 4, pageIO = pageIO, fileId = indexFileId)
        .insert(Record(9, 9))
        .insert(Record(16, 16))
      val node = BPlusTreeProtocol.pageToNode(pageIO.read(PageId(10, 0)))

      node shouldBe SingleNodeTree(4, List(Record(9, 9), Record(16, 16)))
    }
    scenario("should insert/search on tree with size == degree") {
      DiskBasedBPlusTree(degree = 4, pageIO = pageIO, fileId = indexFileId)
        .insert(Record(9, 9))
        .insert(Record(16, 16))
        .insert(Record(2, 2))
        .insert(Record(7, 7))

      val root = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(0)))
      val leaf1 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(1)))
      val leaf2 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(2)))

      root shouldBe Root(4, List(9), List(Ref(2, 1), Ref(9, 2)))
      leaf1 shouldBe Leaf(List(Record(2, 2), Record(7, 7)))
      leaf2 shouldBe Leaf(List(Record(9, 9), Record(16, 16)))
    }

    scenario("should insert/search on tree with size > degree #1") {
      DiskBasedBPlusTree(degree = 4, pageIO = pageIO, fileId = indexFileId)
        .insert(Record(9, 9))
        .insert(Record(16, 16))
        .insert(Record(2, 2))
        .insert(Record(7, 7))
        .insert(Record(12, 12))
        .insert(Record(8, 8))

      val root = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(0)))
      val leaf1 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(1)))
      val leaf2 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(2)))

      root shouldBe Root(4,List(9),List(Ref(2,1), Ref(9,2)))
      leaf1 shouldBe Leaf(List(Record(2,2), Record(7,7), Record(8,8)))
      leaf2 shouldBe Leaf(List(Record(9,9), Record(12,12), Record(16,16)))
    }

    scenario("should insert/search on tree with size > degree #2") {
      DiskBasedBPlusTree(degree = 4, pageIO = pageIO, fileId = indexFileId)
        .insert(Record(9, 9))
        .insert(Record(16, 16))
        .insert(Record(2, 2))
        .insert(Record(7, 7))
        .insert(Record(12, 12))
        .insert(Record(10, 10))

      val root = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(0)))
      val leaf1 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(1)))
      val leaf2 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(2)))
      val leaf3 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(3)))

      root shouldBe Root(4,List(9, 12),List(Ref(2,1), Ref(9,3), Ref(12,2)))
      leaf1 shouldBe Leaf(List(Record(2,2), Record(7,7)))
      leaf3 shouldBe Leaf(List(Record(9,9), Record(10,10)))
      leaf2 shouldBe Leaf(List(Record(12,12), Record(16,16)))
    }

    scenario("should insert/search on tree with single level internal nodes") {
      DiskBasedBPlusTree(degree = 3, pageIO = pageIO, fileId = indexFileId)
        .insert(Record(1, 1))
        .insert(Record(2, 2))
        .insert(Record(3, 3))
        .insert(Record(4, 4))
        .insert(Record(5, 5))

      val root = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(0)))
      val leaf1 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(1)))
      val leaf2 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(2)))
      val leaf3 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(3)))
      val leaf4 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(4)))
      val internal5 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(5)))
      val internal6 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(6)))

      root shouldBe Root(3,List(3),List(Ref(2,5), Ref(4,6)))
      leaf1 shouldBe Leaf(List(Record(1,1)))
      leaf2 shouldBe Leaf(List(Record(4,4), Record(5,5)))
      leaf3 shouldBe Leaf(List(Record(2,2)))
      leaf4 shouldBe Leaf(List(Record(3,3)))
      internal5 shouldBe Internal(List(2),List(Ref(1,1), Ref(2,3)))
      internal6 shouldBe Internal(List(4),List(Ref(3,4), Ref(4,2)))

    }

    scenario("should insert/search on tree with dual level internal nodes") {
      DiskBasedBPlusTree(degree = 3, pageIO = pageIO, fileId = indexFileId)
        .insert(Record(1, 1))
        .insert(Record(2, 2))
        .insert(Record(3, 3))
        .insert(Record(4, 4))
        .insert(Record(5, 5))
        .insert(Record(6, 6))
        .insert(Record(7, 7))
        .insert(Record(8, 8))
        .insert(Record(9, 9))

      val root = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(0)))
      val leaf1 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(1)))
      val leaf2 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(2)))
      val leaf3 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(3)))
      val leaf4 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(4)))
      val internal5 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(5)))
      val internal6 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(6)))
      val leaf7 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(7)))
      val leaf8 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(8)))
      val internal9 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(9)))
      val leaf10 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(10)))
      val leaf11 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(11)))
      val internal12 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(12)))
      val internal13 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(13)))
      val internal14 = BPlusTreeProtocol.pageToNode(pageIO.read(indexPageId(14)))

      root shouldBe  Root(3,List(5),List(Ref(3,13), Ref(7,14)))
      leaf1 shouldBe Leaf(List(Record(1,1)))
      leaf2 shouldBe Leaf(List(Record(8,8), Record(9,9)))
      leaf3 shouldBe Leaf(List(Record(2,2)))
      leaf4 shouldBe Leaf(List(Record(3,3)))
      internal5 shouldBe Internal(List(2),List(Ref(1,1), Ref(2,3)))
      internal6 shouldBe Internal(List(8),List(Ref(7,11), Ref(8,2)))
      leaf7 shouldBe Leaf(List(Record(4,4)))
      leaf8 shouldBe Leaf(List(Record(5,5)))
      internal9 shouldBe Internal(List(4),List(Ref(3,4), Ref(4,7)))
      leaf10 shouldBe Leaf(List(Record(6,6)))
      leaf11 shouldBe Leaf(List(Record(7,7)))
      internal12 shouldBe Internal(List(6),List(Ref(5,8), Ref(6,10)))
      internal13 shouldBe Internal(List(3),List(Ref(2,5), Ref(4,9)))
      internal14 shouldBe Internal(List(7),List(Ref(6,12), Ref(8,6)))
    }

  }

  private def indexPageId(refId: Long): PageId = {
    PageId(indexFileId, BPlusTreeProtocol.computeOffset(refId))
  }
}

class InMemoryBPlusTreeTest extends FeatureSpec with Matchers {

  feature("b plus tree") {
    scenario("should insert/search on tree with size < degree") {
      val tree =
        InMemoryBPlusTree(degree = 4)
          .insert(Record(9, 9))
          .insert(Record(16, 16))

      println(tree.prettyPrint())
      tree.search(9) shouldBe Some(Record(9, 9))
      tree.search(16) shouldBe Some(Record(16, 16))
      tree.root.node.asInstanceOf[SingleNodeTree].records shouldBe List(Record(9, 9), Record(16, 16))
    }

    scenario("should insert/search on tree with size == degree") {
      val tree =
        InMemoryBPlusTree(degree = 4)
          .insert(Record(9, 9))
          .insert(Record(16, 16))
          .insert(Record(2, 2))
          .insert(Record(7, 7))

      println(tree.prettyPrint())
      tree.root.node.getClass shouldBe classOf[Root]
      tree.search(9) shouldBe Some(Record(9, 9))
      tree.search(16) shouldBe Some(Record(16, 16))
      tree.search(2) shouldBe Some(Record(2, 2))
      tree.search(7) shouldBe Some(Record(7, 7))
      tree.root.node.asInstanceOf[Root].keys shouldBe List(9)
    }

    scenario("should insert/search on tree with size > degree #1") {
      val baseTree =
        InMemoryBPlusTree(degree = 4)
          .insert(Record(9, 9))
          .insert(Record(16, 16))
          .insert(Record(2, 2))
          .insert(Record(7, 7))

      val tree =
        baseTree
          .insert(Record(12, 12))
          .insert(Record(8, 8))

      println(tree.prettyPrint())
      tree.root.node.asInstanceOf[Root].keys shouldBe List(9)
      tree.search(9) shouldBe Some(Record(9, 9))
      tree.search(16) shouldBe Some(Record(16, 16))
      tree.search(2) shouldBe Some(Record(2, 2))
      tree.search(7) shouldBe Some(Record(7, 7))
      tree.search(8) shouldBe Some(Record(8, 8))
    }

    scenario("should insert/search on tree with size > degree #2") {
      val baseTree =
        InMemoryBPlusTree(degree = 4)
          .insert(Record(9, 9))
          .insert(Record(16, 16))
          .insert(Record(2, 2))
          .insert(Record(7, 7))
          .insert(Record(12, 12))

      val tree = baseTree
          .insert(Record(10, 10))

      println(tree.prettyPrint())
      tree.root.node.getClass shouldBe classOf[Root]
      tree.search(9) shouldBe Some(Record(9, 9))
      tree.search(16) shouldBe Some(Record(16, 16))
      tree.search(2) shouldBe Some(Record(2, 2))
      tree.search(7) shouldBe Some(Record(7, 7))
      tree.search(10) shouldBe Some(Record(10, 10))
      tree.root.node.asInstanceOf[Root].keys shouldBe List(9, 12)
    }

    scenario("should insert/search on tree with single level internal nodes") {
      val baseTree = InMemoryBPlusTree(degree = 3)
        .insert(Record(1, 1))
        .insert(Record(2, 2))
        .insert(Record(3, 3))
        .insert(Record(4, 4))

      val tree = baseTree
        .insert(Record(5, 5))

      println(tree.prettyPrint())
      tree.root.node.getClass shouldBe classOf[Root]
      tree.search(1) shouldBe Some(Record(1, 1))
      tree.search(2) shouldBe Some(Record(2, 2))
      tree.search(3) shouldBe Some(Record(3, 3))
      tree.search(4) shouldBe Some(Record(4, 4))
      tree.search(5) shouldBe Some(Record(5, 5))
      tree.root.node.asInstanceOf[Root].keys shouldBe List(3)
    }

    scenario("should insert/search on tree with dual level internal nodes") {
      val baseTree = InMemoryBPlusTree(degree = 3)
        .insert(Record(1, 1))
        .insert(Record(2, 2))
        .insert(Record(3, 3))
        .insert(Record(4, 4))
        .insert(Record(5, 5))
        .insert(Record(6, 6))
        .insert(Record(7, 7))
        .insert(Record(8, 8))

      val tree = baseTree
        .insert(Record(9, 9))

      println(tree.prettyPrint())
      tree.root.node.asInstanceOf[Root].keys shouldBe List(5)
      tree.search(1) shouldBe Some(Record(1, 1))
      tree.search(2) shouldBe Some(Record(2, 2))
      tree.search(3) shouldBe Some(Record(3, 3))
      tree.search(4) shouldBe Some(Record(4, 4))
      tree.search(5) shouldBe Some(Record(5, 5))
      tree.search(6) shouldBe Some(Record(6, 6))
      tree.search(7) shouldBe Some(Record(7, 7))
      tree.search(8) shouldBe Some(Record(8, 8))
      tree.search(9) shouldBe Some(Record(9, 9))
    }

    scenario("should insert/search on tree with dual level internal nodes with even degree") {
      val baseTree = InMemoryBPlusTree(degree = 4)
        .insert(Record(1, 1))
        .insert(Record(2, 2))
        .insert(Record(3, 3))
        .insert(Record(4, 4))
        .insert(Record(5, 5))
        .insert(Record(6, 6))
        .insert(Record(7, 7))
        .insert(Record(8, 8))
        .insert(Record(9, 9))

      val tree = baseTree
        .insert(Record(10, 10))

      println(tree.prettyPrint())
      tree.root.node.asInstanceOf[Root].keys shouldBe List(7)
      tree.search(1) shouldBe Some(Record(1, 1))
      tree.search(2) shouldBe Some(Record(2, 2))
      tree.search(3) shouldBe Some(Record(3, 3))
      tree.search(4) shouldBe Some(Record(4, 4))
      tree.search(5) shouldBe Some(Record(5, 5))
      tree.search(6) shouldBe Some(Record(6, 6))
      tree.search(7) shouldBe Some(Record(7, 7))
      tree.search(8) shouldBe Some(Record(8, 8))
      tree.search(9) shouldBe Some(Record(9, 9))
      tree.search(10) shouldBe Some(Record(10, 10))
    }

  }
}
