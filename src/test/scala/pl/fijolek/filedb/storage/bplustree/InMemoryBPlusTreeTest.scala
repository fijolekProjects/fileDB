package pl.fijolek.filedb.storage.bplustree

import org.scalatest.{FeatureSpec, Matchers}

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


  }
}
