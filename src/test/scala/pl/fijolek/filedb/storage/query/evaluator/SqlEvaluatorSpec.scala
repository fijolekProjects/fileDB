package pl.fijolek.filedb.storage.query.evaluator

import org.scalatest.{BeforeAndAfterEach, FeatureSpec, Matchers}
import pl.fijolek.filedb.storage.query.parser.SqlParser
import pl.fijolek.filedb.storage.TestFactory._
import pl.fijolek.filedb.storage.TestData._

class SqlEvaluatorSpec extends FeatureSpec with Matchers with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    cleanAllTables(List(instructorTableData.name))
    systemCatalogManager.init()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    cleanAllTables(List(instructorTableData.name))
  }

  feature("sql evaluator") {
    scenario("basic select query") {
      val record = instructorRecord(123, "abc")
      val records = List(record)
      systemCatalogManager.createTable(instructorTableData)

      fileManager.insertRecords(instructorTableData.name, records)

      val evaluator = new SqlEvaluator(fileManager)
      val recordRead = evaluator.evaluateSelect(SqlParser.parseSelect("SELECT * FROM INSTRUCTOR"))
      recordRead shouldBe records
    }

    scenario("basic select with projection") {
      val record = instructorRecord(123, "abc")
      val records = List(record)
      systemCatalogManager.createTable(instructorTableData)

      fileManager.insertRecords(instructorTableData.name, records)

      val evaluator = new SqlEvaluator(fileManager)
      val recordRead = evaluator.evaluateSelect(SqlParser.parseSelect("SELECT NAME FROM INSTRUCTOR"))
      recordRead shouldBe List(nameRecord("abc"))
    }

    scenario("basic select query with where clause") {
      val record = instructorRecord(123, "abc")
      val record2 = instructorRecord(234, "bcd")
      val record3 = instructorRecord(345, "cde")
      val records = List(record, record2, record3)
      systemCatalogManager.createTable(instructorTableData)

      fileManager.insertRecords(instructorTableData.name, records)

      val evaluator = new SqlEvaluator(fileManager)
      val recordRead = evaluator.evaluateSelect(SqlParser.parseSelect("SELECT * FROM INSTRUCTOR WHERE NAME = 'bcd'"))
      recordRead shouldBe List(record2)
    }

  }

}
