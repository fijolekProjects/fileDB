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

  val createInstructorTable = "CREATE TABLE INSTRUCTOR(ID BigInt, name Varchar(20))"

  feature("sql evaluator") {
    scenario("basic select query") {
      val evaluator = new SqlEvaluator(fileManager)
      evaluator.evaluateCreateTable(SqlParser.parseCreateTable(createInstructorTable))
      evaluator.evaluateInsertInto(SqlParser.parseInsertInto("INSERT INTO INSTRUCTOR (ID, NAME) VALUES (123, 'abc')"))

      val recordRead = evaluator.evaluateSelect(SqlParser.parseSelect("SELECT * FROM INSTRUCTOR"))

      recordRead shouldBe List(instructorRecord(123, "abc"))
    }

    scenario("basic select with projection") {
      val evaluator = new SqlEvaluator(fileManager)
      evaluator.evaluateCreateTable(SqlParser.parseCreateTable(createInstructorTable))
      evaluator.evaluateInsertInto(SqlParser.parseInsertInto("INSERT INTO INSTRUCTOR (ID, NAME) VALUES (123, 'abc')"))

      val recordRead = evaluator.evaluateSelect(SqlParser.parseSelect("SELECT NAME FROM INSTRUCTOR"))

      recordRead shouldBe List(nameRecord("abc"))
    }

    scenario("basic select query with where clause") {
      val evaluator = new SqlEvaluator(fileManager)
      evaluator.evaluateCreateTable(SqlParser.parseCreateTable(createInstructorTable))
      evaluator.evaluateInsertInto(SqlParser.parseInsertInto("INSERT INTO INSTRUCTOR (ID, NAME) VALUES (123, 'abc')"))
      evaluator.evaluateInsertInto(SqlParser.parseInsertInto("INSERT INTO INSTRUCTOR (ID, NAME) VALUES (234, 'bcd')"))
      evaluator.evaluateInsertInto(SqlParser.parseInsertInto("INSERT INTO INSTRUCTOR (ID, NAME) VALUES (345, 'cde')"))

      val recordRead = evaluator.evaluateSelect(SqlParser.parseSelect("SELECT * FROM INSTRUCTOR WHERE NAME = 'bcd'"))

      recordRead shouldBe List(instructorRecord(234, "bcd"))
    }

  }

}
