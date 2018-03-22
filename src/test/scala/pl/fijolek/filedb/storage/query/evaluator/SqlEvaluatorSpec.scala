package pl.fijolek.filedb.storage.query.evaluator

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, Matchers}
import pl.fijolek.filedb.storage.query.parser.SqlParser
import pl.fijolek.filedb.storage.TestFactory._
import pl.fijolek.filedb.storage.TestData._

class SqlEvaluatorSpec extends FeatureSpec with Matchers with BeforeAndAfterEach with TableDrivenPropertyChecks {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    cleanAllTables(List(extendedInstructorTableData.name))
    systemCatalogManager.init()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    cleanAllTables(List(extendedInstructorTableData.name))
  }

  val createInstructorTable = "CREATE TABLE INSTRUCTOR(ID BigInt, name Varchar(20), subject Varchar(10))"

  feature("sql evaluator") {
    scenario("basic select query") {
      val evaluator = new SqlEvaluator(fileManager)
      evaluator.evaluateCreateTable(SqlParser.parseCreateTable(createInstructorTable))
      evaluator.evaluateInsertInto(SqlParser.parseInsertInto("INSERT INTO INSTRUCTOR (ID, NAME, SUBJECT) VALUES (123, 'abc', 'a')"))

      val recordRead = evaluator.evaluateSelect(SqlParser.parseSelect("SELECT * FROM INSTRUCTOR"))

      recordRead shouldBe List(extendedInstructorRecord(123, "abc", "a"))
    }

    scenario("basic select with projection") {
      val evaluator = new SqlEvaluator(fileManager)
      evaluator.evaluateCreateTable(SqlParser.parseCreateTable(createInstructorTable))
      evaluator.evaluateInsertInto(SqlParser.parseInsertInto("INSERT INTO INSTRUCTOR (ID, NAME, SUBJECT) VALUES (123, 'abc', 'a')"))

      val recordRead = evaluator.evaluateSelect(SqlParser.parseSelect("SELECT NAME FROM INSTRUCTOR"))

      recordRead shouldBe List(nameRecord("abc"))
    }

    scenario("basic select query with where clause") {
      val evaluator = new SqlEvaluator(fileManager)
      evaluator.evaluateCreateTable(SqlParser.parseCreateTable(createInstructorTable))
      evaluator.evaluateInsertInto(SqlParser.parseInsertInto("INSERT INTO INSTRUCTOR (ID, NAME, SUBJECT) VALUES (123, 'abc', 'a')"))
      evaluator.evaluateInsertInto(SqlParser.parseInsertInto("INSERT INTO INSTRUCTOR (ID, NAME, SUBJECT) VALUES (234, 'bcd', 'b')"))
      evaluator.evaluateInsertInto(SqlParser.parseInsertInto("INSERT INTO INSTRUCTOR (ID, NAME, SUBJECT) VALUES (345, 'cde', 'c')"))

      val aRecord = extendedInstructorRecord(123, "abc", "a")
      val bRecord = extendedInstructorRecord(234, "bcd", "b")
      val cRecord = extendedInstructorRecord(345, "cde", "c")
      val table = Table(
        ("query", "result"),
        ("SELECT * FROM INSTRUCTOR WHERE NAME = 'bcd'", List(bRecord)),
        ("SELECT * FROM INSTRUCTOR WHERE NAME = 'bcd' AND ID = 234", List(bRecord)),
        ("SELECT * FROM INSTRUCTOR WHERE NAME = 'bcd' AND ID = 234 AND SUBJECT = 'b'", List(bRecord)),
        ("SELECT * FROM INSTRUCTOR WHERE NAME = 'bcd' AND ID != 234", List()),
        ("SELECT * FROM INSTRUCTOR WHERE SUBJECT = 'b' OR SUBJECT = 'c'", List(bRecord, cRecord)),
        ("SELECT * FROM INSTRUCTOR WHERE (NAME = 'abc' AND SUBJECT = 'a') OR (NAME = 'bcd' AND SUBJECT = 'b')", List(aRecord, bRecord)),
        ("SELECT * FROM INSTRUCTOR WHERE (NAME = 'abc' OR NAME = 'bcd') AND (SUBJECT = 'a' OR SUBJECT = 'b')", List(aRecord, bRecord))
      )

      forAll(table) { (query, result) =>
        val recordsRead = evaluator.evaluateSelect(SqlParser.parseSelect(query))
        recordsRead shouldBe result
      }
    }

  }

}
