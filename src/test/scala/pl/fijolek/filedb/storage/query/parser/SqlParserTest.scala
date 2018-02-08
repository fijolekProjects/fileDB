package pl.fijolek.filedb.storage.query.parser

import org.scalatest.{BeforeAndAfterEach, FeatureSpec, Matchers}
import pl.fijolek.filedb.storage.query.parser.SqlAst._

class SqlParserTest extends FeatureSpec with Matchers with BeforeAndAfterEach {

  feature("query parsing") {
    scenario("create table") {
      val createTableResult = SqlParser.parseCreateTable("CREATE TABLE INSTRUCTOR(ID BigInt, NAME Varchar(2000))")

      createTableResult.name shouldBe "INSTRUCTOR"
      createTableResult.columns shouldBe List(Column("ID", "BigInt"), Column("NAME", "Varchar(2000)"))
    }

    scenario("create index") {
      val createIndexResult = SqlParser.parseCreateIndex("CREATE INDEX INSTRUCTOR_NAME ON INSTRUCTOR(NAME)")

      createIndexResult.name shouldBe "INSTRUCTOR_NAME"
      createIndexResult.table shouldBe "INSTRUCTOR"
      createIndexResult.column shouldBe "NAME"
    }

    scenario("basic insert") {
      val insertResult = SqlParser.parseInsertInto("INSERT INTO INSTRUCTOR (ID, NAME) VALUES (1, 'foo')")

      insertResult.table shouldBe "INSTRUCTOR"
      insertResult.values shouldBe Map("ID" -> SqlNumericalLiteral("1"), "NAME" -> SqlStringLiteral("foo"))
    }

    scenario("basic select") {
      val selectResult = SqlParser.parseSelect("SELECT * FROM FOO")

      selectResult.selectList shouldBe List(SqlIdentifier("*"))
      selectResult.from shouldBe SqlIdentifier("FOO")
      selectResult.where shouldBe None
    }

    scenario("basic select with simple where clause") {
      val selectResult = SqlParser.parseSelect("SELECT * FROM FOO WHERE BAR = 3")

      selectResult.selectList shouldBe List(SqlIdentifier("*"))
      selectResult.from shouldBe SqlIdentifier("FOO")
      selectResult.where shouldBe Some(SqlBinaryOperator(EqualsOperatorValue, SqlIdentifier("BAR"), SqlNumericalLiteral("3")))
    }

  }
}