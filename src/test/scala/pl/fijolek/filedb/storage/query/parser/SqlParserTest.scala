package pl.fijolek.filedb.storage.query.parser

import org.scalatest.{BeforeAndAfterEach, FeatureSpec, Matchers}
import pl.fijolek.filedb.storage.query.parser.SqlAst._

class SqlParserTest extends FeatureSpec with Matchers with BeforeAndAfterEach {

  import WhereTreeBuilder._

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
      insertResult.values shouldBe Map("ID" -> SqlBigIntLiteral(1), "NAME" -> SqlStringLiteral("foo"))
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
      selectResult.where shouldBe Some(
        EQ("BAR", 3)
      )
    }

    scenario("select with where AND clause #1") {
      val selectResult = SqlParser.parseSelect("SELECT * FROM FOO WHERE BAR = 3 AND ID = 1")

      selectResult.selectList shouldBe List(SqlIdentifier("*"))
      selectResult.from shouldBe SqlIdentifier("FOO")

      selectResult.where shouldBe Some(
        AND(EQ("BAR", 3), EQ("ID", 1))
      )
    }

    scenario("select with where AND clause #2") {
      val selectResult = SqlParser.parseSelect("SELECT * FROM FOO WHERE BAR = 3 AND ID = 1 AND BAZ = 4")

      selectResult.selectList shouldBe List(SqlIdentifier("*"))
      selectResult.from shouldBe SqlIdentifier("FOO")
      selectResult.where shouldBe Some(
        AND(
          AND(EQ("BAR", 3), EQ("ID", 1)),
          EQ("BAZ", 4)
        )
      )
    }

    scenario("select with where AND clause #3") {
      val selectResult = SqlParser.parseSelect("SELECT * FROM FOO WHERE BAR = 3 AND ID = 1 AND BAZ = 4 AND QUX = 5")

      selectResult.selectList shouldBe List(SqlIdentifier("*"))
      selectResult.from shouldBe SqlIdentifier("FOO")

      selectResult.where shouldBe Some(
        AND(
          AND(
            AND(EQ("BAR", 3), EQ("ID", 1)),
            EQ("BAZ", 4)
          ),
          EQ("QUX", 5)
        )
      )
    }

    scenario("select with where AND clause #3'") {
      val selectResult = SqlParser.parseSelect("SELECT * FROM FOO WHERE (BAR = 3 AND ID = 1) AND (BAZ = 4 AND QUX = 5)")

      selectResult.selectList shouldBe List(SqlIdentifier("*"))
      selectResult.from shouldBe SqlIdentifier("FOO")

      selectResult.where shouldBe Some(
        AND(
          AND(EQ("BAR", 3), EQ("ID", 1)),
          AND(EQ("BAZ", 4), EQ("QUX", 5))
        )
      )
    }

    scenario("select with AND and OR within where clause #1") {
      val selectResult = SqlParser.parseSelect("SELECT * FROM FOO WHERE (BAR = 3 AND ID = 1) OR (BAZ = 4 AND QUX = 5)")

      selectResult.selectList shouldBe List(SqlIdentifier("*"))
      selectResult.from shouldBe SqlIdentifier("FOO")

      selectResult.where shouldBe Some(
        OR(
          AND(EQ("BAR", 3), EQ("ID", 1)),
          AND(EQ("BAZ", 4), EQ("QUX", 5))
        )
      )
    }

    scenario("select with AND and OR within where clause #2") {
      val selectResult = SqlParser.parseSelect("SELECT * FROM FOO WHERE (BAR = 3 AND ID = 1 AND FOO = 6) OR (BAZ = 4 AND QUX = 5)")

      selectResult.selectList shouldBe List(SqlIdentifier("*"))
      selectResult.from shouldBe SqlIdentifier("FOO")

      selectResult.where shouldBe Some(
        OR(
          AND(AND(EQ("BAR", 3), EQ("ID", 1)), EQ("FOO", 6)),
          AND(EQ("BAZ", 4), EQ("QUX", 5))
        )
      )
    }
  }

  object WhereTreeBuilder {
    def EQ(id: String, literal: Any): SqlBinaryOperator = {
      SqlBinaryOperator(EqualsOperatorValue, SqlIdentifier(id), SqlLiteral.fromString(literal.toString))
    }

    def AND(left: SqlBinaryOperator, right: SqlBinaryOperator): SqlBinaryOperator = {
      SqlBinaryOperator(AndOperatorValue, left, right)
    }

    def OR(left: SqlBinaryOperator, right: SqlBinaryOperator): SqlBinaryOperator = {
      SqlBinaryOperator(OrOperatorValue, left, right)
    }


  }
}