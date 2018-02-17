package pl.fijolek.filedb.storage.query.parser

import scala.util.Try

object SqlAst {
  sealed trait SqlOperator
  case class SqlBinaryOperator(operator: SqlOperatorValue, leftOperand: SqlNode, rightOperand: SqlNode) extends SqlOperator

  sealed trait SqlNode
  case class SqlSelect(from: SqlNode, selectList: List[SqlIdentifier], where: Option[SqlBinaryOperator]) extends SqlNode
  case class SqlIdentifier(name: String) extends SqlNode {
    def isStar = name == "*"
  }

  sealed trait SqlLiteral extends SqlNode
  case class SqlBigIntLiteral(value: Long) extends SqlLiteral {}
  case class SqlStringLiteral(value: String) extends SqlLiteral
  object SqlLiteral {
    def fromString(s: String): SqlLiteral = {
      if (Try(s.toLong).isSuccess) {
        SqlBigIntLiteral(s.toLong)
      } else {
        SqlStringLiteral(s.replaceAll("'", ""))
      }
    }
  }

  sealed trait SqlOperatorValue
  case object EqualsOperatorValue extends SqlOperatorValue
  case object NotEqualsOperatorValue extends SqlOperatorValue
  object SqlOperatorValue {
    def fromString(s: String): SqlOperatorValue = {
      s match {
        case "=" => EqualsOperatorValue
        case "!=" => NotEqualsOperatorValue
      }
    }
  }

  case class Column(name: String, columnType: String)
  sealed trait CreateSql
  case class CreateTable(name: String, columns: List[Column]) extends CreateSql
  case class CreateIndex(name: String, table: String, column: String) extends CreateSql

  case class InsertInto(table: String, values: Map[String, SqlLiteral]) {
    def columnValue(column: String): SqlLiteral = {
      values.getOrElse(column.toUpperCase, values(column.toLowerCase))
    }
  }
}