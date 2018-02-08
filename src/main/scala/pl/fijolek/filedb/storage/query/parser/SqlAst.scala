package pl.fijolek.filedb.storage.query.parser

import scala.util.Try

object SqlAst {
  sealed trait SqlOperator
  case class SqlBinaryOperator(operator: SqlOperatorValue, leftOperand: SqlNode, rightOperand: SqlNode) extends SqlOperator

  sealed trait SqlNode
  case class SqlSelect(from: SqlIdentifier, selectList: List[SqlIdentifier], where: Option[SqlBinaryOperator]) extends SqlNode
  case class SqlIdentifier(name: String) extends SqlNode

  sealed trait SqlLiteral extends SqlNode
  case class SqlNumericalLiteral(value: String) extends SqlLiteral
  case class SqlStringLiteral(value: String) extends SqlLiteral
  object SqlLiteral {
    def fromString(s: String): SqlLiteral = {
      if (Try(BigDecimal.apply(s)).isSuccess) {
        SqlNumericalLiteral(s)
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
  case class CreateTable(name: String, columns: List[Column])
  case class CreateIndex(name: String, table: String, column: String)
  case class InsertInto(table: String, values: Map[String, SqlLiteral])
}