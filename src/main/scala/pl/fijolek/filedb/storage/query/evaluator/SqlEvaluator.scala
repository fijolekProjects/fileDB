package pl.fijolek.filedb.storage.query.evaluator

import pl.fijolek.filedb.storage.{FileManager, Record, Value}
import pl.fijolek.filedb.storage.query.parser.SqlAst._

class SqlEvaluator(fileManager: FileManager) {

  def evaluateSelect(sqlSelect: SqlSelect): Stream[Record] = {
    val result = sqlSelect.from match {
      case SqlIdentifier(name) =>
        fileManager.readRecords(name).filter { r =>
          sqlSelect.where match {
            case Some(binOp) =>
              val left = evalNode(binOp.leftOperand, r)
              val right = evalNode(binOp.rightOperand, r)
              binOp.operator match {
                case EqualsOperatorValue =>
                  left == right
                case NotEqualsOperatorValue =>
                  left != right
              }
            case None =>
              true
          }
        }
      case _: SqlNumericalLiteral | _: SqlStringLiteral | _: SqlSelect =>
        ???
    }
    result.map(rec => rec.copy(values = projection(sqlSelect.selectList, rec.values)))
  }

  private def evalNode(node: SqlNode, r: Record): Any = {
    node match {
      case SqlNumericalLiteral(value) =>
        BigDecimal.apply(value)
      case SqlStringLiteral(value) =>
        value
      case SqlIdentifier(columnName) =>
        r.values.find(_.belongsToColumn(columnName)).get.value
      case s: SqlSelect =>
        evaluateSelect(s)
    }
  }

  private def projection(selectList: List[SqlIdentifier], values: List[Value]): List[Value] = {
    selectList.flatMap { identifier =>
      if (identifier.isStar) {
        values
      } else {
        values.filter(v => v.belongsToColumn(identifier.name))
      }
    }
  }

}
