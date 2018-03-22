package pl.fijolek.filedb.storage.query.evaluator

import pl.fijolek.filedb._
import pl.fijolek.filedb.storage.{FileManager, Record, TableData, Value}
import pl.fijolek.filedb.storage.query.parser.SqlAst._

class SqlEvaluator(fileManager: FileManager) {

  def evaluateCreateTable(createTable: CreateTable): Unit = {
    val columns = createTable.columns.map { col =>
      storage.Column(col.name, storage.ColumnTypes.ColumnType.fromString(col.columnType))
    }
    fileManager.createTable(TableData(createTable.name, columns, indices = List.empty))
  }

  def evaluateInsertInto(insertInto: InsertInto): Unit = {
    val tableData = fileManager.readTable(insertInto.table)
    assert(tableData.data.columnsEqual(insertInto.values.keys.toList))
    val values = tableData.data.columnsDefinition.map { columnDef =>
      val value = insertInto.columnValue(columnDef.name) match {
        case SqlStringLiteral(str) => str
        case SqlBigIntLiteral(long) => long
      }
      Value(columnDef, value)
    }
    val record = Record(values)
    fileManager.insertRecords(tableData.data.name, List(record))
  }

  def evaluateSelect(sqlSelect: SqlSelect): Stream[Record] = {
    val result = sqlSelect.from match {
      case SqlIdentifier(name) =>
        fileManager.readRecords(name).filter { r =>
          sqlSelect.where match {
            case Some(binOp) =>
              eval(binOp, r)
            case None =>
              true
          }
        }
    }
    result.map(rec => rec.copy(values = projection(sqlSelect.selectList, rec.values)))
  }

  private def eval(binaryOperator: SqlBinaryOperator, r: Record): Boolean = {
    def evalNode(operand: SqlNode): Any = operand match {
      case SqlBigIntLiteral(value) =>
        BigDecimal.apply(value)
      case SqlStringLiteral(value) =>
        value
      case SqlIdentifier(columnName) =>
        r.values.find(_.belongsToColumn(columnName)).get.value
      case bin: SqlBinaryOperator =>
        val left = evalNode(bin.leftOperand)
        val right = evalNode(bin.rightOperand)
        bin.operator match {
          case EqualsOperatorValue =>
            left == right
          case NotEqualsOperatorValue =>
            left != right
          case AndOperatorValue =>
            left.asInstanceOf[Boolean] && right.asInstanceOf[Boolean]
          case OrOperatorValue =>
            left.asInstanceOf[Boolean] || right.asInstanceOf[Boolean]
        }
      case _: SqlSelect =>
        ???
    }
    evalNode(binaryOperator).asInstanceOf[Boolean]
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
