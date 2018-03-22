package pl.fijolek.filedb.storage.query.parser

import org.antlr.v4.runtime.tree.ParseTreeWalker
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import pl.fijolek.filedb.storage.query.parser.SqlAst._

import scala.collection.JavaConverters._
import scala.collection.mutable

object SqlParser {
  import pl.fijolek.filedb.query.antlr

  def parseCreateTable(query: String): CreateTable = {
    var createTableResult = CreateTable(null, List.empty)
    val createTableContext = createParser(query).create_table()

    ParseTreeWalker.DEFAULT.walk(new antlr.SqlBaseListener() {
      override def enterCreate_table(ctx: antlr.SqlParser.Create_tableContext): Unit = {
        val tableName = ctx.table_name().getText
        val columns = ctx.column_def().asScala.toList.map { columnDef =>
          Column(columnDef.column_name().getText, columnDef.type_name().getText)
        }
        createTableResult = createTableResult.copy(name = tableName, columns = columns)
      }
    }, createTableContext)

    createTableResult
  }

  def parseCreateIndex(query: String): CreateIndex = {
    var createIndexResult = CreateIndex(null, null, null)
    val createTableContext = createParser(query).create_index()

    ParseTreeWalker.DEFAULT.walk(new antlr.SqlBaseListener() {
      override def enterCreate_index(ctx: antlr.SqlParser.Create_indexContext): Unit = {
        val indexName = ctx.index_name().getText
        val tableName = ctx.table_name().getText
        val columnName = ctx.column_name().getText
        createIndexResult = createIndexResult.copy(name = indexName, table = tableName, column = columnName)
      }

    }, createTableContext)

    createIndexResult
  }

  def parseInsertInto(query: String): InsertInto = {
    var insertIntoResult = InsertInto(null, Map.empty)
    val createTableContext = createParser(query).insert()

    ParseTreeWalker.DEFAULT.walk(new antlr.SqlBaseListener() {
      override def enterInsert(ctx: antlr.SqlParser.InsertContext): Unit = {
        val tableName = ctx.table_name().getText
        val values = ctx.column_name().asScala.zip(ctx.literal_value().asScala).toList.map { case (column, value) =>
          column.getText -> SqlLiteral.fromString(value.getText)
        }.toMap
        insertIntoResult = insertIntoResult.copy(table = tableName, values = values)
      }
    }, createTableContext)

    insertIntoResult
  }

  def parseSelect(query: String): SqlSelect = {
    var selectResult: SqlSelect = SqlSelect(null, List.empty, None)
    val selectContext = createParser(query).select()
    val operators = new java.util.Stack[SqlBinaryOperator]()

    var localOperators: java.util.Stack[SqlBinaryOperator] = null
    val joiningExpressions = mutable.Set.empty[String]

    ParseTreeWalker.DEFAULT.walk(new antlr.SqlBaseListener() {

      override def enterTable_name(ctx: antlr.SqlParser.Table_nameContext): Unit = {
        val tableName = SqlIdentifier(ctx.getText)
        selectResult = selectResult.copy(from = tableName)
      }

      override def enterResult_column(ctx: antlr.SqlParser.Result_columnContext): Unit = {
        val colName = SqlIdentifier(ctx.getText)
        selectResult = selectResult.copy(selectList = selectResult.selectList ++ List(colName))
      }

      override def enterExpr(ctx: antlr.SqlParser.ExprContext): Unit = {
        val text = ctx.getText
        val grouped = isGrouped(text)
        val leftGroupCount = text.count(_ == '(')
        val rightGroupCount = text.count(_ == ')')
        if (grouped && leftGroupCount == 1 && rightGroupCount == 1) {
          localOperators = new java.util.Stack[SqlBinaryOperator]()
          ()
        }
        if (grouped && leftGroupCount > 1 && rightGroupCount > 1) {
          joiningExpressions.add(ctx.getText)
          ()
        }
      }

      override def exitExpr(ctx: antlr.SqlParser.ExprContext): Unit = {
        val text = ctx.getText
        val operatorsStack = if (localOperators == null) operators else localOperators
        if (!isGrouped(text) && ctx.children.size() == 3) {
          val operatorValue = SqlOperatorValue.fromString(ctx.children.get(1).getText)
          if (operatorsStack.size() == 2) {
            val right = operatorsStack.pop()
            val left = operatorsStack.pop()
            val operator = SqlBinaryOperator(operatorValue, left, right)
            operatorsStack.push(operator)
            ()
          } else {
            val expr = ctx.expr()
            val (left, right) = (expr.get(0).column_name().getText, expr.get(1).literal_value().getText)
            val operator = SqlBinaryOperator(operatorValue, SqlIdentifier(left), SqlLiteral.fromString(right))
            operatorsStack.push(operator)
            ()
          }
        }

        if (isGrouped(text) && !joiningExpressions.contains(text)) {
          operators.addAll(localOperators)
          localOperators.clear()
        }
        if (joiningExpressions.contains(text) && operators.size() == 2) {
          val operatorValue = SqlOperatorValue.fromString(ctx.children.get(1).getText)
          val right = operators.pop()
          val left = operators.pop()
          val operator = SqlBinaryOperator(operatorValue, left, right)
          operators.push(operator)
          joiningExpressions.remove(text)
          ()
        }
      }

      override def exitSelect(ctx: antlr.SqlParser.SelectContext): Unit = {
        if (!operators.empty()) {
          assert(operators.size() == 1)
          val operator = operators.pop()
          selectResult = selectResult.copy(where = Some(operator))
        }
      }
    }, selectContext)

    selectResult
  }

  private def isGrouped(text: String) = {
    text.startsWith("(") && text.endsWith(")")
  }

  private def createParser(query: String): antlr.SqlParser = {
    new antlr.SqlParser(new CommonTokenStream(new antlr.SqlLexer(CharStreams.fromString(query))))
  }

}