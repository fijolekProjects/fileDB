package pl.fijolek.filedb.storage.query.parser

import org.antlr.v4.runtime.tree.ParseTreeWalker
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import pl.fijolek.filedb.storage.query.parser.SqlAst._

import scala.collection.JavaConverters._

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
    val localOperators = new java.util.Stack[SqlBinaryOperator]()
    var shouldUseLocalOperator = false

    ParseTreeWalker.DEFAULT.walk(new antlr.SqlBaseListener() {

      override def enterTable_name(ctx: antlr.SqlParser.Table_nameContext): Unit = {
        val tableName = SqlIdentifier(ctx.getText)
        selectResult = selectResult.copy(from = tableName)
      }

      override def enterResult_column(ctx: antlr.SqlParser.Result_columnContext): Unit = {
        val colName = SqlIdentifier(ctx.getText)
        selectResult = selectResult.copy(selectList = selectResult.selectList ++ List(colName))
      }

      override def enterExpr_group(ctx: antlr.SqlParser.Expr_groupContext): Unit = {
        shouldUseLocalOperator = true
      }

      override def exitExpr_group(ctx: antlr.SqlParser.Expr_groupContext): Unit = {
        val op = localOperators.pop()
        operators.push(op)
        shouldUseLocalOperator = false
      }

      override def exitExpr_comparison(ctx: antlr.SqlParser.Expr_comparisonContext): Unit = {
        val expr = ctx.expr()
        val operatorValue = SqlOperatorValue.fromString(ctx.children.get(1).getText)
        val (left, right) = (expr.get(0).getText, expr.get(1).getText)  //TODO handle column on the right hand side
        val operator = SqlBinaryOperator(operatorValue, SqlIdentifier(left), SqlLiteral.fromString(right))
        currentOperators.push(operator)
        ()
      }

      override def exitExpr_and(ctx: antlr.SqlParser.Expr_andContext): Unit = {
        currentOperators.push(margeLastTwoOperators(AndOperatorValue))
        ()
      }

      override def exitExpr_or(ctx: antlr.SqlParser.Expr_orContext): Unit = {
        currentOperators.push(margeLastTwoOperators(OrOperatorValue))
        ()
      }

      private def margeLastTwoOperators(operatorValue: SqlOperatorValue): SqlBinaryOperator = {
        val right = currentOperators.pop()
        val left = currentOperators.pop()
        SqlBinaryOperator(operatorValue, left, right)
      }

      override def exitSelect(ctx: antlr.SqlParser.SelectContext): Unit = {
        if (!operators.empty()) {
          assert(operators.size() == 1 && localOperators.empty())
          val operator = operators.pop()
          selectResult = selectResult.copy(where = Some(operator))
        }
      }

      private def currentOperators: java.util.Stack[SqlBinaryOperator] = {
        if (shouldUseLocalOperator) localOperators
        else operators
      }

    }, selectContext)

    selectResult
  }

  private def createParser(query: String): antlr.SqlParser = {
    new antlr.SqlParser(new CommonTokenStream(new antlr.SqlLexer(CharStreams.fromString(query))))
  }

}