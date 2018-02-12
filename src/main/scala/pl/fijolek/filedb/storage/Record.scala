package pl.fijolek.filedb.storage

import java.math.BigInteger
import java.nio.ByteBuffer

import pl.fijolek.filedb.storage.ColumnTypes.ColumnType

case class Record(values: List[Value]) {

  def toBytes: Array[Byte] = {
    Record.toBytes(values)
  }

}

object Record {

  def fromBytes(recordBytes: Array[Byte], columnsDefinition: List[Column]): Option[Record] = {
    if (recordBytes.forall(_ == 0)) {
      None
    } else {
      val (recordFields, _) = columnsDefinition.foldLeft((List.empty[Value], 0)) { case ((values, offset), colDef) =>
        val columnBytes = java.util.Arrays.copyOfRange(recordBytes, offset, offset + colDef.typ.sizeInBytes)
        val value: Any = colDef.typ match {
          case _: ColumnTypes.Varchar =>
            new String(columnBytes.takeWhile(_ != 0))
          case _: ColumnTypes.BigInt.type =>
            ByteBuffer.wrap(columnBytes).getLong
          case numType: ColumnTypes.Numeric =>
            val unscaledValue = BigDecimal(new BigInteger(columnBytes))
            unscaledValue / BigDecimal(10).pow(numType.scale)
        }
        (values :+ Value(colDef, value), offset + colDef.typ.sizeInBytes)
      }
      Some(Record(recordFields))
    }
  }

  def toBytes(values: List[Value]): Array[Byte] = {
    val bytes = values.toArray.flatMap { value =>
      val sizeInBytes = value.column.typ.sizeInBytes
      value.column.typ match {
        case _: ColumnTypes.Varchar =>
          val stringBytes = value.value.asInstanceOf[String].getBytes
          java.util.Arrays.copyOf(stringBytes, sizeInBytes)
        case _: ColumnTypes.BigInt.type =>
          ByteBuffer.allocate(sizeInBytes).putLong(value.value.asInstanceOf[Long]).array()
        case _: ColumnTypes.Numeric =>
          //          val bytes = value.value.asInstanceOf[BigDecimal].setScale(numType.scale).underlying().unscaledValue().toByteArray
          //          watch out for little/big endian - BigInteger assumes big-endian
          //          System.arraycopy(bytes, 0, bytesToWrite, sizeInBytes - bytes.length, bytes.length)
          //          bytesToWrite
          ???
      }
    }
    bytes
  }

}


case class Column(name: String, typ: ColumnType)
object ColumnTypes {
  object ColumnType {
    def numberWithPrecisionByteSize(precision: Int): Int = {
      BigDecimal((1 to precision).map(_ => "9").mkString).underlying().unscaledValue().toByteArray.length
    }

    def fromString(columnTypeString: String): ColumnType = {
      if (columnTypeString == "BigInt") {
        ColumnTypes.BigInt
      } else if (columnTypeString.contains("Varchar")) {
        val varcharLength = "Varchar\\((.*)\\)".r.findAllMatchIn(columnTypeString).toList(0).group(1).toInt
        Varchar(varcharLength)
      } else {
        throw new UnsupportedOperationException(s"Unknown column type: $columnTypeString")
      }
    }
  }
  abstract sealed class ColumnType(val sizeInBytes: Int) {
    type baseType
  }
  case class Numeric(precision: Int, scale: Int) extends ColumnType(ColumnType.numberWithPrecisionByteSize(precision)) {
    override type baseType = BigDecimal
  }
  case object BigInt extends ColumnType(java.lang.Long.BYTES) {
    override type baseType = Long
  }
  case class Varchar(length: Int) extends ColumnType(length) {
    override type baseType = String
  }
}




//TODO make column type and value type equal
case class Value(column: Column, value: Any) {
  def belongsToColumn(columnName: String): Boolean = columnName.toUpperCase == column.name.toUpperCase
}