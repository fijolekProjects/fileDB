package pl.fijolek.filedb.storage

import pl.fijolek.filedb.storage.bplustree.DiskBasedBPlusTree
import pl.fijolek.filedb.storage.bplustree.CollectionImplicits._

import scala.collection.mutable.ArrayBuffer

class RecordsIO(fileIdMapper: FileIdMapper, pageIO: PageIO) {

  def readRecords(storedTableData: StoredTableData): List[Record] = {
    val tableData = storedTableData.data
    val records = new ArrayBuffer[Record]()
    val filePath = fileIdMapper.path(storedTableData.fileId)
    FileUtils.traverse(filePath) { page =>
      val recordsRead = tableData.readRecords(page)
      records ++= recordsRead
    }
    records.toList
  }

  def readPageRecords(storedTableData: StoredTableData, pageId: PageId): List[Record] = {
    val page = pageIO.read(pageId)
    storedTableData.data.readRecords(page)
  }

  def insertRecords(tableData: StoredTableData, records: List[Record]): Unit = {
    val recordSize = tableData.data.recordSize
    val fileId = tableData.fileId
    val indices = tableData.indices
    val recordsToWriteSize = records.length * recordSize
    val lastPage = pageIO.lastPage(fileId).getOrElse(Page.newPage(fileId, 0))
    val pagesToWrite = if (lastPage.spareBytesAtTheEnd > recordsToWriteSize) {
      val pageToWrite = lastPage.add(records.map(_.toBytes))
      val indexRecords = indexRecordsFor(indices, records, pageToWrite)
      List(PageToWrite(pageToWrite, indexRecords))
    } else {
      val recordsThatFitsInLastPageCount = lastPage.spareBytesAtTheEnd / recordSize
      val recordsToWriteInOtherPagesCount = records.length - recordsThatFitsInLastPageCount
      val recordsInSinglePageCount = DbConstants.pageDataSize / recordSize
      val newPagesCount = Math.ceil(recordsToWriteInOtherPagesCount.toDouble / recordsInSinglePageCount.toDouble).toInt
      val recordsForLastPage = records.take(recordsThatFitsInLastPageCount)
      val plainLastPageFull = lastPage.add(recordsForLastPage.map(_.toBytes))
      val indexRecords = indexRecordsFor(indices, recordsForLastPage, plainLastPageFull)
      val lastPageFull = PageToWrite(plainLastPageFull, indexRecords)
      val newPages = (0 until newPagesCount).toList.map { i =>
        val pageOffset = lastPage.offset + (i + 1) * DbConstants.pageSize
        val recordsOffset = i * recordsInSinglePageCount + recordsThatFitsInLastPageCount
        val newPageRecords = records.slice(recordsOffset, recordsOffset + recordsInSinglePageCount)
        val newPageRecordsBytes = newPageRecords.map(_.toBytes)
        val newPage = Page.newPage(fileId, pageOffset)
        val modifiedPage = newPage.add(newPageRecordsBytes)
        val indexRecords = indexRecordsFor(indices, newPageRecords, modifiedPage)
        PageToWrite(modifiedPage, indexRecords)
      }
      lastPageFull :: newPages
    }

    pagesToWrite.foreach { pageToWrite =>
      pageIO.writePage(pageToWrite.page)
    }
    pagesToWrite.flatMap(_.indexRecords).tupleListToMap.mapValuesNow(_.flatten).foreach { case (columnName, indexRecords) =>
      tableData.indices(columnName).insert(indexRecords)
    }
    ()
  }

  def delete(storedTableData: StoredTableData, record: Record): Unit = {
    val data = storedTableData.data
    val filePath = fileIdMapper.path(storedTableData.fileId)
    FileUtils.traverse(filePath) { page =>
      val newPage = data.prepareDelete(record, page)
      pageIO.writePage(newPage)
    }
  }

  private def indexRecordsFor(indices: Map[String, DiskBasedBPlusTree], records: List[Record], page: Page): Map[ColumnName, List[bplustree.Record]] = {
    val indexRecords = records.flatMap { record =>
      record.values.collect { case recordValue if indices.contains(recordValue.column.name) =>
        val columnName = recordValue.column.name
        val indexKey = recordValue.value.asInstanceOf[Long]
        val indexValue = page.offset
        columnName -> bplustree.Record(indexKey, indexValue)
      }
    }
    indexRecords.tupleListToMap
  }

  type ColumnName = String
  case class PageToWrite(page: Page, indexRecords: Map[ColumnName, List[bplustree.Record]])
}