package pl.fijolek.filedb.storage

import scala.collection.mutable.ArrayBuffer

class RecordsIO {

  def readRecords(tableData: TableData, filePath: String): List[Record] = {
    val records = new ArrayBuffer[Record]()
    FileUtils.traverse(filePath) { page =>
      val recordsRead = tableData.readRecords(page)
      records ++= recordsRead
    }
    records.toList
  }

  def insertRecords(data: TableData, filePath: String, records: List[Record]): Unit = {
    val recordsToWriteSize = records.length * data.recordSize
    val lastPage = Page.lastPage(filePath).getOrElse(Page.newPage(data, filePath, 0))
    val pagesToWrite = if (lastPage.spareBytesAtTheEnd > recordsToWriteSize) {
      List(lastPage.add(records.map(_.toBytes)))
    } else {
      val recordsThatFitsInLastPageCount = lastPage.spareBytesAtTheEnd / data.recordSize
      val recordsToWriteInOtherPagesCount = records.length - recordsThatFitsInLastPageCount
      val recordsInSinglePageCount = (DbConstants.pageSize - DbConstants.pageHeaderSize) / data.recordSize
      val newPagesCount = Math.ceil(recordsToWriteInOtherPagesCount.toDouble / recordsInSinglePageCount.toDouble).toInt
      val lastPageFull = lastPage.add(records.take(recordsThatFitsInLastPageCount).map(_.toBytes))
      val newPages = (0 until newPagesCount).toList.map { i =>
        val pageOffset = lastPage.offset + (i + 1) * DbConstants.pageSize
        val recordsOffset = i * recordsInSinglePageCount + recordsThatFitsInLastPageCount
        val newPageRecords = records.slice(recordsOffset, recordsOffset + recordsInSinglePageCount)
        val newPageRecordsBytes = newPageRecords.map(_.toBytes)
        val newPage = Page.newPage(data, filePath, pageOffset)
        newPage.add(newPageRecordsBytes)
      }
      lastPageFull :: newPages
    }

    pagesToWrite.foreach { page =>
      page.write()
    }
    ()
  }

  def delete(data: TableData, filePath: String, record: Record): Unit = {
    FileUtils.traverse(filePath) { page =>
      val newPage = data.prepareDelete(record, page)
      newPage.write()
    }
  }

}
