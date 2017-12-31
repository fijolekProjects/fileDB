package pl.fijolek.filedb.storage

class PageIO(val fileIdMapper: FileIdMapper) {

  def writePage(page: Page): Unit = {
    val filePath = fileIdMapper.path(page.pageId.fileId)
    val toWrite = page.bytes
    FileUtils.write(filePath, page.pageId.offset, toWrite)
  }

  def read(pageId: PageId): Page = {
    val filePath = fileIdMapper.path(pageId.fileId)
    val pageBytes = FileUtils.read(filePath, pageId.offset)
    Page(pageBytes)
  }

  def lastPage(fileId: Long): Option[Page] = {
    val filePath = fileIdMapper.path(fileId)
    FileUtils.withFileOpen(filePath) { file =>
      val fileSize = file.length()
      if (fileSize == 0) {
        None
      } else {
        val pageOffset = (fileSize / DbConstants.pageSize) * DbConstants.pageSize
        file.seek(pageOffset)
        val pageBytes = new Array[Byte](DbConstants.pageSize)
        file.read(pageBytes)
        Some(Page(pageBytes))
      }
    }
  }

}
