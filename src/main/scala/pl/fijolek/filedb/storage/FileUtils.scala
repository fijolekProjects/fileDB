package pl.fijolek.filedb.storage

import java.io.{File, RandomAccessFile}

object FileUtils {

  def traverse(filePath: String)(process: Page => Unit): Unit = {
    withFileOpen(filePath) { file =>
      val buffer = new Array[Byte](DbConstants.pageSize)
      var bytesRead = -1
      while ( {
        bytesRead = file.read(buffer)
        bytesRead != -1
      }) {
        val page = Page.apply(buffer)
        process(page)
      }
    }
  }

  def write(filePath: String, offset: Long, bytes: Array[Byte]): Unit = {
    withFileOpen(filePath) { file =>
      file.seek(offset)
      file.write(bytes)
    }
  }

  def withFileOpen[T](filePath: String)(process: (RandomAccessFile) => T): T = {
    val file = new RandomAccessFile(filePath, "rw")
    try {
      process(file)
    } finally {
      file.close()
    }
  }

  def touchFile(file: File): Unit = {
    file.getParentFile.mkdirs()
    file.createNewFile()
    ()
  }
}
