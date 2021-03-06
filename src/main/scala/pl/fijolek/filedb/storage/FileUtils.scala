package pl.fijolek.filedb.storage

import java.io.{File, RandomAccessFile}

import com.github.ghik.silencer.silent

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

  def read(filePath: String, offset: Long): Array[Byte] = {
    withFileOpen(filePath) { file =>
      val buffer = new Array[Byte](DbConstants.pageSize)
      file.seek(offset)
      @silent val unused = file.read(buffer)
      buffer
    }
  }

  case class ReadPageResult(page: Page, eofReached: Boolean, currentOffset: Long)
  def readExtended(filePath: String, offset: Long): ReadPageResult = {
    withFileOpen(filePath) { file =>
      val buffer = new Array[Byte](DbConstants.pageSize)
      file.seek(offset)
      val bytesRead = file.read(buffer)
      ReadPageResult(Page.apply(buffer), bytesRead == -1, offset + DbConstants.pageSize)
    }
  }

  def write(filePath: String, offset: Long, bytes: Array[Byte]): Unit = {
    withFileOpen(filePath) { file =>
      file.seek(offset)
      file.write(bytes)
    }
  }

  def fileSize(filePath: String): Long = {
    withFileOpen(filePath) { file =>
      file.length()
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
