package spark.scd2.utils

import org.apache.hadoop.fs.{FileSystem, Path}

trait FileManager {
  def deletePath(path: Path): Unit
  def renamePaths(srcPath: Path, dstPath: Path): Unit
}

class HdfsFileManager(fs: FileSystem) extends FileManager {
  override def deletePath(path: Path): Unit = {
    // Унифицированная проверка перед удалением
    if (fs.exists(path) && !fs.delete(path, true)) {
      throw new RuntimeException(s"Error deleting $path")
    }
  }

  override def renamePaths(srcPath: Path, dstPath: Path): Unit = {
    if(fs.exists(srcPath) && !fs.rename(srcPath, dstPath)) {
      throw new RuntimeException("Backup creation failed")
    }
  }
}
