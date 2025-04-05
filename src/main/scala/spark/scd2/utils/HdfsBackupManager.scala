package spark.scd2.utils

import org.apache.hadoop.fs.{FileSystem, Path}

trait BackupManager {
  def createBackup(backupPath: Path, targetPath: Path, fs: FileSystem): Unit

  def restore(
               backupPath: Path,
               targetPath: Path,
               tempPath: Path,
               fs: FileSystem
             ): Unit
}

class HdfsBackupManager(hdfsFileManager: HdfsFileManager) extends BackupManager {

  override def createBackup(backupPath: Path, targetPath: Path, fs: FileSystem): Unit = {
    if (fs.exists(targetPath)) {
      if (!fs.rename(targetPath, backupPath)) {
        throw new RuntimeException("Backup creation failed")
      }
    }
  }

  override def restore(backupPath: Path, targetPath: Path, tempPath: Path, fs: FileSystem): Unit = {
    if (fs.exists(backupPath)) {
      // Удаление поврежденных данных
      hdfsFileManager.deletePath(targetPath)
      // Восстановление бэкапа
      if (!fs.rename(backupPath, targetPath)) {
        throw new RuntimeException("Critical error: Backup restoration failed!")
      }
    }

    // Очистка временных данных
    hdfsFileManager.deletePath(tempPath)
  }
}
