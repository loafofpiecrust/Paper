package io.paperdb

import android.util.Log
import kotlinx.io.IOException

import java.io.File
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.FileOutputStream

import java.lang.reflect.Type

class DbStoragePlainFile internal constructor(
    dbFilesDir: String,
    dbName: String,
    private val serializer: PaperSerializer
) {
    internal val rootFolderPath = dbFilesDir + File.separator + dbName
    @Volatile
    private var paperDirIsCreated: Boolean = false
    private val keyLocker = KeyLocker() // To sync key-dependent operations by key

    // remove extensions
    internal val allKeys: List<String>
        get() {
            assertInit()

            val bookFolder = File(rootFolderPath)
            val names = bookFolder.list()
            return if (names != null) {
                for (i in names.indices) {
                    names[i] = names[i].replace(".pt", "")
                }
                names.asList()
            } else {
                listOf()
            }
        }


    @Synchronized
    fun destroy() {
        assertInit()

        if (!deleteDirectory(rootFolderPath)) {
            Log.e(Paper.TAG, "Couldn't delete Paper dir $rootFolderPath")
        }
        paperDirIsCreated = false
    }

    internal suspend fun insert(key: String, value: Any?, type: Type) {
        keyLocker.withLock(key) {
            assertInit()

            val originalFile = getOriginalFile(key)
            val backupFile = makeBackupFile(originalFile)
            // Rename the current file so it may be used as a backup during the next read
            if (originalFile.exists()) {
                //Rename original to backup
                if (!backupFile.exists()) {
                    if (!originalFile.renameTo(backupFile)) {
                        throw PaperDbException("Couldn't rename file " + originalFile
                            + " to backup file " + backupFile)
                    }
                } else {
                    //Backup exist -> original file is broken and must be deleted

                    originalFile.delete()
                }
            }

            writeTableFile(key, value, type, originalFile, backupFile)
        }
    }

    internal suspend fun select(key: String, type: Type): Any? {
        return keyLocker.withLock(key) {
            assertInit()

            val originalFile = getOriginalFile(key)
            val backupFile = makeBackupFile(originalFile)
            if (backupFile.exists()) {

                originalFile.delete()

                backupFile.renameTo(originalFile)
            }

            if (!existsInternal(key)) {
                null
            } else readTableFile(key, type, originalFile)
        }
    }

    internal suspend fun exists(key: String): Boolean {
        return keyLocker.withLock(key) {
            existsInternal(key)
        }
    }

    private fun existsInternal(key: String): Boolean {
        assertInit()

        val originalFile = getOriginalFile(key)
        return originalFile.exists()
    }

    internal suspend fun lastModified(key: String): Long {
        return keyLocker.withLock(key) {
            assertInit()

            val originalFile = getOriginalFile(key)
            if (originalFile.exists()) originalFile.lastModified() else -1
        }
    }

    internal suspend fun deleteIfExists(key: String) {
        keyLocker.withLock(key) {
            assertInit()

            val originalFile = getOriginalFile(key)
            if (originalFile.exists()) {
                val deleted = originalFile.delete()
                if (!deleted) {
                    throw PaperDbException("Couldn't delete file " + originalFile
                        + " for table " + key)
                }
            }
        }
    }

    internal fun setLogLevel(level: Int) {
        com.esotericsoftware.minlog.Log.set(level)
    }

    internal fun getOriginalFilePath(key: String): String {
        return rootFolderPath + File.separator + key + ".pt"
    }

    private fun getOriginalFile(key: String): File {
        val tablePath = getOriginalFilePath(key)
        return File(tablePath)
    }

    /**
     * Attempt to write the file, delete the backup and return true as atomically as
     * possible.  If any exception occurs, delete the new file; next time we will restore
     * from the backup.
     *
     * @param key          table key
     * @param paperTable   table instance
     * @param originalFile file to write new data
     * @param backupFile   backup file to be used if write is failed
     */
    private suspend fun writeTableFile(key: String, paperTable: Any?, type: Type,
                                   originalFile: File, backupFile: File) {
        try {
            val fileStream = FileOutputStream(originalFile)
            fileStream.write(serializer.serialize(paperTable, type))
            fileStream.flush()
            sync(fileStream)
            fileStream.close()

            // Writing was successful, delete the backup file if there is one.

            backupFile.delete()
        } catch (e: IOException) {
            // Clean up an unsuccessfully written file
            if (originalFile.exists()) {
                if (!originalFile.delete()) {
                    throw PaperDbException("Couldn't clean up partially-written file $originalFile", e)
                }
            }
            throw PaperDbException("Couldn't save table: " + key + ". " +
                "Backed up table will be used on next read attempt", e)
        }
    }

    private suspend fun readTableFile(key: String, type: Type, originalFile: File): Any {
        if (!originalFile.exists()) {
            val errorMessage = ("Couldn't read file "
                + originalFile + " for table " + key)
            throw PaperDbException(errorMessage)
        }
        return readContent(originalFile, type)
    }

    @Throws(FileNotFoundException::class)
    private suspend fun readContent(originalFile: File, type: Type): Any {
        return FileInputStream(originalFile).use { i ->
            val paperTable = serializer.deserialize<Any>(i.readBytes(), type)
            paperTable
        }
    }

    private fun assertInit() {
        if (!paperDirIsCreated) {
            createPaperDir()
            paperDirIsCreated = true
        }
    }

    private fun createPaperDir() {
        val rootFile = File(rootFolderPath)
        if (!rootFile.exists()) {
            synchronized(Paper) {
                val isReady = rootFile.mkdirs()
                if (!isReady && !rootFile.exists()) {
                    throw RuntimeException("Couldn't create Paper dir: $rootFolderPath")
                }
            }
        }
    }

    private fun deleteDirectory(dirPath: String): Boolean {
        val directory = File(dirPath)
        if (directory.exists()) {
            val files = directory.listFiles()
            if (null != files) {
                for (file in files) {
                    if (file.isDirectory) {
                        deleteDirectory(file.toString())
                    } else {
                        file.delete()
                    }
                }
            }
        }
        return directory.delete()
    }

    private fun makeBackupFile(originalFile: File): File {
        return File(originalFile.path + ".bak")
    }

    /**
     * Perform an fsync on the given FileOutputStream.  The stream at this
     * point must be flushed but not yet closed.
     */
    private fun sync(stream: FileOutputStream?) {
        try {
            stream?.fd?.sync()
        } catch (e: IOException) {
        }

    }
}

