package io.paperdb

import android.util.Log

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.KryoException
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer
import de.javakaffee.kryoserializers.*

import org.objenesis.strategy.StdInstantiatorStrategy

import java.io.File
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.io.IOException
import java.util.ArrayList
import java.util.Arrays
import java.util.LinkedList
import java.util.UUID

import io.paperdb.serializer.NoArgCollectionSerializer

class DbStoragePlainFile internal constructor(
    dbFilesDir: String,
    dbName: String,
    private val customSerializers: SerializerMap,
    private val registrations: RegistrationMap
) {

    internal val rootFolderPath = dbFilesDir + File.separator + dbName
    @Volatile
    private var paperDirIsCreated: Boolean = false
    private val keyLocker = KeyLocker() // To sync key-dependent operations by key

    private val kryo = SuspendObjectPool(4) {
        createKryoInstance(false)
    }

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
                Arrays.asList(*names)
            } else {
                listOf()
            }
        }

    private fun createKryoInstance(compatibilityMode: Boolean): Kryo {
        val kryo = Kryo()

        if (compatibilityMode) {
            kryo.fieldSerializerConfig.isOptimizedGenerics = true
        }

        kryo.references = false
        kryo.setDefaultSerializer(CompatibleFieldSerializer::class.java)

        // Serialize Arrays$ArrayList

        UnmodifiableCollectionsSerializer.registerSerializers(kryo)
        SynchronizedCollectionsSerializer.registerSerializers(kryo)
        SubListSerializers.addDefaultSerializers(kryo)

        for ((clazz, serializerClass) in customSerializers) {
            kryo.addDefaultSerializer(clazz, serializerClass)
        }

        // To keep backward compatibility don't change the order of serializers below!
        kryo.register(Arrays.asList("").javaClass, ArraysAsListSerializer())

        // UUID support
        kryo.register(UUID::class.java, UUIDSerializer())

        for ((clazz, reg) in registrations) {
            if (reg.serializer != null) {
                kryo.register(clazz, reg.serializer, reg.id)
            } else {
                kryo.register(clazz, reg.id)
            }
        }

        kryo.instantiatorStrategy = Kryo.DefaultInstantiatorStrategy(StdInstantiatorStrategy())

        return kryo
    }


    @Synchronized
    fun destroy() {
        assertInit()

        if (!deleteDirectory(rootFolderPath)) {
            Log.e(Paper.TAG, "Couldn't delete Paper dir $rootFolderPath")
        }
        paperDirIsCreated = false
    }

    internal suspend fun insert(key: String, value: Any?) {
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

            writeTableFile(key, value, originalFile, backupFile)
        }
    }

    internal suspend fun select(key: String): Any? {
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
            } else readTableFile(key, originalFile)
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
    private suspend fun writeTableFile(key: String, paperTable: Any?,
                                   originalFile: File, backupFile: File) {
        try {
            val fileStream = FileOutputStream(originalFile)

            val kryoOutput = Output(fileStream)
            kryo.use { it.writeClassAndObject(kryoOutput, paperTable) }
            kryoOutput.flush()
            fileStream.flush()
            sync(fileStream)
            kryoOutput.close() //also close file stream

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
        } catch (e: KryoException) {
            if (originalFile.exists()) {
                if (!originalFile.delete()) {
                    throw PaperDbException("Couldn't clean up partially-written file $originalFile", e)
                }
            }
            throw PaperDbException("Couldn't save table: $key. Backed up table will be used on next read attempt", e)
        }

    }

    private suspend fun readTableFile(key: String, originalFile: File): Any {
        try {
            if (!originalFile.exists()) {
                throw FileNotFoundException()
            }
            return readContent(originalFile)
        } catch (e: FileNotFoundException) {
            val errorMessage = ("Couldn't read file "
                + originalFile + " for table " + key)
            throw PaperDbException(errorMessage, e)
        } catch (e: KryoException) {
            val exception = try {
                return readContent(originalFile, createKryoInstance(true))
            } catch (compatibleReadException: FileNotFoundException) {
                compatibleReadException
            } catch (compatibleReadException: KryoException) {
                compatibleReadException
            } catch (compatibleReadException: ClassCastException) {
                compatibleReadException
            }
            val errorMessage = "Couldn't deserialize file $originalFile for table $key"
            throw PaperDbException(errorMessage, exception)
        } catch (e: ClassCastException) {
            val errorMessage = "Couldn't read/deserialize file $originalFile for table $key"
            throw PaperDbException(errorMessage, e)
        }

    }

    @Throws(FileNotFoundException::class, KryoException::class)
    private suspend fun readContent(originalFile: File, kryo: Kryo? = null): Any {
        return Input(FileInputStream(originalFile)).use { i ->
            val paperTable = if (kryo != null) {
                kryo.readClassAndObject(i)
            } else {
                this.kryo.use { it.readClassAndObject(i) }
            }

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

