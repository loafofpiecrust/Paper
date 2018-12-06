package io.paperdb

import android.util.Log

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.KryoException
import com.esotericsoftware.kryo.Registration
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer

import org.objenesis.strategy.StdInstantiatorStrategy

import java.io.File
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.FileOutputStream
import java.io.IOException
import java.util.ArrayList
import java.util.Arrays
import java.util.HashMap
import java.util.LinkedList
import java.util.UUID

import de.javakaffee.kryoserializers.ArraysAsListSerializer
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer
import de.javakaffee.kryoserializers.UUIDSerializer
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer
import io.paperdb.serializer.NoArgCollectionSerializer

class DbStoragePlainFile internal constructor(
    dbFilesDir: String,
    dbName: String,
    private val mCustomSerializers: List<Registration>,
    private val registrations: HashMap<Class<*>, Int>
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
        @Synchronized get() {
            assertInit()

            val bookFolder = File(rootFolderPath)
            val names = bookFolder.list()
            return if (names != null) {
                for (i in names.indices) {
                    names[i] = names[i].replace(".pt", "")
                }
                Arrays.asList(*names)
            } else {
                ArrayList()
            }
        }

    private fun createKryoInstance(compatibilityMode: Boolean): Kryo {
        val kryo = Kryo()

        if (compatibilityMode) {
            kryo.fieldSerializerConfig.isOptimizedGenerics = true
        }

//        kryo.register(PaperTable::class.java)
        kryo.setDefaultSerializer(CompatibleFieldSerializer::class.java)
        kryo.references = false

        // Serialize Arrays$ArrayList

        kryo.register(Arrays.asList("").javaClass, ArraysAsListSerializer())
        UnmodifiableCollectionsSerializer.registerSerializers(kryo)
        SynchronizedCollectionsSerializer.registerSerializers(kryo)
        // Serialize inner AbstractList$SubAbstractListRandomAccess
        kryo.addDefaultSerializer(ArrayList<Any>().subList(0, 0).javaClass,
            NoArgCollectionSerializer())
        // Serialize AbstractList$SubAbstractList
        kryo.addDefaultSerializer(LinkedList<Any>().subList(0, 0).javaClass,
            NoArgCollectionSerializer())
        // To keep backward compatibility don't change the order of serializers above

        // UUID support
        kryo.register(UUID::class.java, UUIDSerializer())

        for (reg in mCustomSerializers) {
            kryo.register(reg)
        }

        for ((clazz, id) in registrations) {
            kryo.register(clazz, id)
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

    internal suspend fun <E> insert(key: String, value: E) {
        try {
            keyLocker.acquire(key)
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
        } finally {
            keyLocker.release(key)
        }
    }

    internal suspend fun <E> select(key: String): E? {
        try {
            keyLocker.acquire(key)
            assertInit()

            val originalFile = getOriginalFile(key)
            val backupFile = makeBackupFile(originalFile)
            if (backupFile.exists()) {

                originalFile.delete()

                backupFile.renameTo(originalFile)
            }

            return if (!existsInternal(key)) {
                null
            } else readTableFile<E>(key, originalFile)

        } finally {
            keyLocker.release(key)
        }
    }

    internal fun exists(key: String): Boolean {
        try {
            keyLocker.acquire(key)
            return existsInternal(key)
        } finally {
            keyLocker.release(key)
        }
    }

    private fun existsInternal(key: String): Boolean {
        assertInit()

        val originalFile = getOriginalFile(key)
        return originalFile.exists()
    }

    internal fun lastModified(key: String): Long {
        try {
            keyLocker.acquire(key)
            assertInit()

            val originalFile = getOriginalFile(key)
            return if (originalFile.exists()) originalFile.lastModified() else -1
        } finally {
            keyLocker.release(key)
        }
    }

    internal fun deleteIfExists(key: String) {
        try {
            keyLocker.acquire(key)
            assertInit()

            val originalFile = getOriginalFile(key)
            if (!originalFile.exists()) {
                return
            }

            val deleted = originalFile.delete()
            if (!deleted) {
                throw PaperDbException("Couldn't delete file " + originalFile
                    + " for table " + key)
            }
        } finally {
            keyLocker.release(key)
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
    private suspend fun <E> writeTableFile(key: String, paperTable: E,
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

    private suspend fun <E> readTableFile(key: String, originalFile: File): E {
        try {
            return readContent(originalFile)
        } catch (e: FileNotFoundException) {
            var exception: Exception = e
            // Give one more chance, read data in paper 1.x compatibility mode
            if (e is KryoException) {
                try {
                    return readContent(originalFile, createKryoInstance(true))
                } catch (compatibleReadException: FileNotFoundException) {
                    exception = compatibleReadException
                } catch (compatibleReadException: KryoException) {
                    exception = compatibleReadException
                } catch (compatibleReadException: ClassCastException) {
                    exception = compatibleReadException
                }

            }
            val errorMessage = ("Couldn't read/deserialize file "
                + originalFile + " for table " + key)
            throw PaperDbException(errorMessage, exception)
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
            val errorMessage = "Couldn't read/deserialize file $originalFile for table $key"
            throw PaperDbException(errorMessage, exception)
        } catch (e: ClassCastException) {
            var exception: Exception = e
            if (e is KryoException) {
                try {
                    return readContent(originalFile, createKryoInstance(true))
                } catch (compatibleReadException: FileNotFoundException) {
                    exception = compatibleReadException
                } catch (compatibleReadException: KryoException) {
                    exception = compatibleReadException
                } catch (compatibleReadException: ClassCastException) {
                    exception = compatibleReadException
                }
            }
            val errorMessage = "Couldn't read/deserialize file $originalFile for table $key"
            throw PaperDbException(errorMessage, exception)
        }

    }

    @Throws(FileNotFoundException::class, KryoException::class)
    private suspend fun <E> readContent(originalFile: File, kryo: Kryo? = null): E {
        return Input(FileInputStream(originalFile)).use { i ->
            val paperTable = if (kryo != null) {
                kryo.readClassAndObject(i)
            } else {
                this.kryo.use { it.readClassAndObject(i) }
            }

            paperTable as E
        }
    }

    private fun assertInit() {
        if (!paperDirIsCreated) {
            createPaperDir()
            paperDirIsCreated = true
        }
    }

    private fun createPaperDir() {
        if (!File(rootFolderPath).exists()) {
            val isReady = File(rootFolderPath).mkdirs()
            if (!isReady) {
                throw RuntimeException("Couldn't create Paper dir: $rootFolderPath")
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

