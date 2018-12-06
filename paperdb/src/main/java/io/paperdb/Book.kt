package io.paperdb

import com.esotericsoftware.kryo.Registration
import com.esotericsoftware.kryo.Serializer
import kotlinx.coroutines.runBlocking
import java.util.HashMap

class Book internal constructor(
    dbPath: String,
    dbName: String,
    serializers: List<Registration>,
    registrations: HashMap<Class<*>, Int>
) {

    private val mStorage = DbStoragePlainFile(
        dbPath, dbName, serializers, registrations
    )

    /**
     * Returns all keys for objects in book.
     *
     * @return all keys
     */
    val allKeys: List<String>
        get() = mStorage.allKeys

    /**
     * Returns path to a folder containing *.pt files for all keys kept
     * in the current Book. Could be handy for Book export/import purposes.
     * The returned path does not exist if the method has been called prior
     * saving any data in the current Book.
     *
     *
     * See also [.getPath].
     *
     * @return path to a folder locating data files for the current Book
     */
    val path: String
        get() = mStorage.rootFolderPath


    /**
     * Destroys all data saved in Book.
     */
    fun destroy() {
        mStorage.destroy()
    }

    /**
     * Saves any types of POJOs or collections in Book storage.
     *
     * @param key   object key is used as part of object's file name
     * @param value object to save, must have no-arg constructor, can't be null.
     * @param <T>   object type
     * @return this Book instance
    </T> */
    fun <T> write(key: String, value: T?): Book {
        if (value == null) {
            throw PaperDbException("Paper doesn't support writing null root values")
        } else runBlocking {
            mStorage.insert(key, value)
        }
        return this
    }

    /**
     * Instantiates saved object using original object class (e.g. LinkedList). Support limited
     * backward and forward compatibility: removed fields are ignored, new fields have their
     * default values.
     *
     *
     * All instantiated objects must have no-arg constructors.
     *
     * @param key object key to read
     * @return the saved object instance or null
     */
    fun <T> read(key: String): T? {
        return read<T>(key, null)
    }

    /**
     * Instantiates saved object using original object class (e.g. LinkedList). Support limited
     * backward and forward compatibility: removed fields are ignored, new fields have their
     * default values.
     *
     *
     * All instantiated objects must have no-arg constructors.
     *
     * @param key          object key to read
     * @param defaultValue will be returned if key doesn't exist
     * @return the saved object instance or null
     */
    fun <T> read(key: String, defaultValue: T?): T? = runBlocking {
        val value = mStorage.select<T>(key)
        value ?: defaultValue
    }

    /**
     * Checks if an object with the given key is saved in Book storage.
     *
     * @param key object key
     * @return true if Book storage contains an object with given key, false otherwise
     */
    operator fun contains(key: String): Boolean {
        return mStorage.exists(key)
    }

    /**
     * Checks if an object with the given key is saved in Book storage.
     *
     * @param key object key
     * @return true if object with given key exists in Book storage, false otherwise
     */
    @Deprecated("As of release 2.6, replaced by {@link #contains(String)}}")
    fun exist(key: String): Boolean {
        return mStorage.exists(key)
    }

    /**
     * Returns lastModified timestamp of last write in ms.
     * NOTE: only granularity in seconds is guaranteed. Some file systems keep
     * file modification time only in seconds.
     *
     * @param key object key
     * @return timestamp of last write for given key in ms if it exists, otherwise -1
     */
    fun lastModified(key: String): Long {
        return mStorage.lastModified(key)
    }

    /**
     * Delete saved object for given key if it is exist.
     *
     * @param key object key
     */
    fun delete(key: String) {
        mStorage.deleteIfExists(key)
    }

    /**
     * Sets log level for internal Kryo serializer
     *
     * @param level one of levels from [com.esotericsoftware.minlog.Log]
     */
    fun setLogLevel(level: Int) {
        mStorage.setLogLevel(level)
    }

    /**
     * Returns path to a *.pt file containing saved object for a given key.
     * Could be handy for object export/import purposes.
     * The returned path does not exist if the method has been called prior
     * saving data for the given key.
     *
     *
     * See also [.getPath].
     *
     * @param key object key
     * @return path to a *.pt file containing saved object for a given key.
     */
    fun getPath(key: String): String {
        return mStorage.getOriginalFilePath(key)
    }
}