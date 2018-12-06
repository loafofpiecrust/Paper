package io.paperdb

import android.annotation.SuppressLint
import android.app.Application
import android.content.Context
import android.os.Bundle
import com.esotericsoftware.kryo.Registration

import com.esotericsoftware.kryo.Serializer

import java.io.File
import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap

/**
 * Fast NoSQL data storage with auto-upgrade support to save any types of Plain Old Java Objects or
 * collections using Kryo serialization.
 *
 *
 * Every custom class must have no-arg constructor. Common classes supported out of the box.
 *
 *
 * Auto upgrade works in a way that removed object's fields are ignored on read and new fields
 * have their default values on create class instance.
 *
 *
 * Each object is saved in separate Paper file with name like object_key.pt.
 * All Paper files are created in the /files/io.paperdb dir in app's private storage.
 */
object Paper {
    internal val TAG = "paperdb"

    const val DEFAULT_DB_NAME = "io.paperdb"

    // Keep _application_ context
//    @SuppressLint("StaticFieldLeak")
//    private var mContext: Context? = null
    private var defaultPath: File? = null

    private val mBookMap = ConcurrentHashMap<String, Book>()
    private val mCustomSerializers = mutableListOf<Registration>()
    private val customRegistrations = HashMap<Class<*>, Int>()

    /**
     * Lightweight method to init Paper instance. Should be executed in [Application.onCreate]
     * or [android.app.Activity.onCreate].
     *
     *
     *
     * @param context context, used to get application context
     */
    fun init(defaultPath: File) {
        this.defaultPath = defaultPath
    }

    /**
     * Returns book instance with the given name
     *
     * @param name name of new database
     * @return Paper instance
     */
    fun book(name: String): Book {
        if (name == DEFAULT_DB_NAME)
            throw PaperDbException("$DEFAULT_DB_NAME name is reserved for default library name")
        return getBook(null, name)
    }

    /**
     * Returns default book instance
     *
     * @return Book instance
     */
    fun book(): Book {
        return getBook(null, DEFAULT_DB_NAME)
    }

    /**
     * Returns book instance to save data at custom location, e.g. on sdcard.
     *
     * @param location the path to a folder where the book's folder will be placed
     * @param name     the name of the book
     * @return book instance
     */
    @JvmOverloads
    fun bookOn(location: String, name: String = DEFAULT_DB_NAME): Book {
        var location = location
        location = removeLastFileSeparatorIfExists(location)
        return getBook(location, name)
    }

    private fun getBook(location: String?, name: String): Book {
        if (defaultPath == null) {
            throw PaperDbException("Paper.init is not called")
        }
        val key = (location ?: "") + name
        synchronized(mBookMap) {
            var book = mBookMap[key]
            if (book == null) {
                val path = location ?: defaultPath!!.toString()
                book = Book(path, name, mCustomSerializers, customRegistrations)
                mBookMap[key] = book
            }
            return book
        }
    }

    private fun removeLastFileSeparatorIfExists(customLocation: String): String {
        return if (customLocation.endsWith(File.separatorChar)) {
            customLocation.substring(0, customLocation.length - 1)
        } else customLocation
    }

    /**
     * Sets log level for internal Kryo serializer
     *
     * @param level one of levels from [com.esotericsoftware.minlog.Log]
     */
    fun setLogLevel(level: Int) {
        for ((_, value) in mBookMap) {
            value.setLogLevel(level)
        }
    }

    /**
     * Adds a custom serializer for a specific class
     * When used, must be called right after Paper.init()
     *
     * @param clazz      type of the custom serializer
     * @param serializer the serializer instance
     * @param <T>        type of the serializer
    </T> */
    fun <T> register(clazz: Class<T>, serializer: Serializer<T>, id: Int) {
        mCustomSerializers.add(Registration(clazz, serializer, id))
    }

    fun <T> register(clazz: Class<T>, id: Int) {
        customRegistrations[clazz] = id
    }

    inline fun <reified T> register(id: Int, serializer: Serializer<T>? = null) {
        if (serializer != null) {
            register(T::class.java, serializer, id)
        } else {
            register(T::class.java, id)
        }
    }
}
/**
 * Returns book instance to save data at custom location, e.g. on sdcard.
 *
 * @param location the path to a folder where the book's folder will be placed
 * @return book instance
 */
