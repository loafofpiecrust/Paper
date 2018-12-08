package io.paperdb

import android.app.Application

import com.esotericsoftware.kryo.Serializer

import java.io.File
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
    internal const val TAG = "paperdb"

    const val DEFAULT_DB_NAME = "io.paperdb"

    private var defaultPath: File? = null

    class Registration(val id: Int, val serializer: Serializer<*>?)

    var serializer: PaperSerializer? = null

    private val books = ConcurrentHashMap<String, Book>()
    private val customSerializers = SerializerMap()
    private val customRegistrations = RegistrationMap()

    /**
     * Lightweight method to init Paper instance. Should be executed in [Application.onCreate]
     * or [android.app.Activity.onCreate]
     *
     * @param defaultPath root path for all books to save under
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
    fun book(): Book = getBook(null, DEFAULT_DB_NAME)

    /**
     * Returns book instance to save data at custom location, e.g. on sdcard.
     *
     * @param location the path to a folder where the book's folder will be placed
     * @param name     the name of the book
     * @return book instance
     */
    @JvmOverloads
    fun bookOn(location: String, name: String = DEFAULT_DB_NAME): Book {
        return getBook(removeLastFileSeparator(location), name)
    }

    /**
     * Returns book instance to save data at custom location, e.g. on sdcard.
     *
     * @param location the path to a folder where the book's folder will be placed
     * @return book instance
     */
    private fun getBook(location: String?, name: String): Book {
        val defaultPath = defaultPath ?: throw PaperDbException("Paper.init is not called")

        val key = (location ?: "") + name
//        return synchronized(books) {
            return books.getOrPut(key) {
                val path = location ?: defaultPath.toString()
                Book(path, name, customSerializers, customRegistrations)
            }
//        }
    }

    private fun removeLastFileSeparator(customLocation: String): String {
        return customLocation.removeSuffix(File.separator)
    }

    /**
     * Sets log level for internal Kryo serializer
     *
     * @param level one of levels from [com.esotericsoftware.minlog.Log]
     */
    fun setLogLevel(level: Int) {
        for ((_, value) in books) {
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
    fun <T> register(clazz: Class<T>, id: Int, serializer: Serializer<T>?) {
        customRegistrations[clazz] = Registration(id, serializer)
    }

    fun <T> addDefaultSerializer(clazz: Class<T>, serializerClass: Class<out Serializer<*>>) {
        customSerializers[clazz] = serializerClass
    }
}

inline fun <reified T> Paper.register(id: Int, serializer: Serializer<T>? = null) {
    register(T::class.java, id, serializer)
}

inline fun <reified T, reified R: Serializer<*>> Paper.addDefaultSerializer() {
    addDefaultSerializer(T::class.java, R::class.java)
}

internal typealias SerializerMap = LinkedHashMap<Class<*>, Class<out Serializer<*>>>
internal typealias RegistrationMap = LinkedHashMap<Class<*>, Paper.Registration>