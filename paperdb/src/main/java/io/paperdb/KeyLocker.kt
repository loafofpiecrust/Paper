package io.paperdb

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.Semaphore

/**
 * This class allows multiple threads to lock against a string key
 *
 *
 * Created by hiperion on 2017/3/15.
 */
internal class KeyLocker {
    private val semaphoreMap = ConcurrentHashMap<String, Semaphore>()

    fun acquire(key: String?) {
        if (key == null) {
            throw IllegalArgumentException("Key couldn't be null")
        }

        if (!semaphoreMap.containsKey(key)) {
            semaphoreMap[key] = Semaphore(1, true)
        }
        val semaphore = semaphoreMap[key]
        semaphore!!.acquireUninterruptibly()
    }

    fun release(key: String?) {
        if (key == null) {
            throw IllegalArgumentException("Key couldn't be null")
        }

        val semaphore = semaphoreMap[key]
            ?: throw IllegalStateException("Couldn't release semaphore. The acquire() with the same key '"
                + key + "' has to be called prior to calling release()")
        semaphore.release()
    }

}
