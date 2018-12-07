package io.paperdb

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.ActorScope
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap

/**
 * This class allows multiple threads to lock against a string key
 *
 *
 * Created by hiperion on 2017/3/15.
 */
internal class KeyLocker {
    private val mutexes = ConcurrentHashMap<String, Mutex>()

    suspend fun <R> withLock(key: String, block: suspend () -> R): R {
        val m = mutexes.getOrPut(key) { Mutex(false) }
        return m.withLock { block() }
    }
}
