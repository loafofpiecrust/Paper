package io.paperdb

import android.util.Log
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.channels.sendBlocking
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.selects.selectUnbiased
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

private typealias Usage<T, R> = (T) -> R

class SuspendObjectPool<T>(
    private val capacity: Int,
    private val produceInstance: () -> T
) {
    private val instances = mutableMapOf<T, Job>()

    private val inbox = GlobalScope.actor<Usage<T, *>>(
        capacity = Channel.UNLIMITED
    ) {
        for (block in channel) {
            Log.i("suspendpool", "received offering")
            if (instances.isEmpty() || (instances.values.all { it.isActive } && instances.size < capacity)) {
                Log.i("suspendpool", "new instance on ${Thread.currentThread().name}")
                val instance = produceInstance()
                instances[instance] = GlobalScope.launch {
                    block(instance)
                }
            } else {
                Log.i("suspendpool", "selecting on ${Thread.currentThread().name}")
                select {
                    for ((instance, job) in instances) {
                        job.onJoin {
                            Log.i("suspendpool", "selected")
                            instances[instance] = GlobalScope.launch {
                                block(instance)
                            }
                        }
                    }
                }
            }
        }
    }

    suspend fun <R> use(block: Usage<T, R>): R = suspendCoroutine { cont ->
        inbox.offer {
            try {
                cont.resume(block(it))
            } catch (e: Throwable) {
                cont.resumeWithException(e)
            }
        }
    }

    fun <R> useBlocking(block: Usage<T, R>): R = runBlocking { use(block) }
}

