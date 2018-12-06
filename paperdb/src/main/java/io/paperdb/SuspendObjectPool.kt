package io.paperdb

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.selects.select
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

private typealias Usage<T, R> = (T) -> R

class SuspendObjectPool<T>(
    private val capacity: Int,
    private val produceInstance: () -> T
) {
    private val instances = mutableMapOf<T, Job>()

    private val inbox = GlobalScope.actor<Usage<T, *>> {
        for (block in channel) {
            if (instances.isEmpty() || (instances.values.all { it.isActive } && instances.size < capacity)) {
                val instance = produceInstance()
                instances[instance] = launch {
                    block(instance)
                }
            } else select {
                for ((instance, job) in instances) {
                    job.onJoin {
                        instances[instance] = launch {
                            block(instance)
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

