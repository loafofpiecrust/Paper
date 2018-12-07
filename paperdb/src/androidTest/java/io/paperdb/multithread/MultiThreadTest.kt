package io.paperdb.multithread

import android.support.test.runner.AndroidJUnit4
import android.util.Log

import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

import java.util.Collections
import java.util.LinkedList
import java.util.Random
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

import io.paperdb.Paper
import io.paperdb.testdata.Person
import io.paperdb.testdata.TestDataGenerator

import android.support.test.InstrumentationRegistry.getTargetContext
import junit.framework.TestCase.assertFalse
import kotlinx.coroutines.*
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue

/**
 * Tests read/write into Paper data from multiple threads
 */
@RunWith(AndroidJUnit4::class)
class MultiThreadTest {

    private val insertRunnable: Runnable
        get() = Runnable {
            val size = Random().nextInt(200)
            val inserted100 = TestDataGenerator.genPersonList(size)
            runBlocking { Paper.book().write<Any>("persons", inserted100) }
        }

    private val selectRunnable: Runnable
        get() = Runnable {
            runBlocking { Paper.book().read<Any>("persons") }
        }

    @Before
    fun setUp() {
        Paper.init(getTargetContext().filesDir)
        Paper.book().destroy()
    }

    @Test
    @Throws(InterruptedException::class)
    fun read_write_same_key() = runBlocking<Unit> {
        writeLargeDataSet("dataset")
        delay(10)

        Log.d(TAG, "read dataset: start")
        // Read for same key 'dataset' should be postponed until writing is done
        val readData = Paper.book().read<List<Person>>("dataset") ?: emptyList()
        assertEquals(10000, readData.size.toLong())
        Log.d(TAG, "read dataset: finish")
    }

    @Test
    @Throws(InterruptedException::class)
    fun write_exists_same_key() = runBlocking<Unit> {
        assertFalse(Paper.book().contains("dataset"))

        writeLargeDataSet("dataset")
        delay(10)

        Log.d(TAG, "check dataset contains: start")
        // Read for same key 'dataset' should be postponed until writing is done
        assertTrue(Paper.book().contains("dataset"))
        Log.d(TAG, "check dataset contains: finish")
    }

    @Test
    @Throws(InterruptedException::class)
    fun write_delete_same_key() = runBlocking<Unit> {
        assertFalse(Paper.book().contains("dataset"))

        writeLargeDataSet("dataset")
        delay(10)

        Log.d(TAG, "check dataset delete: start")
        // Read for same key 'dataset' should be postponed until writing is done
        Paper.book().delete("dataset")
        assertFalse(Paper.book().contains("dataset"))
        Log.d(TAG, "check dataset delete: finish")
    }

    @Test
    @Throws(InterruptedException::class)
    fun read_write_different_keys() = runBlocking<Unit> {
        // Primary write something else
        Paper.book().write<Any>("city", "Victoria")

        // Start writing large dataset
        val job = writeLargeDataSet("dataset")

        Log.d(TAG, "read other key: start")
        // Read for different key 'city' should be locked by writing other key 'dataset'
        assertEquals("Victoria", Paper.book().read("city"))
        job.join()
        assertEquals(10000, Paper.book().read<List<Person>>("dataset")!!.size.toLong())
        Log.d(TAG, "read other key: finish")
    }

    @Test
    @Throws(Exception::class)
    fun testMultiThreadAccess() {
        val executor = Executors.newFixedThreadPool(10)
        val todo = LinkedList<Callable<Any>>()

        for (i in 0..1000) {
            val task = if (i % 2 == 0) {
                insertRunnable
            } else {
                selectRunnable
            }
            todo.add(Executors.callable(task))
        }
        val futures = executor.invokeAll(todo)
        for (future in futures) {
            future.get()
        }
    }

    @Test
    fun multipleCoroutineAccess() = runBlocking(Dispatchers.IO) {
        val totalCount = 1000
        val concurrentCount = 20
        val countEach = totalCount / concurrentCount
        for (i in 0..totalCount) {
            val index = kotlin.random.Random.nextInt(countEach)
            if (i % 2 == 0) launch {
                val size = Random().nextInt(200)
                val inserted100 = TestDataGenerator.genPersonList(size)
                Paper.book().write<Any>("persons$index", inserted100)
            } else launch {
                Paper.book().read<Any>("persons$index")
            }
        }
    }

    private suspend fun writeLargeDataSet(key: String) = coroutineScope {
        launch {
            val dataset = TestDataGenerator.genPersonList(10000)
            Log.d(TAG, "write '$key': start")
            Paper.book().write(key, dataset)
            Log.d(TAG, "write '$key': finish")
        }
    }

    companion object {
        private val TAG = "MultiThreadTest"
    }
}
