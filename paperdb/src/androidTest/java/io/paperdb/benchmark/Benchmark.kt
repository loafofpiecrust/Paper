package io.paperdb.benchmark

import android.os.SystemClock
import android.support.test.filters.LargeTest
import android.support.test.runner.AndroidJUnit4
//import android.test.AndroidTestCase;
//import android.test.suitebuilder.annotation.LargeTest;
import android.util.Log

import com.orhanobut.hawk.Hawk

import org.junit.Test
import org.junit.runner.RunWith

import io.paperdb.Paper
import io.paperdb.testdata.Person
import io.paperdb.testdata.PersonArg
import io.paperdb.testdata.TestDataGenerator

import android.support.test.InstrumentationRegistry.getTargetContext
import kotlinx.coroutines.runBlocking

@RunWith(AndroidJUnit4::class)
@LargeTest
class Benchmark {

    @Test
    @Throws(Exception::class)
    fun testReadWrite500Contacts() {
        val contacts = TestDataGenerator.genPersonList(OBJECT_COUNT)
        Paper.init(getTargetContext().filesDir)
        Paper.book().destroy()
        val paperTime = runTest(PaperReadWriteContactsTest(), contacts, REPEAT_COUNT)

        Hawk.init(getTargetContext())
        Hawk.clear()
        val hawkTime = runTest(HawkReadWriteContactsTest(), contacts, REPEAT_COUNT)

        val contactsArg = TestDataGenerator.genPersonArgList(OBJECT_COUNT)
        Paper.init(getTargetContext().filesDir)
        Paper.book().destroy()
        val paperArg = runTest(PaperReadWriteContactsArgTest(), contactsArg, REPEAT_COUNT)

        printResults("Read/write 500 contacts", paperTime, hawkTime, paperArg)
    }

    @Test
    @Throws(Exception::class)
    fun testWrite500Contacts() {
        val contacts = TestDataGenerator.genPersonList(OBJECT_COUNT)
        Paper.init(getTargetContext().filesDir)
        Paper.book().destroy()
        val paperTime = runTest(PaperWriteContactsTest(), contacts, REPEAT_COUNT)

        Hawk.init(getTargetContext())
        Hawk.clear()
        val hawkTime = runTest(HawkWriteContactsTest(), contacts, REPEAT_COUNT)

        printResults("Write 500 contacts", paperTime, hawkTime)
    }

    @Test
    @Throws(Exception::class)
    fun testRead500Contacts() {
        val contacts = TestDataGenerator.genPersonList(OBJECT_COUNT)
        Paper.init(getTargetContext().filesDir)
        Paper.book().destroy()
        runTest(PaperWriteContactsTest(), contacts, REPEAT_COUNT) //Prepare
        val paperTime = runTest(PaperReadContactsTest(), contacts, REPEAT_COUNT)

        Hawk.init(getTargetContext())
        Hawk.clear()
        runTest(HawkWriteContactsTest(), contacts, REPEAT_COUNT) //Prepare
        val hawkTime = runTest(HawkReadContactsTest(), contacts, REPEAT_COUNT)

        printResults("Read 500 contacts", paperTime, hawkTime)
    }

    private fun printResults(name: String, paperTime: Long, hawkTime: Long) {
        Log.i(TAG, String.format("..................................\n%s \n Paper: %d \n Hawk: %d",
            name, paperTime, hawkTime))
    }

    private fun printResults(name: String, paperTime: Long, hawkTime: Long, paperArgTime: Long) {
        Log.i(TAG, String.format("..................................\n%s " + "\n Paper: %d \n Paper(arg-cons): %d \n Hawk: %d",
            name, paperTime, paperArgTime, hawkTime))
    }

    private fun <T> runTest(task: TestTask<T>, extra: T, repeat: Int): Long {
        val start = SystemClock.uptimeMillis()
        for (i in 0 until repeat) {
            task.run(i, extra)
        }
        return (SystemClock.uptimeMillis() - start) / repeat
    }

    internal interface TestTask<T> {
        fun run(i: Int, extra: T)
    }

    private inner class PaperReadWriteContactsTest : TestTask<List<Person>> {
        override fun run(i: Int, extra: List<Person>) = runBlocking<Unit> {
            val key = "contacts$i"
            Paper.book().write<Any>(key, extra)
            Paper.book().read<List<Person>>(key)
        }
    }

    private inner class PaperReadWriteContactsArgTest : TestTask<List<PersonArg>> {
        override fun run(i: Int, extra: List<PersonArg>) = runBlocking<Unit> {
            val key = "contacts$i"
            Paper.book().write<Any>(key, extra)
            Paper.book().read<List<Person>>(key)
        }
    }

    private inner class HawkReadWriteContactsTest : TestTask<List<Person>> {
        override fun run(i: Int, extra: List<Person>) {
            val key = "contacts$i"
            Hawk.put(key, extra)
            Hawk.get<List<Person>>(key)
        }
    }

    private inner class PaperWriteContactsTest : TestTask<List<Person>> {
        override fun run(i: Int, extra: List<Person>) = runBlocking<Unit> {
            val key = "contacts$i"
            Paper.book().write<Any>(key, extra)
        }
    }

    private inner class HawkWriteContactsTest : TestTask<List<Person>> {
        override fun run(i: Int, extra: List<Person>) {
            val key = "contacts$i"
            Hawk.put(key, extra)
        }
    }

    private inner class PaperReadContactsTest : TestTask<List<Person>> {
        override fun run(i: Int, extra: List<Person>) = runBlocking<Unit> {
            val key = "contacts$i"
            Paper.book().read<List<Person>>(key)
        }
    }

    private inner class HawkReadContactsTest : TestTask<List<Person>> {
        override fun run(i: Int, extra: List<Person>) {
            val key = "contacts$i"
            Hawk.get<List<Person>>(key)
        }
    }

    companion object {
        private const val TAG = "paper-benchmark"

        private const val REPEAT_COUNT = 30
        private const val OBJECT_COUNT = 1000
    }
}
