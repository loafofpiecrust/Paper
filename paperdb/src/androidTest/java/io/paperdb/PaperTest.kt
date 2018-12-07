package io.paperdb

import android.os.SystemClock
import android.support.test.runner.AndroidJUnit4

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

import java.io.IOException

import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer
import io.paperdb.testdata.TestDataGenerator
import io.paperdb.utils.TestUtils

import android.support.test.InstrumentationRegistry.getTargetContext
import junit.framework.Assert.assertEquals
import junit.framework.TestCase.assertTrue
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNotNull

@RunWith(AndroidJUnit4::class)
class PaperTest {

    @Before
    @Throws(Exception::class)
    fun setUp() {
        Paper.init(getTargetContext().filesDir)
        Paper.book().destroy()
    }

    @Test
    @Throws(Exception::class)
    fun testContains() = runBlocking<Unit> {
        assertFalse(Paper.book().contains("persons"))
        Paper.book().write<Any>("persons", TestDataGenerator.genPersonList(10))
        assertTrue(Paper.book().contains("persons"))
    }

    @Test
    @Throws(Exception::class)
    fun testDelete() = runBlocking<Unit> {
        Paper.book().write<Any>("persons", TestDataGenerator.genPersonList(10))
        assertTrue(Paper.book().contains("persons"))
        Paper.book().delete("persons")
        assertFalse(Paper.book().contains("persons"))
    }

    @Test
    @Throws(Exception::class)
    fun testDeleteNotExisted() = runBlocking<Unit> {
        assertFalse(Paper.book().contains("persons"))
        Paper.book().delete("persons")
    }

    @Test
    @Throws(Exception::class)
    fun testClear() = runBlocking<Unit> {
        Paper.book().write<Any>("persons", TestDataGenerator.genPersonList(10))
        Paper.book().write<Any>("persons2", TestDataGenerator.genPersonList(20))
        assertTrue(Paper.book().contains("persons"))
        assertTrue(Paper.book().contains("persons2"))

        Paper.book().destroy()
        // init() call is not required after clear()
        assertFalse(Paper.book().contains("persons"))
        assertFalse(Paper.book().contains("persons2"))

        // Should be possible to continue to use Paper after clear()
        Paper.book().write<Any>("persons3", TestDataGenerator.genPersonList(30))
        assertTrue(Paper.book().contains("persons3"))
        assertThat(Paper.book().read<List<*>>("persons3")).hasSize(30)
    }

    @Test
    fun testWriteReadNormal() = runBlocking<Unit> {
        Paper.book().write("city", "Lund")
        val `val` = Paper.book().read<String>("city") ?: "default"
        assertThat(`val`).isEqualTo("Lund")
    }

    @Test
    fun testWriteReadNormalAfterReinit() = runBlocking<Unit> {
        Paper.book().write("city", "Lund")
        val `val` = Paper.book().read<String>("city") ?: "default"
        Paper.init(getTargetContext().filesDir)// Reinit Paper instance
        assertThat(`val`).isEqualTo("Lund")
    }

    @Test
    fun testReadNotExisted() = runBlocking<Unit> {
        val `val` = Paper.book().read<String>("non-existed")
        assertThat(`val`).isNull()
    }

    @Test
    fun testReadDefault() = runBlocking<Unit> {
        val `val` = Paper.book().read<String>("non-existed") ?: "default"
        assertThat(`val`).isEqualTo("default")
    }

    @Test
    fun testReplace() = runBlocking<Unit> {
        Paper.book().write("city", "Lund")
        assertThat(Paper.book().read<Any>("city")).isEqualTo("Lund")
        Paper.book().write("city", "Kyiv")
        assertThat(Paper.book().read<Any>("city")).isEqualTo("Kyiv")
    }

    @Test
    fun testValidKeyNames() = runBlocking<Unit> {
        Paper.book().write("city", "Lund")
        assertThat(Paper.book().read<Any>("city")).isEqualTo("Lund")

        Paper.book().write("city.dasd&%", "Lund")
        assertThat(Paper.book().read<Any>("city.dasd&%")).isEqualTo("Lund")

        Paper.book().write("city-ads", "Lund")
        assertThat(Paper.book().read<Any>("city-ads")).isEqualTo("Lund")
    }

    @Test(expected = PaperDbException::class)
    fun testInvalidKeyNameBackslash() = runBlocking<Unit> {
        Paper.book().write<Any>("city/ads", "Lund")
        assertThat(Paper.book().read<Any>("city/ads")).isEqualTo("Lund")
    }

    @Test(expected = PaperDbException::class)
    fun testGetBookWithDefaultBookName() {
        Paper.book(Paper.DEFAULT_DB_NAME)
    }

    @Test
    fun testCustomBookReadWrite() = runBlocking<Unit> {
        val NATIVE = "native"
        assertThat(Paper.book()).isNotSameAs(Paper.book(NATIVE))
        Paper.book(NATIVE).destroy()

        Paper.book().write<Any>("city", "Lund")
        Paper.book(NATIVE).write<Any>("city", "Kyiv")

        assertThat(Paper.book().read<Any>("city")).isEqualTo("Lund")
        assertThat(Paper.book(NATIVE).read<Any>("city")).isEqualTo("Kyiv")
    }

    @Test
    fun testCustomBookDestroy() = runBlocking<Unit> {
        val NATIVE = "native"
        Paper.book(NATIVE).destroy()

        Paper.book().write<Any>("city", "Lund")
        Paper.book(NATIVE).write<Any>("city", "Kyiv")

        Paper.book(NATIVE).destroy()

        assertThat(Paper.book().read<Any>("city")).isEqualTo("Lund")
        assertThat(Paper.book(NATIVE).read<Any>("city")).isNull()
    }

    @Test
    fun testGetAllKeys() = runBlocking<Unit> {
        Paper.book().destroy()

        Paper.book().write<Any>("city", "Lund")
        Paper.book().write<Any>("city1", "Lund1")
        Paper.book().write<Any>("city2", "Lund2")
        val allKeys = Paper.book().allKeys

        assertThat(allKeys.size).isEqualTo(3)
        assertThat(allKeys.contains("city")).isTrue
        assertThat(allKeys.contains("city1")).isTrue
        assertThat(allKeys.contains("city2")).isTrue
    }

    @Test
    fun testCustomSerializer() = runBlocking<Unit> {
        Paper.register(DateTime::class.java, 100, JodaDateTimeSerializer())
        val now = DateTime.now(DateTimeZone.UTC)

        Paper.book().write<Any>("joda-datetime", now)
        assertEquals(now, Paper.book().read("joda-datetime"))
    }

    @Test
    fun testTimestampNoObject() = runBlocking<Unit> {
        Paper.book().destroy()
        val timestamp = Paper.book().lastModified("city")
        assertEquals(-1, timestamp)
    }

    @Test
    fun testTimestamp() = runBlocking<Unit> {
        val testStartMS = System.currentTimeMillis()

        Paper.book().destroy()
        Paper.book().write<Any>("city", "Lund")

        val fileWriteMS = Paper.book().lastModified("city")
        assertNotEquals(-1, fileWriteMS)

        val elapsed = fileWriteMS - testStartMS
        // Many file systems only support seconds granularity for last-modification time
        assertThat(elapsed < 1000 || elapsed > -1000).isTrue
    }

    @Test
    fun testTimestampChanges() = runBlocking<Unit> {
        Paper.book().destroy()
        Paper.book().write<Any>("city", "Lund")
        val fileWrite1MS = Paper.book().lastModified("city")

        // Add 1 sec delay as many file systems only support seconds granularity for last-modification time
        SystemClock.sleep(1000)

        Paper.book().write<Any>("city", "Kyiv")
        val fileWrite2MS = Paper.book().lastModified("city")

        assertThat(fileWrite2MS > fileWrite1MS).isTrue
    }

    @Test
    @Throws(IOException::class)
    fun testDbFileExistsAfterFailedRead() = runBlocking<Unit> {
        val key = "cityMap"
        assertFalse(Paper.book().contains(key))

        TestUtils.replacePaperDbFileBy("invalid_data.pt", key)
        assertTrue(Paper.book().contains(key))

        var expectedException: Throwable? = null
        try {
            val thing = Paper.book().read<Any>(key)
            assertThat(thing).isNull()
        } catch (e: PaperDbException) {
            expectedException = e
            assertThat(e).isNotNull
        }

        // Data file should exist even if previous read attempt was failed
        assertTrue(Paper.book().contains(key))
    }

    @Test
    fun getFolderPathForBook_default() {
        val path = Paper.book().path
        assertTrue(path.endsWith("/io.paperdb.test/files/io.paperdb"))
    }

    @Test
    fun getFilePathForKey_defaultBook() {
        val path = Paper.book().getPath("my_key")
        assertTrue(path.endsWith("/io.paperdb.test/files/io.paperdb/my_key.pt"))
    }

}