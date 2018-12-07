package io.paperdb

import android.support.test.runner.AndroidJUnit4

import junit.framework.Assert

import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

import java.io.File
import java.util.HashSet

import android.support.test.InstrumentationRegistry.getTargetContext
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue

@RunWith(AndroidJUnit4::class)
class CustomBookLocationTest {

    @Before
    fun setUp() {
        Paper.init(getTargetContext().filesDir)
    }

    @Test
    fun readWriteDelete_customLocation_with_sub_dirs() = runBlocking<Unit> {

        deleteRecursive(File(getTargetContext().filesDir.toString() + "/custom"))

        val customLocation = getTargetContext().filesDir.toString() + "/custom/location"
        val book = Paper.bookOn(customLocation)

        book.write<Any>("city", "Victoria")
        assertEquals("Victoria", book.read("city"))

        // Check sub folders created recursively
        val customSubDir = File(getTargetContext().filesDir.toString() + "/custom")
        assertTrue(customSubDir.exists())
        assertTrue(customSubDir.isDirectory)
        assertEquals(1, customSubDir.listFiles().size.toLong())
        assertTrue(customSubDir.listFiles()[0].isDirectory)
        assertEquals("location", customSubDir.listFiles()[0].name)

        val locationSubDir = File(getTargetContext().filesDir.toString() + "/custom/location")
        assertTrue(locationSubDir.exists())
        assertTrue(locationSubDir.isDirectory)
        assertEquals(1, locationSubDir.listFiles().size.toLong())
        assertTrue(locationSubDir.listFiles()[0].isDirectory)
        assertEquals("io.paperdb", locationSubDir.listFiles()[0].name)

        book.delete("city")
        assertFalse(book.contains("city"))
    }

    @Test
    fun readWriteDelete_customLocation_defaultBook() = runBlocking<Unit> {
        val customLocation = getTargetContext().filesDir.toString() + "/custom_location"
        val bookOnSdcard = Paper.bookOn(customLocation)
        val defaultBook = Paper.book()

        bookOnSdcard.destroy()
        defaultBook.destroy()

        bookOnSdcard.write<Any>("city", "Victoria")
        defaultBook.write<Any>("city", "Kyiv")

        assertEquals("Victoria", bookOnSdcard.read("city"))
        assertEquals("Kyiv", defaultBook.read("city"))

        bookOnSdcard.delete("city")

        assertFalse(bookOnSdcard.contains("city"))
        assertEquals("Kyiv", defaultBook.read("city"))
    }

    @Test
    fun readWriteDelete_customLocation_customBook() = runBlocking<Unit> {
        val customLocation = getTargetContext().filesDir.toString() + "/custom/location"
        val bookOnSdcard = Paper.bookOn(customLocation, "encyclopedia")
        val defaultBook = Paper.book("encyclopedia")

        bookOnSdcard.destroy()
        defaultBook.destroy()

        bookOnSdcard.write<Any>("city", "Victoria")
        defaultBook.write<Any>("city", "Kyiv")

        assertEquals("Victoria", bookOnSdcard.read("city"))
        assertEquals("Kyiv", defaultBook.read("city"))

        bookOnSdcard.delete("city")

        assertFalse(bookOnSdcard.contains("city"))
        assertEquals("Kyiv", defaultBook.read("city"))
    }

    @Test
    fun useCacheFolderAsCustomLocation() = runBlocking<Unit> {
        val cachePath = getTargetContext().cacheDir.toString()
        val cache = Paper.bookOn(cachePath)
        cache.destroy()

        cache.write<Any>("city", "Kyiv")
        assertEquals("Kyiv", cache.read("city"))

        Assert.assertTrue(cache.path.endsWith("/io.paperdb.test/cache/io.paperdb"))
    }

    @Test
    fun getPath() {
        val defaultBookOnSdCard = Paper.bookOn("/sdcard")
        val encyclopediaOnSdCard = Paper.bookOn("/sdcard", "encyclopedia")

        assertEquals("/sdcard/io.paperdb", defaultBookOnSdCard.path)
        assertEquals("/sdcard/io.paperdb/key.pt", defaultBookOnSdCard.getPath("key"))
        assertEquals("/sdcard/encyclopedia", encyclopediaOnSdCard.path)
        assertEquals("/sdcard/encyclopedia/key.pt", encyclopediaOnSdCard.getPath("key"))
    }

    @Test
    fun bookInstanceIsTheSameForSameLocationAndBookName() {
        val defaultBook = Paper.book()
        val encyclopedia = Paper.book("encyclopedia")
        val defaultBookOnSdCard = Paper.bookOn("/sdcard")
        val encyclopediaOnSdCard = Paper.bookOn("/sdcard", "encyclopedia")

        // Check all instances are unique
        val instanceSet = HashSet<Book>()
        instanceSet.add(defaultBook)
        instanceSet.add(encyclopedia)
        instanceSet.add(defaultBookOnSdCard)
        instanceSet.add(encyclopediaOnSdCard)
        assertEquals(4, instanceSet.size.toLong())

        assertSame(defaultBook, Paper.book())
        assertSame(encyclopedia, Paper.book("encyclopedia"))
        assertSame(defaultBookOnSdCard, Paper.bookOn("/sdcard"))
        assertSame(encyclopediaOnSdCard, Paper.bookOn("/sdcard", "encyclopedia"))
    }

    @Test
    fun locationCanBeWithFileSeparatorAtTheEnd() {
        assertEquals("/sdcard/io.paperdb", Paper.bookOn("/sdcard").path)
        assertEquals("/sdcard/io.paperdb", Paper.bookOn("/sdcard/").path)
        assertEquals("/sdcard/encyclopedia",
            Paper.bookOn("/sdcard", "encyclopedia").path)
        assertEquals("/sdcard/encyclopedia",
            Paper.bookOn("/sdcard/", "encyclopedia").path)
    }

    private fun deleteRecursive(fileOrDirectory: File) {
        if (fileOrDirectory.isDirectory)
            for (child in fileOrDirectory.listFiles())
                deleteRecursive(child)


        fileOrDirectory.delete()
    }
}
