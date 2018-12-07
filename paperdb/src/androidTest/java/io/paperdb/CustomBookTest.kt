package io.paperdb

import android.support.test.runner.AndroidJUnit4

import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

import android.support.test.InstrumentationRegistry.getTargetContext
import junit.framework.Assert.assertEquals
import junit.framework.Assert.assertTrue
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertFalse

@RunWith(AndroidJUnit4::class)
class CustomBookTest {

    @Before
    fun setUp() {
        Paper.init(getTargetContext().filesDir)
    }

    @Test
    fun getFolderPathForBook_custom() {
        val path = Paper.book("custom").path
        assertTrue(path.endsWith("/io.paperdb.test/files/custom"))
    }

    @Test
    fun getFilePathForKey_customBook() {
        val path = Paper.book("custom").getPath("my_key")
        assertTrue(path.endsWith("/io.paperdb.test/files/custom/my_key.pt"))
    }

    @Test
    fun readWriteDeleteToDifferentBooks() = runBlocking<Unit> {
        val custom = "custom"
        Paper.book().destroy()
        Paper.book(custom).destroy()

        Paper.book().write<Any>("city", "Victoria")
        Paper.book(custom).write<Any>("city", "Kyiv")

        assertEquals("Victoria", Paper.book().read<Any>("city"))
        assertEquals("Kyiv", Paper.book(custom).read<Any>("city"))

        Paper.book().delete("city")
        assertFalse(Paper.book().contains("city"))
        assertTrue(Paper.book(custom).contains("city"))
    }


}
