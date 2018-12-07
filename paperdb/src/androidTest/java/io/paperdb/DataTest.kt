package io.paperdb

import android.support.test.runner.AndroidJUnit4

import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

import java.util.ArrayList
import java.util.Arrays
import java.util.Collections
import java.util.GregorianCalendar
import java.util.LinkedList

import io.paperdb.testdata.Person
import io.paperdb.testdata.PersonArg

import android.support.test.InstrumentationRegistry.getTargetContext
import io.paperdb.testdata.TestDataGenerator.genPerson
import io.paperdb.testdata.TestDataGenerator.genPersonList
import io.paperdb.testdata.TestDataGenerator.genPersonMap
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat

/**
 * Tests List write/read API
 */
@RunWith(AndroidJUnit4::class)
class DataTest {

    @Before
    @Throws(Exception::class)
    fun setUp() {
        Paper.init(getTargetContext().filesDir)
        Paper.book().destroy()
    }

    @Test
    @Throws(Exception::class)
    fun testPutEmptyList() = runBlocking<Unit> {
        val inserted = genPersonList(0)
        Paper.book().write<Any>("persons", inserted)
        assertThat(Paper.book().read<List<*>>("persons")).isEmpty()
    }

    @Test
    fun testPutGetList() = runBlocking<Unit> {
        val inserted = genPersonList(10000)
        Paper.book().write<Any>("persons", inserted)
        val persons = Paper.book().read<List<Person>>("persons")
        assertThat(persons).isEqualTo(inserted)
    }

    @Test
    fun testPutMap() = runBlocking<Unit> {
        val inserted = genPersonMap(10000)
        Paper.book().write<Any>("persons", inserted)

        val personMap = Paper.book().read<Map<Int, Person>>("persons")
        assertThat(personMap).isEqualTo(inserted)
    }

    @Test
    fun testPutPOJO() = runBlocking<Unit> {
        val person = genPerson(Person(), 1)
        Paper.book().write<Any>("profile", person)

        val savedPerson = Paper.book().read<Person>("profile")
        assertThat(savedPerson).isEqualTo(person)
        assertThat(savedPerson).isNotSameAs(person)
    }

    @Test
    fun testPutSubAbstractListRandomAccess() {
        val origin = genPersonList(100)
        val sublist = origin.subList(10, 30)
        testReadWriteWithoutClassCheck(sublist)
    }

    @Test
    fun testPutSubAbstractList() {
        val origin = LinkedList(genPersonList(100))
        val sublist = origin.subList(10, 30)
        testReadWriteWithoutClassCheck(sublist)
    }

    @Test
    fun testPutLinkedList() {
        val origin = LinkedList(genPersonList(100))
        testReadWrite(origin)
    }

    @Test
    fun testPutArraysAsLists() {
        testReadWrite(Arrays.asList("123", "345"))
    }

    @Test
    fun testPutCollectionsEmptyList() {
        testReadWrite(emptyList<Any>())
    }

    @Test
    fun testPutCollectionsEmptyMap() {
        testReadWrite(emptyMap<Any, Any>())
    }

    @Test
    fun testPutCollectionsEmptySet() {
        testReadWrite(emptySet<Any>())
    }

    @Test
    fun testPutSingletonList() {
        testReadWrite(listOf("item"))
    }

    @Test
    fun testPutSingletonSet() {
        testReadWrite(setOf("item"))
    }

    @Test
    fun testPutSingletonMap() {
        testReadWrite(Collections.singletonMap("key", "value"))
    }

    @Test
    fun testPutGeorgianCalendar() {
        testReadWrite(GregorianCalendar())
    }

    @Test
    fun testPutSynchronizedList() {
        testReadWrite(Collections.synchronizedList(ArrayList<Any>()))
    }

    @Test
    fun testReadWriteClassWithoutNoArgConstructor() {
        testReadWrite(PersonArg("name"))
    }

    private fun testReadWriteWithoutClassCheck(originObj: Any): Any? = runBlocking {
        Paper.book().write("obj", originObj)
        val readObj = Paper.book().read<Any>("obj")
        assertThat(readObj).isNotNull
        assertThat(readObj).isEqualTo(originObj)
        readObj
    }

    private fun testReadWrite(originObj: Any) {
        val readObj = testReadWriteWithoutClassCheck(originObj)
        assertThat(readObj!!.javaClass).isEqualTo(originObj.javaClass)
    }

}
