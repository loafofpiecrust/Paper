package io.paperdb

import android.support.test.runner.AndroidJUnit4

import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

import java.lang.reflect.Field

import android.support.test.InstrumentationRegistry.getTargetContext
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat

/**
 * Tests support for forward and backward compatibility. Fields can be added or removed.
 * Changing the type of a field is not supported except very limited cases like int->long.
 */
@RunWith(AndroidJUnit4::class)
class CompatibilityTest {

    @Before
    @Throws(Exception::class)
    fun setUp() {
        Paper.init(getTargetContext().filesDir)
        Paper.book().destroy()
    }

    @Test
    @Throws(IllegalAccessException::class, NoSuchFieldException::class, InstantiationException::class)
    fun testChangeClass() = runBlocking<Unit> {
        val testClass = getClassInstanceWithNewName(TestClass::class.java, TestClassNew::class.java.name)
        testClass.name = "original"
        testClass.value = "test"
        testClass.timestamp = 123

        // Save original class. Only class name is changed to TestClassNew
        Paper.book().write<Any>("test", testClass)

        // Read and instantiate a modified class TestClassNew based on saved data in TestClass
        val newTestClass = Paper.book().read<TestClassNew>("test")
        // Check original value is restored despite new default value in TestClassNew
        assertThat(newTestClass!!.name).isEqualTo("original")
        // Check default value for new added field
        assertThat(newTestClass.newField).isEqualTo("default")
        // Check compatible field type change
        assertThat(newTestClass.timestamp).isEqualTo(123L)
    }

    @Test(expected = Exception::class)
    @Throws(Exception::class)
    fun testNotCompatibleClassChanges() = runBlocking<Unit> {
        val testClass = getClassInstanceWithNewName(TestClass::class.java,
            TestClassNotCompatible::class.java.name)
        testClass.timestamp = 123
        Paper.book().write<Any>("not-compatible", testClass)

        val read = Paper.book().read<TestClassNotCompatible>("not-compatible")
        read!!.name
    }

    @Test
    @Throws(Exception::class)
    fun testTransientFields() = runBlocking<Unit> {
        val tc = TestClassTransient()
        tc.timestamp = 123
        tc.transientField = "changed"

        Paper.book().write<Any>("transient-class", tc)

        val readTc = Paper.book().read<TestClassTransient>("transient-class")
        assertThat(readTc!!.timestamp).isEqualTo(123)
        assertThat(readTc.transientField).isEqualTo("default")
    }

    @Throws(NoSuchFieldException::class, IllegalAccessException::class, InstantiationException::class)
    private fun <T> getClassInstanceWithNewName(classToInstantiate: Class<T>, newName: String): T {
        val name = classToInstantiate.javaClass.getDeclaredField("name")
        name.isAccessible = true
        name.set(classToInstantiate, newName)
        return classToInstantiate.newInstance()
    }

    class TestClass {
        var name = "original"
        var value = "test"
        var timestamp: Int = 0
    }

    /**
     * Emulates changes in class TestClass
     */
    class TestClassNew {
        var name = "new-class"
        // Has been removed
        // public String value;
        var newField = "default"
        var timestamp: Long = 0
    }

    /**
     * Emulates not compatible changes in class TestClass
     */
    class TestClassNotCompatible {
        var name = "not-compatible-class"
        var timestamp: String? = null //Changed field type long->String
    }

    class TestClassTransient {
        var name = "transient"
        @Transient
        var transientField = "default"
        var timestamp: Int = 0
    }


}
