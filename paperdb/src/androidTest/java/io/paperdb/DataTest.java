package io.paperdb;

import android.support.test.runner.AndroidJUnit4;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import io.paperdb.testdata.Person;
import io.paperdb.testdata.PersonArg;

import static android.support.test.InstrumentationRegistry.getTargetContext;
import static io.paperdb.testdata.TestDataGenerator.genPerson;
import static io.paperdb.testdata.TestDataGenerator.genPersonList;
import static io.paperdb.testdata.TestDataGenerator.genPersonMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests List write/read API
 */
@RunWith(AndroidJUnit4.class)
public class DataTest {

    @Before
    public void setUp() throws Exception {
        Paper.INSTANCE.init(getTargetContext().getFilesDir());
        Paper.INSTANCE.book().destroy();
    }

    @Test
    public void testPutEmptyList() throws Exception {
        final List<Person> inserted = genPersonList(0);
        Paper.INSTANCE.book().write("persons", inserted);
        assertThat(Paper.INSTANCE.book().<List>read("persons")).isEmpty();
    }

    @Test
    public void testPutGetList() {
        final List<Person> inserted = genPersonList(10000);
        Paper.INSTANCE.book().write("persons", inserted);
        List<Person> persons = Paper.INSTANCE.book().read("persons");
        assertThat(persons).isEqualTo(inserted);
    }

    @Test
    public void testPutMap() {
        final Map<Integer, Person> inserted = genPersonMap(10000);
        Paper.INSTANCE.book().write("persons", inserted);

        final Map<Integer, Person> personMap = Paper.INSTANCE.book().read("persons");
        assertThat(personMap).isEqualTo(inserted);
    }

    @Test
    public void testPutPOJO() {
        final Person person = genPerson(new Person(), 1);
        Paper.INSTANCE.book().write("profile", person);

        final Person savedPerson = Paper.INSTANCE.book().read("profile");
        assertThat(savedPerson).isEqualTo(person);
        assertThat(savedPerson).isNotSameAs(person);
    }

    @Test
    public void testPutSubAbstractListRandomAccess() {
        final List<Person> origin = genPersonList(100);
        List<Person> sublist = origin.subList(10, 30);
        testReadWriteWithoutClassCheck(sublist);
    }

    @Test
    public void testPutSubAbstractList() {
        final LinkedList<Person> origin = new LinkedList<>(genPersonList(100));
        List<Person> sublist = origin.subList(10, 30);
        testReadWriteWithoutClassCheck(sublist);
    }

    @Test
    public void testPutLinkedList() {
        final LinkedList<Person> origin = new LinkedList<>(genPersonList(100));
        testReadWrite(origin);
    }

    @Test
    public void testPutArraysAsLists() {
        testReadWrite(Arrays.asList("123", "345"));
    }

    @Test
    public void testPutCollectionsEmptyList() {
        testReadWrite(Collections.emptyList());
    }

    @Test
    public void testPutCollectionsEmptyMap() {
        testReadWrite(Collections.emptyMap());
    }

    @Test
    public void testPutCollectionsEmptySet() {
        testReadWrite(Collections.emptySet());
    }

    @Test
    public void testPutSingletonList() {
        testReadWrite(Collections.singletonList("item"));
    }

    @Test
    public void testPutSingletonSet() {
        testReadWrite(Collections.singleton("item"));
    }

    @Test
    public void testPutSingletonMap() {
        testReadWrite(Collections.singletonMap("key", "value"));
    }

    @Test
    public void testPutGeorgianCalendar() {
        testReadWrite(new GregorianCalendar());
    }

    @Test
    public void testPutSynchronizedList() {
        testReadWrite(Collections.synchronizedList(new ArrayList<>()));
    }

    @Test()
    public void testReadWriteClassWithoutNoArgConstructor() {
        testReadWrite(new PersonArg("name"));
    }

    private Object testReadWriteWithoutClassCheck(Object originObj) {
        Paper.INSTANCE.book().write("obj", originObj);
        Object readObj = Paper.INSTANCE.book().read("obj");
        assertThat(readObj).isEqualTo(originObj);
        return readObj;
    }

    private void testReadWrite(Object originObj) {
        Object readObj = testReadWriteWithoutClassCheck(originObj);
        assertThat(readObj.getClass()).isEqualTo(originObj.getClass());
    }

}
