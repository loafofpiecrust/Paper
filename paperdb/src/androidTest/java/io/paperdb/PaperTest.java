package io.paperdb;

import android.os.SystemClock;
import android.support.test.runner.AndroidJUnit4;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;

import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import io.paperdb.testdata.TestDataGenerator;
import io.paperdb.utils.TestUtils;

import static android.support.test.InstrumentationRegistry.getTargetContext;
import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(AndroidJUnit4.class)
public class PaperTest {

    @Before
    public void setUp() throws Exception {
        Paper.INSTANCE.init(getTargetContext().getFilesDir());
        Paper.INSTANCE.book().destroy();
    }

    @Test
    public void testContains() throws Exception {
        assertFalse(Paper.INSTANCE.book().contains("persons"));
        Paper.INSTANCE.book().write("persons", TestDataGenerator.genPersonList(10));
        assertTrue(Paper.INSTANCE.book().contains("persons"));
    }

    @Test
    public void testDelete() throws Exception {
        Paper.INSTANCE.book().write("persons", TestDataGenerator.genPersonList(10));
        assertTrue(Paper.INSTANCE.book().contains("persons"));
        Paper.INSTANCE.book().delete("persons");
        assertFalse(Paper.INSTANCE.book().contains("persons"));
    }

    @Test
    public void testDeleteNotExisted() throws Exception {
        assertFalse(Paper.INSTANCE.book().contains("persons"));
        Paper.INSTANCE.book().delete("persons");
    }

    @Test
    public void testClear() throws Exception {
        Paper.INSTANCE.book().write("persons", TestDataGenerator.genPersonList(10));
        Paper.INSTANCE.book().write("persons2", TestDataGenerator.genPersonList(20));
        assertTrue(Paper.INSTANCE.book().contains("persons"));
        assertTrue(Paper.INSTANCE.book().contains("persons2"));

        Paper.INSTANCE.book().destroy();
        // init() call is not required after clear()
        assertFalse(Paper.INSTANCE.book().contains("persons"));
        assertFalse(Paper.INSTANCE.book().contains("persons2"));

        // Should be possible to continue to use Paper after clear()
        Paper.INSTANCE.book().write("persons3", TestDataGenerator.genPersonList(30));
        assertTrue(Paper.INSTANCE.book().contains("persons3"));
        assertThat(Paper.INSTANCE.book().<List>read("persons3")).hasSize(30);
    }

    @Test
    public void testWriteReadNormal() {
        Paper.INSTANCE.book().write("city", "Lund");
        String val = Paper.INSTANCE.book().read("city", "default");
        assertThat(val).isEqualTo("Lund");
    }

    @Test
    public void testWriteReadNormalAfterReinit() {
        Paper.INSTANCE.book().write("city", "Lund");
        String val = Paper.INSTANCE.book().read("city", "default");
        Paper.INSTANCE.init(getTargetContext().getFilesDir());// Reinit Paper instance
        assertThat(val).isEqualTo("Lund");
    }

    @Test
    public void testReadNotExisted() {
        String val = Paper.INSTANCE.book().read("non-existed");
        assertThat(val).isNull();
    }

    @Test
    public void testReadDefault() {
        String val = Paper.INSTANCE.book().read("non-existed", "default");
        assertThat(val).isEqualTo("default");
    }

    @Test(expected = PaperDbException.class)
    public void testWriteNull() {
        Paper.INSTANCE.book().write("city", null);
    }

    @Test
    public void testReplace() {
        Paper.INSTANCE.book().write("city", "Lund");
        assertThat(Paper.INSTANCE.book().read("city")).isEqualTo("Lund");
        Paper.INSTANCE.book().write("city", "Kyiv");
        assertThat(Paper.INSTANCE.book().read("city")).isEqualTo("Kyiv");
    }

    @Test
    public void testValidKeyNames() {
        Paper.INSTANCE.book().write("city", "Lund");
        assertThat(Paper.INSTANCE.book().read("city")).isEqualTo("Lund");

        Paper.INSTANCE.book().write("city.dasd&%", "Lund");
        assertThat(Paper.INSTANCE.book().read("city.dasd&%")).isEqualTo("Lund");

        Paper.INSTANCE.book().write("city-ads", "Lund");
        assertThat(Paper.INSTANCE.book().read("city-ads")).isEqualTo("Lund");
    }

    @Test(expected = PaperDbException.class)
    public void testInvalidKeyNameBackslash() {
        Paper.INSTANCE.book().write("city/ads", "Lund");
        assertThat(Paper.INSTANCE.book().read("city/ads")).isEqualTo("Lund");
    }

    @Test(expected = PaperDbException.class)
    public void testGetBookWithDefaultBookName() {
        Paper.INSTANCE.book(Paper.DEFAULT_DB_NAME);
    }

    @Test
    public void testCustomBookReadWrite() {
        final String NATIVE = "native";
        assertThat(Paper.INSTANCE.book()).isNotSameAs(Paper.INSTANCE.book(NATIVE));
        Paper.INSTANCE.book(NATIVE).destroy();

        Paper.INSTANCE.book().write("city", "Lund");
        Paper.INSTANCE.book(NATIVE).write("city", "Kyiv");

        assertThat(Paper.INSTANCE.book().read("city")).isEqualTo("Lund");
        assertThat(Paper.INSTANCE.book(NATIVE).read("city")).isEqualTo("Kyiv");
    }

    @Test
    public void testCustomBookDestroy() {
        final String NATIVE = "native";
        Paper.INSTANCE.book(NATIVE).destroy();

        Paper.INSTANCE.book().write("city", "Lund");
        Paper.INSTANCE.book(NATIVE).write("city", "Kyiv");

        Paper.INSTANCE.book(NATIVE).destroy();

        assertThat(Paper.INSTANCE.book().read("city")).isEqualTo("Lund");
        assertThat(Paper.INSTANCE.book(NATIVE).read("city")).isNull();
    }

    @Test
    public void testGetAllKeys() {
        Paper.INSTANCE.book().destroy();

        Paper.INSTANCE.book().write("city", "Lund");
        Paper.INSTANCE.book().write("city1", "Lund1");
        Paper.INSTANCE.book().write("city2", "Lund2");
        List<String> allKeys = Paper.INSTANCE.book().getAllKeys();

        assertThat(allKeys.size()).isEqualTo(3);
        assertThat(allKeys.contains("city")).isTrue();
        assertThat(allKeys.contains("city1")).isTrue();
        assertThat(allKeys.contains("city2")).isTrue();
    }

    @Test
    public void testCustomSerializer() {
        Paper.INSTANCE.register(DateTime.class, new JodaDateTimeSerializer(), 100);
        DateTime now = DateTime.now(DateTimeZone.UTC);

        Paper.INSTANCE.book().write("joda-datetime", now);
        assertEquals(now, Paper.INSTANCE.book().read("joda-datetime"));
    }

    @Test
    public void testTimestampNoObject() {
        Paper.INSTANCE.book().destroy();
        long timestamp = Paper.INSTANCE.book().lastModified("city");
        assertEquals(-1, timestamp);
    }

    @Test
    public void testTimestamp() {
        long testStartMS = System.currentTimeMillis();

        Paper.INSTANCE.book().destroy();
        Paper.INSTANCE.book().write("city", "Lund");

        long fileWriteMS = Paper.INSTANCE.book().lastModified("city");
        assertNotEquals(-1, fileWriteMS);

        long elapsed = fileWriteMS - testStartMS;
        // Many file systems only support seconds granularity for last-modification time
        assertThat(elapsed < 1000 || elapsed > -1000).isTrue();
    }

    @Test
    public void testTimestampChanges() {
        Paper.INSTANCE.book().destroy();
        Paper.INSTANCE.book().write("city", "Lund");
        long fileWrite1MS = Paper.INSTANCE.book().lastModified("city");

        // Add 1 sec delay as many file systems only support seconds granularity for last-modification time
        SystemClock.sleep(1000);

        Paper.INSTANCE.book().write("city", "Kyiv");
        long fileWrite2MS = Paper.INSTANCE.book().lastModified("city");

        assertThat(fileWrite2MS > fileWrite1MS).isTrue();
    }

    @Test
    public void testDbFileExistsAfterFailedRead() throws IOException {
        String key = "cityMap";
        assertFalse(Paper.INSTANCE.book().contains(key));

        TestUtils.replacePaperDbFileBy("invalid_data.pt", key);
        assertTrue(Paper.INSTANCE.book().contains(key));

        Throwable expectedException = null;
        try {
            Paper.INSTANCE.book().read(key);
        } catch (PaperDbException e) {
            expectedException = e;
        }
        assertNotNull(expectedException);
        // Data file should exist even if previous read attempt was failed
        assertTrue(Paper.INSTANCE.book().contains(key));
    }

    @Test
    public void getFolderPathForBook_default() {
        String path = Paper.INSTANCE.book().getPath();
        assertTrue(path.endsWith("/io.paperdb.test/files/io.paperdb"));
    }

    @Test
    public void getFilePathForKey_defaultBook() {
        String path = Paper.INSTANCE.book().getPath("my_key");
        assertTrue(path.endsWith("/io.paperdb.test/files/io.paperdb/my_key.pt"));
    }

}