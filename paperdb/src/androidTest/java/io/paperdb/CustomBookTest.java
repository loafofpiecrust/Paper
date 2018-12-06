package io.paperdb;

import android.support.test.runner.AndroidJUnit4;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static android.support.test.InstrumentationRegistry.getTargetContext;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

@RunWith(AndroidJUnit4.class)
public class CustomBookTest {

    @Before
    public void setUp() {
        Paper.INSTANCE.init(getTargetContext().getFilesDir());
    }

    @Test
    public void getFolderPathForBook_custom() {
        String path = Paper.INSTANCE.book("custom").getPath();
        assertTrue(path.endsWith("/io.paperdb.test/files/custom"));
    }

    @Test
    public void getFilePathForKey_customBook() {
        String path = Paper.INSTANCE.book("custom").getPath("my_key");
        assertTrue(path.endsWith("/io.paperdb.test/files/custom/my_key.pt"));
    }

    @Test
    public void readWriteDeleteToDifferentBooks() {
        String custom = "custom";
        Paper.INSTANCE.book().destroy();
        Paper.INSTANCE.book(custom).destroy();

        Paper.INSTANCE.book().write("city", "Victoria");
        Paper.INSTANCE.book(custom).write("city", "Kyiv");

        assertEquals("Victoria", Paper.INSTANCE.book().read("city"));
        assertEquals("Kyiv", Paper.INSTANCE.book(custom).read("city"));

        Paper.INSTANCE.book().delete("city");
        assertFalse(Paper.INSTANCE.book().contains("city"));
        assertTrue(Paper.INSTANCE.book(custom).contains("city"));
    }



}
