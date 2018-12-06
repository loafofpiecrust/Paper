package io.paperdb;

import android.support.test.runner.AndroidJUnit4;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.HashSet;

import static android.support.test.InstrumentationRegistry.getTargetContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(AndroidJUnit4.class)
public class CustomBookLocationTest {

    @Before
    public void setUp() {
        Paper.INSTANCE.init(getTargetContext().getFilesDir());
    }

    @Test
    public void readWriteDelete_customLocation_with_sub_dirs() {
        //noinspection ResultOfMethodCallIgnored
        deleteRecursive(new File(getTargetContext().getFilesDir() + "/custom"));

        String customLocation = getTargetContext().getFilesDir() + "/custom/location";
        Book book = Paper.INSTANCE.bookOn(customLocation);

        book.write("city", "Victoria");
        assertEquals("Victoria", book.read("city"));

        // Check sub folders created recursively
        File customSubDir = new File(getTargetContext().getFilesDir() + "/custom");
        assertTrue(customSubDir.exists());
        assertTrue(customSubDir.isDirectory());
        assertEquals(1, customSubDir.listFiles().length);
        assertTrue(customSubDir.listFiles()[0].isDirectory());
        assertEquals("location", customSubDir.listFiles()[0].getName());

        File locationSubDir = new File(getTargetContext().getFilesDir() + "/custom/location");
        assertTrue(locationSubDir.exists());
        assertTrue(locationSubDir.isDirectory());
        assertEquals(1, locationSubDir.listFiles().length);
        assertTrue(locationSubDir.listFiles()[0].isDirectory());
        assertEquals("io.paperdb", locationSubDir.listFiles()[0].getName());

        book.delete("city");
        assertFalse(book.contains("city"));
    }

    @Test
    public void readWriteDelete_customLocation_defaultBook() {
        String customLocation = getTargetContext().getFilesDir() + "/custom_location";
        Book bookOnSdcard = Paper.INSTANCE.bookOn(customLocation);
        Book defaultBook = Paper.INSTANCE.book();

        bookOnSdcard.destroy();
        defaultBook.destroy();

        bookOnSdcard.write("city", "Victoria");
        defaultBook.write("city", "Kyiv");

        assertEquals("Victoria", bookOnSdcard.read("city"));
        assertEquals("Kyiv", defaultBook.read("city"));

        bookOnSdcard.delete("city");

        assertFalse(bookOnSdcard.contains("city"));
        assertEquals("Kyiv", defaultBook.read("city"));
    }

    @Test
    public void readWriteDelete_customLocation_customBook() {
        String customLocation = getTargetContext().getFilesDir() + "/custom/location";
        Book bookOnSdcard = Paper.INSTANCE.bookOn(customLocation, "encyclopedia");
        Book defaultBook = Paper.INSTANCE.book("encyclopedia");

        bookOnSdcard.destroy();
        defaultBook.destroy();

        bookOnSdcard.write("city", "Victoria");
        defaultBook.write("city", "Kyiv");

        assertEquals("Victoria", bookOnSdcard.read("city"));
        assertEquals("Kyiv", defaultBook.read("city"));

        bookOnSdcard.delete("city");

        assertFalse(bookOnSdcard.contains("city"));
        assertEquals("Kyiv", defaultBook.read("city"));
    }

    @Test
    public void useCacheFolderAsCustomLocation() {
        String cachePath = getTargetContext().getCacheDir().toString();
        Book cache = Paper.INSTANCE.bookOn(cachePath);
        cache.destroy();

        cache.write("city", "Kyiv");
        assertEquals("Kyiv", cache.read("city"));

        Assert.assertTrue(cache.getPath().endsWith("/io.paperdb.test/cache/io.paperdb"));
    }

    @Test
    public void getPath() {
        Book defaultBookOnSdCard = Paper.INSTANCE.bookOn("/sdcard");
        Book encyclopediaOnSdCard = Paper.INSTANCE.bookOn("/sdcard", "encyclopedia");

        assertEquals("/sdcard/io.paperdb", defaultBookOnSdCard.getPath());
        assertEquals("/sdcard/io.paperdb/key.pt", defaultBookOnSdCard.getPath("key"));
        assertEquals("/sdcard/encyclopedia", encyclopediaOnSdCard.getPath());
        assertEquals("/sdcard/encyclopedia/key.pt", encyclopediaOnSdCard.getPath("key"));
    }

    @Test
    public void bookInstanceIsTheSameForSameLocationAndBookName() {
        Book defaultBook = Paper.INSTANCE.book();
        Book encyclopedia = Paper.INSTANCE.book("encyclopedia");
        Book defaultBookOnSdCard = Paper.INSTANCE.bookOn("/sdcard");
        Book encyclopediaOnSdCard = Paper.INSTANCE.bookOn("/sdcard", "encyclopedia");

        // Check all instances are unique
        HashSet<Book> instanceSet = new HashSet<>();
        instanceSet.add(defaultBook);
        instanceSet.add(encyclopedia);
        instanceSet.add(defaultBookOnSdCard);
        instanceSet.add(encyclopediaOnSdCard);
        assertEquals(4, instanceSet.size());

        assertSame(defaultBook, Paper.INSTANCE.book());
        assertSame(encyclopedia, Paper.INSTANCE.book("encyclopedia"));
        assertSame(defaultBookOnSdCard, Paper.INSTANCE.bookOn("/sdcard"));
        assertSame(encyclopediaOnSdCard, Paper.INSTANCE.bookOn("/sdcard", "encyclopedia"));
    }

    @Test
    public void locationCanBeWithFileSeparatorAtTheEnd() {
        assertEquals("/sdcard/io.paperdb", Paper.INSTANCE.bookOn("/sdcard").getPath());
        assertEquals("/sdcard/io.paperdb", Paper.INSTANCE.bookOn("/sdcard/").getPath());
        assertEquals("/sdcard/encyclopedia",
                Paper.INSTANCE.bookOn("/sdcard", "encyclopedia").getPath());
        assertEquals("/sdcard/encyclopedia",
                Paper.INSTANCE.bookOn("/sdcard/", "encyclopedia").getPath());
    }

    private void deleteRecursive(File fileOrDirectory) {
        if (fileOrDirectory.isDirectory())
            for (File child : fileOrDirectory.listFiles())
                deleteRecursive(child);

        //noinspection ResultOfMethodCallIgnored
        fileOrDirectory.delete();
    }
}
