package io.paperdb.utils

import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream

import android.support.test.InstrumentationRegistry.getTargetContext

object TestUtils {

    @Throws(IOException::class)
    fun replacePaperDbFileBy(fileName: String, asKey: String) {
        val filesDir = File(getTargetContext().filesDir, "io.paperdb")
        if (!filesDir.exists()) {

            filesDir.mkdirs()
        }

        val inputStream = TestUtils::class.java.classLoader!!.getResourceAsStream(fileName)
        val outputStream = FileOutputStream(File(filesDir, "$asKey.pt"))
        copyFile(inputStream, outputStream)
        outputStream.close()
    }

    @Throws(IOException::class)
    private fun copyFile(inputStream: InputStream, outputStream: OutputStream) {
        val buffer = ByteArray(2048)

        while (true) {
            val r = inputStream.read(buffer)
            if (r < 0) {
                break
            }

            outputStream.write(buffer)
        }
        outputStream.flush()
    }
}
