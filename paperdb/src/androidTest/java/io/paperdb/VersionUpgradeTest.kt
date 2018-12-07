package io.paperdb

import android.support.test.runner.AndroidJUnit4

import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

import java.io.IOException
import java.util.HashMap

import io.paperdb.utils.TestUtils

import android.support.test.InstrumentationRegistry.getTargetContext
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat

@RunWith(AndroidJUnit4::class)
class VersionUpgradeTest {
    @Before
    @Throws(Exception::class)
    fun setUp() {
        Paper.init(getTargetContext().filesDir)
        Paper.book().destroy()
    }

    @Test
    fun testWrite() = runBlocking<Unit> {
        val recipe = Recipe()
        recipe.name = "chocolate cake"
        recipe.ingredients = HashMap()
        recipe.ingredients!!["flour"] = 300
        recipe.ingredients!!["eggs"] = 4
        recipe.ingredients!!["chocolate"] = 200
        recipe.duration = 30

        Paper.book().write<Any>("recipe", recipe)
    }

    @Test
    @Throws(IOException::class)
    fun testRead() = runBlocking<Unit> {
        TestUtils.replacePaperDbFileBy("recipe_1.5.pt", "recipe")

        val recipe = Paper.book().read<Recipe>("recipe")
        assertThat(recipe).isNotNull
        assertThat(recipe!!.name).isEqualTo("chocolate cake")
    }

    class Recipe {
        internal var ingredients: HashMap<String, Int>? = null
        internal var name: String? = null
        internal var duration: Int = 0
    }
}
