package io.paperdb.testdata

import java.util.ArrayList
import java.util.HashMap

object TestDataGenerator {
    fun genPersonList(size: Int): List<Person> {
        val list = ArrayList<Person>()
        for (i in 0 until size) {
            val p = genPerson(Person(), i)
            list.add(p)
        }
        return list
    }

    fun genPersonArgList(size: Int): List<PersonArg> {
        val list = ArrayList<PersonArg>()
        for (i in 0 until size) {
            val p = genPerson(PersonArg("name"), i)
            list.add(p)
        }
        return list
    }

    fun <T : Person> genPerson(p: T, i: Int): T {
        p.age = i
        p.bikes = arrayOf("Kellys gen#$i", "Trek gen#$i")
        p.phoneNumbers = listOf(
            PhoneNumber("0-KEEP-CALM$i"),
            PhoneNumber("0-USE-PAPER$i")
        )
        return p
    }

    fun genPersonMap(size: Int): Map<Int, Person> {
        val map = HashMap<Int, Person>()
        var i = 0
        for (person in genPersonList(size)) {
            map[i++] = person
        }
        return map
    }
}
