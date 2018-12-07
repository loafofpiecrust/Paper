package io.paperdb.testdata

import java.util.Arrays

// Person + arg constructor
class PersonArg(extraData: String) : Person() {
    init {
        name = "changed$extraData"
    }

    override fun equals(o: Any?): Boolean {
        if (this === o) return true
        if (o == null || PersonArg::class.java != o.javaClass) return false

        val person = o as PersonArg?

        if (age != person!!.age) return false
        if (!Arrays.equals(bikes, person.bikes)) return false
        if (if (name != null) name != person.name else person.name != null)
            return false

        return if (if (phoneNumbers != null)
                phoneNumbers != person.phoneNumbers
            else
                person.phoneNumbers != null) false else true

    }

    override fun hashCode(): Int {
        return super.hashCode()
    }

    override fun toString(): String {
        return "PersonArg($name)"
    }
}
