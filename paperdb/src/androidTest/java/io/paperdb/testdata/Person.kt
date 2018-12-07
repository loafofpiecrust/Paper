package io.paperdb.testdata

import java.util.Arrays

open class Person {
    open var name: String? = null
    var age: Int = 0
    var phoneNumbers: List<PhoneNumber>? = null
    var bikes: Array<String>? = null

    override fun equals(o: Any?): Boolean {
        if (this === o) return true
        if (o == null || o !is Person) return false

        return phoneNumbers == o.phoneNumbers && name == o.name && age == o.age
    }

    override fun hashCode(): Int {
        var result = if (name != null) name!!.hashCode() else 0
        result = 31 * result + age
        result = 31 * result + if (phoneNumbers != null) phoneNumbers!!.hashCode() else 0
        result = 31 * result + if (bikes != null) Arrays.hashCode(bikes) else 0
        return result
    }
}
