package io.paperdb.testdata

/**
 * Uses to test read/write generics
 */
class PhoneNumber(var phoneNumber: String?) {

    override fun equals(o: Any?): Boolean {
        if (this === o) return true
        if (o == null || javaClass != o.javaClass) return false

        val that = o as PhoneNumber?

        return if (phoneNumber != null) phoneNumber == that!!.phoneNumber else that!!.phoneNumber == null
    }

    override fun hashCode(): Int {
        return if (phoneNumber != null) phoneNumber!!.hashCode() else 0
    }
}
