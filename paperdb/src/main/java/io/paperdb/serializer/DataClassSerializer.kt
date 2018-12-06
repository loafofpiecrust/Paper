package io.paperdb.serializer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.KryoException
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import java.lang.reflect.InvocationTargetException
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor

class DataClassSerializer<T: Any>(val klass: KClass<T>) : Serializer<T>() {
    private val props = klass.memberProperties.sortedBy { it.name }
    private val propsByName = props.associateBy { it.name }
    private val constructor = klass.primaryConstructor!!

    init {
        // Verify that this class is immutable (all properties are final)
        assert(props.none { it is KMutableProperty<*> })
    }

    override fun write(kryo: Kryo, output: Output, obj: T) {
        output.writeVarInt(constructor.parameters.size, true)
        output.writeInt(constructor.parameters.hashCode())
        for (param in constructor.parameters) {
            val kProperty = propsByName[param.name!!]!!
            when ((param.type.classifier as KClass<*>).simpleName) {
                "Int" -> output.writeVarInt(kProperty.get(obj) as Int, true)
                "Long" -> output.writeVarLong(kProperty.get(obj) as Long, true)
                "Short" -> output.writeShort(kProperty.get(obj) as Int)
                "Char" -> output.writeChar(kProperty.get(obj) as Char)
                "Byte" -> output.writeByte(kProperty.get(obj) as Byte)
                "Double" -> output.writeDouble(kProperty.get(obj) as Double)
                "Float" -> output.writeFloat(kProperty.get(obj) as Float)
                else -> try {
                    kryo.writeClassAndObject(output, kProperty.get(obj))
                } catch (e: Exception) {
                    throw IllegalStateException("Failed to serialize ${param.name} in ${klass.qualifiedName}", e)
                }
            }
        }
    }

    override fun read(kryo: Kryo, input: Input, type: Class<T>): T {
        assert(type.kotlin == klass)
//        assert(kryo.isRegistrationRequired)
        val numFields = input.readVarInt(true)
        val fieldTypeHash = input.readInt()

        // A few quick checks for data evolution. Note that this is not guaranteed to catch every problem! But it's
        // good enough for a prototype.
        if (numFields != constructor.parameters.size)
            throw KryoException("Mismatch between number of constructor parameters and number of serialised fields for ${klass.qualifiedName} ($numFields vs ${constructor.parameters.size})")
        if (fieldTypeHash != constructor.parameters.hashCode())
            throw KryoException("Hashcode mismatch for parameter types for ${klass.qualifiedName}: unsupported type evolution has happened.")

        val args = arrayOfNulls<Any?>(numFields)
        var cursor = 0
        for (param in constructor.parameters) {
            args[cursor++] = when ((param.type.classifier as KClass<*>).simpleName) {
                "Int" -> input.readVarInt(true)
                "Long" -> input.readVarLong(true)
                "Short" -> input.readShort()
                "Char" -> input.readChar()
                "Byte" -> input.readByte()
                "Double" -> input.readDouble()
                "Float" -> input.readFloat()
                else -> kryo.readClassAndObject(input)
            }
        }
        // If the constructor throws an exception, pass it through instead of wrapping it.
        return try { constructor.call(*args) } catch (e: InvocationTargetException) { throw e.cause!! }
    }
}
