package io.paperdb

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.KryoException
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer
import de.javakaffee.kryoserializers.*
import org.objenesis.strategy.StdInstantiatorStrategy
import java.io.ByteArrayOutputStream
import java.lang.reflect.Type
import java.util.*

interface PaperSerializer {
    suspend fun <T> serialize(value: T?, typeToken: Type?): ByteArray
    suspend fun <T> deserialize(bytes: ByteArray, typeToken: Type?): T
}


class KryoSerializer(
    private val customSerializers: SerializerMap,
    private val registrations: RegistrationMap
): PaperSerializer {
    private val kryo = SuspendObjectPool(4) {
        createKryoInstance(false)
    }

    private fun createKryoInstance(compatibilityMode: Boolean): Kryo {
        val kryo = Kryo()

        if (compatibilityMode) {
            kryo.fieldSerializerConfig.isOptimizedGenerics = true
        }

        kryo.references = false
        kryo.setDefaultSerializer(CompatibleFieldSerializer::class.java)

        // Serialize Arrays$ArrayList

        UnmodifiableCollectionsSerializer.registerSerializers(kryo)
        SynchronizedCollectionsSerializer.registerSerializers(kryo)
        SubListSerializers.addDefaultSerializers(kryo)

        for ((clazz, serializerClass) in customSerializers) {
            kryo.addDefaultSerializer(clazz, serializerClass)
        }

        // To keep backward compatibility don't change the order of serializers below!
        kryo.register(Arrays.asList("").javaClass, ArraysAsListSerializer())

        // UUID support
        kryo.register(UUID::class.java, UUIDSerializer())

        for ((clazz, reg) in registrations) {
            if (reg.serializer != null) {
                kryo.register(clazz, reg.serializer, reg.id)
            } else {
                kryo.register(clazz, reg.id)
            }
        }

        kryo.instantiatorStrategy = Kryo.DefaultInstantiatorStrategy(StdInstantiatorStrategy())

        return kryo
    }

    override suspend fun <T> deserialize(bytes: ByteArray, typeToken: Type?): T {
        return Input(bytes).use { input ->
            try {
                this.kryo.use { it.readClassAndObject(input) } as T
            } catch (e: KryoException) {
                throw PaperDbException("Kryo failed to read object", e)
            }
        }
    }

    override suspend fun <T> serialize(value: T?, typeToken: Type?): ByteArray {
        val bao = ByteArrayOutputStream()
        val kryoOutput = Output(bao)
        kryo.use { it.writeClassAndObject(kryoOutput, value) }
        kryoOutput.flush()
        val bytes = bao.toByteArray()
        kryoOutput.close()
        return bytes
    }
}