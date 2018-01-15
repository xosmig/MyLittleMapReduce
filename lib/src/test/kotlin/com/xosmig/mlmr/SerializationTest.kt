package com.xosmig.mlmr

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import kotlinx.serialization.KSerializer
import org.junit.Test
import readCBORObject
import writeCBORObject
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream


class SerializationTest {
    private fun<T: Any> testSerializable(obj: T, serializer: KSerializer<T>) {
        val encoded = ByteArrayOutputStream().use { bytesOS ->
            bytesOS.writeCBORObject(obj, serializer)
            bytesOS.toByteArray()
        }

        val decoded = ByteArrayInputStream(encoded).use { bytesIS ->
            bytesIS.readCBORObject(serializer)
        }
        assert.that(decoded, equalTo(obj))
    }

    @Test
    fun serializeKVPair() {
        val original = KVPair(SInt(5), SString("hello"))
        val serializer = KVPair.getSerializer(SInt::class, SString::class)
        testSerializable(original, serializer)
    }
}
