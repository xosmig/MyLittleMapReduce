package com.xosmig.mlmr

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import org.junit.Test
import readCBORObject
import writeCBORObject
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream


class SerializationTest {
    @Test
    fun serializeKVPair() {
        val original = KVPair(SInt(5), SString("hello"))
        val serializer = KVPair.getSerializer(SInt::class, SString::class)
        val encoded = ByteArrayOutputStream().use { bytesOS ->
            bytesOS.writeCBORObject(original, serializer)
            bytesOS.toByteArray()
        }

        val decoded = ByteArrayInputStream(encoded).use { bytesIS ->
            bytesIS.readCBORObject(serializer)
        }
        assert.that(decoded, equalTo(original))
    }
}
