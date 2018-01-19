package com.xosmig.mlmr

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.serializer
import kotlin.reflect.KClass

@Serializable
data class KVPair<K, V>(val key: K, val value: V) {
    companion object {
        fun<K: Any, V: Any> getSerializer(klKey: KClass<K>, klVal: KClass<V>): KSerializer<KVPair<K, V>> {
            // Trying to call `KVPair.serializer` found directly from a test causes an exception.
            // Thus, have to wrap it in a usual method.
            // Probably, the reason is a bug in the serialization plugin.
            return KVPair.serializer(klKey.serializer(), klVal.serializer())
        }
    }
}


// Kotlin serialization doesn't seem to work with strings and primitive types
// Thus, have to wrap them in data classes

@Serializable
data class SInt(val value: Int) {
    companion object {
        val zero = SInt(0)
        val one = SInt(1)
    }

    override fun toString(): String = value.toString()

}

@Serializable
data class SString(val value: String) {
    override fun toString(): String = value
}
