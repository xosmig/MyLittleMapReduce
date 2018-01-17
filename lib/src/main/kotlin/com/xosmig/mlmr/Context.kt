package com.xosmig.mlmr

import kotlinx.serialization.KSerializer

interface Context {
    fun<K, V> output(key: K, value: V, serializer: KSerializer<KVPair<K, V>>)
}

// TODO: remove?
class StdoutContext: Context {
    override fun <K, V> output(key: K, value: V, serializer: KSerializer<KVPair<K, V>>) {
        println("OUTPUT: $key, $value")
    }
}
