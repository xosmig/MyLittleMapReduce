package com.xosmig.mlmr

import kotlinx.serialization.KSerializer

class Context {
    fun<K, V> output(key: K, value: V, serializer: KSerializer<KVPair<K, V>>) {
        TODO()
    }
}
