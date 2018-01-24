package com.xosmig.mlmr

import kotlinx.serialization.KSerializer

interface Context {
    fun <K : Any, V : Any> output(key: K, value: V, keySerializer: KSerializer<K>, valueSerializer: KSerializer<V>)
}
