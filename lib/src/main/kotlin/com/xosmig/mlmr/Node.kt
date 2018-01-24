package com.xosmig.mlmr

import kotlinx.serialization.KSerializer
import kotlinx.serialization.serializer
import kotlin.reflect.KClass

class NodeContext<OK : Any, OV : Any>(
        private val context: Context,
        private val keySerializer: KSerializer<OK>,
        private val valueSerializer: KSerializer<OV>) {

    fun output(key: OK, value: OV) {
        context.output(key, value, keySerializer, valueSerializer)
    }
}

abstract class Node<OK : Any, OV : Any>(okKlass: KClass<OK>, ovKlass: KClass<OV>) {
    private val keySerializer = okKlass.serializer()
    private val valueSerializer = ovKlass.serializer()

    fun nodeContext(context: Context): NodeContext<OK, OV> = NodeContext(context, keySerializer, valueSerializer)
}
