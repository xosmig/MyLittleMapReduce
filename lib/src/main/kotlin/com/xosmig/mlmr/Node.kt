package com.xosmig.mlmr

import kotlinx.serialization.KSerializer
import kotlin.reflect.KClass

class NodeContext<OK: Any, OV: Any>(
        private val context: Context,
        private val serializer: KSerializer<KVPair<OK, OV>>) {

    fun output(key: OK, value: OV) {
        context.output(key, value, serializer)
    }
}

abstract class Node<OK: Any, OV: Any>(okKlass: KClass<OK>, ovKlass: KClass<OV>) {
    private val serializer = KVPair.getSerializer(okKlass, ovKlass)

    fun nodeContext(context: Context): NodeContext<OK, OV> = NodeContext(context, serializer)
}
