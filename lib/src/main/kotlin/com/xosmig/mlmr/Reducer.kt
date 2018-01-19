package com.xosmig.mlmr

import kotlinx.serialization.serializer
import java.io.InputStream
import kotlin.reflect.KClass

interface Reducer {
    fun reduce(input: InputStream, context: Context)
}

abstract class TypedReducer<IK: Any, IV: Any, OK: Any, OV: Any>(
        ikKlass: KClass<IK>, ivKlass: KClass<IV>,
        okKlass: KClass<OK>, ovKlass: KClass<OV>
): Node<OK, OV>(okKlass, ovKlass), Reducer {

    private val serializer = KVPair.getSerializer(ikKlass, ivKlass)

    abstract fun reduce(key: IK, values: Iterable<IV>, context: NodeContext<OK, OV>)

    fun reduce(key: IK, values: Iterable<IV>, context: Context) = reduce(key, values, nodeContext(context))

    override final fun reduce(input: InputStream, context: Context) {
//        BufferedInputStream(input).use { bufferedInput ->
//            while (true) {
//                val kv = bufferedInput.readCBORObject(serializer) ?: break
//                reduce(kv.key, kv.value, context)
//            }
//        }
    }
}
