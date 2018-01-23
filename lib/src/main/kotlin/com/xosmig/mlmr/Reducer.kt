package com.xosmig.mlmr

import com.xosmig.mlmr.util.chainAll
import kotlinx.serialization.serializer
import readCBORObject
import java.io.InputStream
import kotlin.reflect.KClass

abstract class Reducer<IK: Any, IV: Any, OK: Any, OV: Any>(
        ikKlass: KClass<IK>, ivKlass: KClass<IV>,
        okKlass: KClass<OK>, ovKlass: KClass<OV>
): Node<OK, OV>(okKlass, ovKlass) {

    private val keySerializer = ikKlass.serializer()
    private val valueSerializer = ivKlass.serializer()

    abstract fun reduce(key: IK, values: Sequence<IV>, context: NodeContext<OK, OV>)

    fun reduce(input: List<InputStream>, context: Context) {
        val bufferedInput = input.map { it.buffered() }
        for ((key, streams) in bufferedInput.groupBy { it.readCBORObject(keySerializer) ?: TODO() /*TODO*/ }) {
            val valueSequences = streams.map { generateSequence { it.readCBORObject(valueSerializer) } }
            reduce(key, valueSequences.chainAll(), nodeContext(context))
        }
    }
}
