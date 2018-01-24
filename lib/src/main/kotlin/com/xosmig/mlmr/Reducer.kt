package com.xosmig.mlmr

import com.xosmig.mlmr.util.chainAll
import kotlinx.serialization.serializer
import readCBORObject
import java.io.IOException
import java.io.InputStream
import kotlin.reflect.KClass

abstract class Reducer<IK : Any, IV : Any, OK : Any, OV : Any>(
        ikKlass: KClass<IK>, ivKlass: KClass<IV>,
        okKlass: KClass<OK>, ovKlass: KClass<OV>
) : Node<OK, OV>(okKlass, ovKlass) {

    private val keySerializer = ikKlass.serializer()
    private val valueSerializer = ivKlass.serializer()

    abstract fun reduce(key: IK, values: Sequence<IV>, context: NodeContext<OK, OV>)

    fun reduce(input: List<InputStream>, context: Context) {
        val bufferedInput = input.map { it.buffered() }
        val grouped = bufferedInput.groupBy {
            it.readCBORObject(keySerializer) ?: throw IOException("Got an empty file as input for reduce")
        }
        for ((key, streams) in grouped) {
            val valueSequences = streams.map { generateSequence { it.readCBORObject(valueSerializer) } }
            reduce(key, valueSequences.chainAll(), nodeContext(context))
        }
    }
}
