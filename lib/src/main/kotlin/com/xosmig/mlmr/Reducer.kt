package com.xosmig.mlmr

import kotlinx.serialization.serializer
import readCBORObject
import java.io.BufferedInputStream
import java.io.InputStream
import kotlin.reflect.KClass

abstract class Reducer<IK: Any, IV: Any, OK: Any, OV: Any>(
        ikKlass: KClass<IK>, ivKlass: KClass<IV>,
        okKlass: KClass<OK>, ovKlass: KClass<OV>
): Node<OK, OV>(okKlass, ovKlass) {

    private val keySerializer = ikKlass.serializer()
    private val valueSerializer = ivKlass.serializer()

    abstract fun reduce(key: IK, values: Sequence<IV>, context: NodeContext<OK, OV>)

    fun reduce(input: InputStream, context: Context) {
        BufferedInputStream(input).use { bufferedInput ->
            val key = bufferedInput.readCBORObject(keySerializer) ?: TODO() // TODO

            val valueIterator = object: Iterator<IV> {
                private var next: IV? = bufferedInput.readCBORObject(valueSerializer)

                override fun hasNext(): Boolean = next != null
                override fun next(): IV {
                    val ret = next ?: throw NoSuchElementException("Trying to read an object from an empty stream")
                    next = bufferedInput.readCBORObject(valueSerializer)
                    return ret
                }
            }

            reduce(key, valueIterator.asSequence(), nodeContext(context))
        }
    }
}
