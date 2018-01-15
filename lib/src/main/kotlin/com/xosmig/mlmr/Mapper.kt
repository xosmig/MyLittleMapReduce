package com.xosmig.mlmr

import kotlinx.serialization.serializer
import readCBORObject
import java.io.BufferedInputStream
import java.io.InputStream
import kotlin.reflect.KClass

interface Mapper

abstract class KeyValueMapper<IK: Any, IV: Any, OK: Any, OV: Any> (
        ikKlass: KClass<IK>, ivKlass: KClass<IV>,
        okKlass: KClass<OK>, ovKlass: KClass<OV>): Node<OK, OV>(okKlass, ovKlass), Mapper {

    private val serializer = KVPair.serializer(ikKlass.serializer(), ivKlass.serializer())

    abstract fun map(key: IK, value: IV, context: NodeContext<OK, OV>)

    fun map(input: InputStream, context: NodeContext<OK, OV>) {
        while (true) {
            BufferedInputStream(input).use { bufferedInput ->
                while (true) {
                    val (key, value) = bufferedInput.readCBORObject(serializer) ?: break
                    map(key, value, context)
                }
            }
        }
    }
}

abstract class InputStreamMapper<OK: Any, OV: Any>(
        okKlass: KClass<OK>, ovKlass: KClass<OV>): Node<OK, OV>(okKlass, ovKlass), Mapper {
    abstract fun map(input: InputStream, context: NodeContext<OK, OV>)
}
