package com.xosmig.mlmr

import kotlinx.serialization.serializer
import readCBORObject
import java.io.BufferedInputStream
import java.io.InputStream
import kotlin.reflect.KClass

interface MapperContext<OK, OV> {
    fun output(key: OK, value: OV)
}

sealed class MapperBase<OK, OV> {
    abstract fun map(input: InputStream, context: MapperContext<OK, OV>)
}

abstract class KeyValueMapper<IK: Any, IV: Any, OK: Any, OV: Any>
        (private val klKey: KClass<IK>, private val klVal: KClass<IV>): MapperBase<OK, OV>() {

    abstract fun map(key: IK, value: IV, context: MapperContext<OK, OV>)

    override fun map(input: InputStream, context: MapperContext<OK, OV>) {
        while (true) {
            val serializer = KVPair.serializer(klKey.serializer(), klVal.serializer())
            BufferedInputStream(input).use { bufferedInput ->
                while (true) {
                    val (key, value) = bufferedInput.readCBORObject(serializer) ?: break
                    map(key, value, context)
                }
            }
        }
    }
}

abstract class InputStreamMapper<OK, OV>: MapperBase<OK, OV>()
