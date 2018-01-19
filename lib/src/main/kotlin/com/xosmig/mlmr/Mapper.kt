package com.xosmig.mlmr

import java.io.InputStream
import kotlin.reflect.KClass

interface Mapper {
    fun map(input: InputStream, context: Context)
}

abstract class TypedMapper<OK: Any, OV: Any>(okKlass: KClass<OK>,
                                             ovKlass: KClass<OV>): Node<OK, OV>(okKlass, ovKlass), Mapper {

    abstract fun map(input: InputStream, context: NodeContext<OK, OV>)

    override fun map(input: InputStream, context: Context) = map(input, nodeContext(context))
}
