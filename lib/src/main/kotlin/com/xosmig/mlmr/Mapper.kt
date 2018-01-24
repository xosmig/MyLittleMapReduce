package com.xosmig.mlmr

import java.io.InputStream
import kotlin.reflect.KClass

abstract class Mapper<OK : Any, OV : Any>(okKlass: KClass<OK>, ovKlass: KClass<OV>) : Node<OK, OV>(okKlass, ovKlass) {

    abstract fun map(input: InputStream, context: NodeContext<OK, OV>)

    fun map(input: InputStream, context: Context) = map(input, nodeContext(context))
}
