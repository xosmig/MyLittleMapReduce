package com.xosmig.mlmr

import kotlin.reflect.KClass

data class JobConfig(
        val mapper: KClass<out Node<*, *>>,
        val combiner: KClass<out Node<*, *>>? = null,
        val reducer: KClass<out Node<*, *>>) {
}
