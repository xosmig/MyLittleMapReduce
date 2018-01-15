package com.xosmig.mlmr.applicationmaster

import com.xosmig.mlmr.Node

data class JobConfig(
        val mapper: Class<out Node<*, *>>,
        val combiner: Class<out Node<*, *>>? = null,
        val reducer: Class<out Node<*, *>>,
        val inputDir: String,
        val outputDir: String)
