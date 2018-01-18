package com.xosmig.mlmr

import java.io.Serializable
import java.nio.file.Path

data class JobConfig(
        val mapper: Class<out Node<*, *>>,
        val combiner: Class<out Node<*, *>>? = null,
        val reducer: Class<out Node<*, *>>,
        val inputDir: String,
        val outputDir: String)

class CompiledJobConfig(config: JobConfig): Serializable {
    val mapper = ClassRef(config.mapper)
    val combiner = if (config.combiner != null) { ClassRef(config.combiner) } else { null }
    val reducer = ClassRef(config.reducer)
    val inputDir = config.inputDir
    val outputDir = config.outputDir
}

class ReduceConfig(config: CompiledJobConfig): Serializable {
    val reducer = config.reducer
    val outputDir = config.outputDir
    // TODO?: inputDir?
}

