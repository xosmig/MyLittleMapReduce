package com.xosmig.mlmr

import java.io.Serializable

class JobConfig private constructor(
        val mapper: Class<out Node<*, *>>,
        val combiner: Class<out Node<*, *>>?,
        val reducer: Class<out Node<*, *>>,
        val inputDir: String,
        val outputDir: String) {

    companion object {

        fun<K1: Any, V1: Any, K2: Any, V2: Any, K3: Any, V3: Any> create(
                mapper: Class<out Mapper<K1, V1>>,
                combiner: Class<out Reducer<K1, V1, K2, V2>>,
                reducer: Class<out Reducer<K2, V2, K3, V3>>,
                inputDir: String,
                outputDir: String
        ): JobConfig = JobConfig(mapper, combiner, reducer, inputDir, outputDir)

        fun<K1: Any, V1: Any, K3: Any, V3: Any> create(
                mapper: Class<out Mapper<K1, V1>>,
                reducer: Class<out Reducer<K1, V1, K3, V3>>,
                inputDir: String,
                outputDir: String
        ): JobConfig = JobConfig(mapper, null, reducer, inputDir, outputDir)
    }
}

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

