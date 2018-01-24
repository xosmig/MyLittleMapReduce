package com.xosmig.mlmr

import java.io.Serializable

class JobConfig private constructor(
        val mapper: Class<out Node<*, *>>,
        val reducer: Class<out Node<*, *>>,
        val inputDir: String,
        val outputDir: String) {

    companion object {
        fun <K1 : Any, V1 : Any, K3 : Any, V3 : Any> create(
                mapper: Class<out Mapper<K1, V1>>,
                reducer: Class<out Reducer<K1, V1, K3, V3>>,
                inputDir: String,
                outputDir: String
        ): JobConfig = JobConfig(mapper, reducer, inputDir, outputDir)
    }
}

class CompiledJobConfig(config: JobConfig) : Serializable {
    val mapper = ClassRef(config.mapper)
    val reducer = ClassRef(config.reducer)
    val inputDir = config.inputDir
    val outputDir = config.outputDir
}
