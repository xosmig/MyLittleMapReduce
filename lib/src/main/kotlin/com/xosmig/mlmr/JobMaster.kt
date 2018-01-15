package com.xosmig.mlmr

data class CompiledJobConfig(
        val jarPath: String,
        val mapper: String,
        val combiner: String,
        val reducer: String)

class JobMaster(val config: JobConfig) {

    fun run(): Int {
        TODO()
    }

    fun runAndExit() {
        System.exit(run())
    }
}
