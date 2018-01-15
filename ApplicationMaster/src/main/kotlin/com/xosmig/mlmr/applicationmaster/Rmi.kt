package com.xosmig.mlmr.applicationmaster

import com.xosmig.mlmr.ClassRef
import java.io.Serializable
import java.rmi.Remote
import java.rmi.RemoteException

class CompiledJobConfig(config: JobConfig): Serializable {
    val mapper = ClassRef(config.mapper)
    val combiner = if (config.combiner != null) { ClassRef(config.combiner) } else { null }
    val reducer = ClassRef(config.reducer)
    val inputDir = config.inputDir
    val outputDir = config.outputDir
}

interface AM2RMRmi: Remote {
    @Throws(RemoteException::class)
    fun startJob(config: CompiledJobConfig): Int
}

interface RM2AMRmi: Remote {
    @Throws(RemoteException::class)
    fun kill(reason: String?)

    @Throws(RemoteException::class)
    fun taskComplete(id: Int)
}
