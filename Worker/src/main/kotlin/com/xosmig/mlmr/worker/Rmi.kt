package com.xosmig.mlmr.worker

import com.xosmig.mlmr.ClassRef
import com.xosmig.mlmr.WorkerId
import java.io.Serializable
import java.rmi.Remote
import java.rmi.RemoteException

interface WorkersManagerRmi: Remote {
    /**
     * @return Config for the task or null, if the are no tasks.
     * The MapNode should die in this case.
     */
    @Throws(RemoteException::class)
    fun registerWorker(id: WorkerId, workerRmi: WorkerRmi): WorkerTask?

    @Throws(RemoteException::class)
    fun workerFinished(id: WorkerId, success: Boolean)
}


interface WorkerRmi : Remote {
    @Throws(RemoteException::class)
    fun healthCheck()
}


sealed class WorkerTask: Serializable {
    abstract val outputDir: String
}

data class MapTask(val mapper: ClassRef,
                   val combiner: ClassRef?,
                   val mapInputPath: String,
                   override val outputDir: String): WorkerTask(), Serializable

data class ReduceTask(val reducer: ClassRef,
                      val reduceInputDir: String,
                      override val outputDir: String): WorkerTask(), Serializable
