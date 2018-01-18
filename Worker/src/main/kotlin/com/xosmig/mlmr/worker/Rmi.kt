package com.xosmig.mlmr.worker

import com.xosmig.mlmr.ClassRef
import com.xosmig.mlmr.WorkerId
import java.io.Serializable
import java.rmi.Remote
import java.rmi.RemoteException

interface ResourceManagerRmiForWorker {
    @Throws(RemoteException::class)
    fun workersManager(): WorkersManagerRmi
}

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
    fun check(): WorkerStatus
}


sealed class WorkerTask

data class MapTask(val mapper: ClassRef,
                   val combiner: ClassRef?,
                   val mapInputPath: String,
                   val mapOutputDir: String): WorkerTask(), Serializable

data class ReduceTask(val reducer: ClassRef,
                      val reduceInputDir: String,
                      val reduceOutputDir: String): WorkerTask(), Serializable


/**
 * @property[progress] number between 0 and 1. Progress of the task.
 */
data class WorkerStatus(val progress: Float)
