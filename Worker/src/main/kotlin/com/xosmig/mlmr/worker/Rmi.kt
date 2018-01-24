package com.xosmig.mlmr.worker

import com.xosmig.mlmr.ClassRef
import com.xosmig.mlmr.WorkerId
import java.io.Serializable
import java.rmi.Remote
import java.rmi.RemoteException
import java.util.*

interface WorkersManagerRmi : Remote {
    /**
     * @return Config for the task or null, if the are no tasks.
     * The MapNode should die in this case.
     */
    @Throws(RemoteException::class)
    fun registerWorker(id: WorkerId, workerRmi: WorkerRmi): WorkerTask?

    @Throws(RemoteException::class)
    fun workerFinished(id: WorkerId)
}


interface WorkerRmi : Remote {
    @Throws(RemoteException::class)
    fun healthCheck()
}


sealed class WorkerTask : Serializable

data class MapTask(val mapper: ClassRef,
                   val mapInputPath: String,
                   val mapOutputDir: String,
                   val groupCnt: Int) : WorkerTask(), Serializable

class ReduceTask(val reducer: ClassRef,
                 val reduceInputDir: String,
                 val outputDir: String) : WorkerTask(), Serializable
