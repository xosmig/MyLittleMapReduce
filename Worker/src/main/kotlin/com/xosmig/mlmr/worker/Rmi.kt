package com.xosmig.mlmr.worker

import com.xosmig.mlmr.ClassRef
import com.xosmig.mlmr.WorkerId
import java.io.Serializable
import java.rmi.Remote
import java.rmi.RemoteException
import java.util.*

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

class ReduceTask(val reducer: ClassRef,
                 val reduceInputDirs: Array<String>,
                 override val outputDir: String): WorkerTask(), Serializable {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ReduceTask) return false

        if (reducer != other.reducer) return false
        if (!Arrays.equals(reduceInputDirs, other.reduceInputDirs)) return false
        if (outputDir != other.outputDir) return false

        return true
    }

    override fun hashCode(): Int {
        var result = reducer.hashCode()
        result = 31 * result + Arrays.hashCode(reduceInputDirs)
        result = 31 * result + outputDir.hashCode()
        return result
    }

    override fun toString(): String {
        return "ReduceTask(reducer=$reducer, reduceInputDirs=${Arrays.toString(reduceInputDirs)}, outputDir='$outputDir')"
    }
}
