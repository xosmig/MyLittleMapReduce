package com.xosmig.mlmr.mapnode

import com.xosmig.mlmr.MapConfig
import com.xosmig.mlmr.WorkerId
import java.rmi.Remote
import java.rmi.RemoteException

interface ResourceManagerRmiForMapNode: Remote {
    /**
     * @return Config for the task or null, if the are no tasks.
     * The MapNode should die in this case.
     */
    @Throws(RemoteException::class)
    fun getTask(id: WorkerId, stub: MapNodeRmi): MapConfig?
}

/**
 * @property[progress] number between 0 and 1. Progress of the task.
 */
data class Status(val progress: Float,
                  val finished: Boolean)

interface MapNodeRmi: Remote {
    @Throws(RemoteException::class)
    fun check(): Status
}
