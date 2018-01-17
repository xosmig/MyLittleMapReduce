package com.xosmig.mlmr.computationnode

import com.xosmig.mlmr.MapConfig
import java.rmi.Remote
import java.rmi.RemoteException

interface ResourceManagerRmiForComputationNode: Remote {
    @Throws(RemoteException::class)
    fun done(config: MapConfig)
}

class NodeBusyException(msg: String? = null): Exception(msg)

interface ComputationNodeRmi: Remote {
    @Throws(RemoteException::class, NodeBusyException::class)
    fun startMap(config: MapConfig)
}
