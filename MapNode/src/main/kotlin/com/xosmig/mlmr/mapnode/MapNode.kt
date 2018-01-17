package com.xosmig.mlmr.mapnode

import com.xosmig.mlmr.RM_REGISTRY_KEY
import com.xosmig.mlmr.WorkerId
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject

class MapNode(registryHost: String, registryPort: Int, val id: WorkerId): MapNodeRmi {
    private val resourceManager = run {
        val registry = LocateRegistry.getRegistry(registryHost, registryPort)
        registry.lookup(RM_REGISTRY_KEY) as ResourceManagerRmiForMapNode
    }

    private val stub by lazy { UnicastRemoteObject.exportObject(this, 0) as MapNodeRmi }

    fun run() {
        val task = resourceManager.getTask(id, stub)

        TODO()
    }

    override fun check(): Status {
        TODO()
    }
}
