package com.xosmig.mlmr.resourcemanager

import com.xosmig.mlmr.DEFAULT_REGISTRY_PORT
import com.xosmig.mlmr.RM_REGISTRY_KEY
import com.xosmig.mlmr.applicationmaster.AM2RMRmi
import com.xosmig.mlmr.applicationmaster.CompiledJobConfig
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject

class ResourceManager: AM2RMRmi {
    override fun startJob(config: CompiledJobConfig): Int {
        TODO()
    }

    fun run(port: Int = DEFAULT_REGISTRY_PORT): Int {
        // Make the resource manager visible
        val registry = LocateRegistry.createRegistry(port)
        val stub = UnicastRemoteObject.exportObject(this, 0)
        registry.bind(RM_REGISTRY_KEY, stub)

        TODO()
    }
}
