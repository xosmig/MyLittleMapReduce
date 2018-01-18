package com.xosmig.mlmr.worker

import com.xosmig.mlmr.*
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject

class Worker(registryHost: String, registryPort: Int, val id: WorkerId): WorkerRmi {
    private val resourceManager = run {
        val registry = LocateRegistry.getRegistry(registryHost, registryPort)
        registry.lookup(RM_REGISTRY_KEY) as ResourceManagerRmiForWorker
    }

    private val stub by lazy { UnicastRemoteObject.exportObject(this, 0) as WorkerRmi }

    fun run(): Int {
        val task = resourceManager.registerWorker(id, stub)
        if (task == null) {
            System.err.println("The task is not found")
            return 3
        }

        when (task) {
            is MapTask -> {
                // TODO: doMap(task)
            }
            is ReduceTask -> {
                // TODO: doReduce(task)
            }
        }

        TODO()
    }

    override fun check(): WorkerStatus {
        TODO()
    }
}
