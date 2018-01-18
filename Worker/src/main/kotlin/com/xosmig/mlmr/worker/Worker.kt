package com.xosmig.mlmr.worker

import com.xosmig.mlmr.*
import java.nio.file.Files
import java.nio.file.Paths
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject

class Worker(registryHost: String, registryPort: Int, val id: WorkerId): WorkerRmi {
    private val resourceManager = run {
        val registry = LocateRegistry.getRegistry(registryHost, registryPort)
        registry.lookup(RM_REGISTRY_KEY) as ResourceManagerRmiForWorker
    }
    private val workersManager = resourceManager.workersManager()

    private val stub by lazy { UnicastRemoteObject.exportObject(this, 0) as WorkerRmi }

    fun run(): Int {
        val task = workersManager.registerWorker(id, stub)
        if (task == null) {
            System.err.println("The task is not found")
            return 3
        }

        when (task) {
            is MapTask -> {
                Files.newInputStream(Paths.get(task.mapInputPath)).use { input ->
                    val mapper = task.mapper.load().newInstance() as Mapper
                    // TODO: Adequate context
                    mapper.map(input, StdoutContext())
                }
            }
            is ReduceTask -> {
                // TODO: doReduce(task)
            }
        }
        workersManager.workerFinished(id, true)
        return 0
    }

    override fun check(): WorkerStatus = WorkerStatus(0.5f)  // TODO
}
