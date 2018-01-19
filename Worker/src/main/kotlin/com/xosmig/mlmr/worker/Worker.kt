package com.xosmig.mlmr.worker

import com.xosmig.mlmr.*
import java.nio.file.Files
import java.nio.file.Paths
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject
import java.util.concurrent.TimeUnit
import java.util.logging.Level.INFO
import java.util.logging.Level.WARNING
import java.util.logging.Logger

internal class Worker(registryHost: String, registryPort: Int, val workerId: WorkerId): WorkerRmi {
    private val workersManager: WorkersManagerRmi

    init {
        @Suppress("NAME_SHADOWING")
        val registryHost =  if (registryHost == "localhost") { null } else { registryHost }
        val registry = LocateRegistry.getRegistry(registryHost, registryPort)
        workersManager = registry.lookup(WM_REGISTRY_KEY) as WorkersManagerRmi
    }

    private val logger = Logger.getLogger(Worker::class.java.name)
    private val stub by lazy { UnicastRemoteObject.exportObject(this, 0) as WorkerRmi }

    fun run(): Int {
        Logger.getLogger(Main.javaClass.name).log(INFO, "Registering...")
        val task = workersManager.registerWorker(workerId, stub)
        if (task == null) {
            System.err.println("The task is not found")
            return 3
        }
        logger.log(INFO, "Registered")

        when (task) {
            is MapTask -> {
                val mapper = task.mapper.load().newInstance() as Mapper
                logger.log(INFO, "Starting map task for file '${task.mapInputPath}' ...")
                Files.newInputStream(Paths.get(task.mapInputPath)).use { input ->
                    WorkerContext(workerId, Paths.get(task.outputDir)).use { context ->
                        mapper.map(input, context)
                    }
                }
            }
            is ReduceTask -> {
                // TODO: doReduce(task)
            }
        }
        workersManager.workerFinished(workerId, true)

        logger.log(INFO, "Successfully finished")
        // Expect resource manager to kill this process within 10 seconds
        Thread.sleep(TimeUnit.SECONDS.toMillis(10))

        logger.log(WARNING, "Seems that resource manager has failed to kill this process")
        return 0
    }

    override fun healthCheck() {}
}
