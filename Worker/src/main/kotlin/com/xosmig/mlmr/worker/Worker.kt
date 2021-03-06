package com.xosmig.mlmr.worker

import com.xosmig.mlmr.*
import com.xosmig.mlmr.util.using
import java.nio.file.Files
import java.nio.file.Paths
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject
import java.util.concurrent.TimeUnit
import java.util.logging.Level.*
import java.util.logging.Logger

internal class Worker(registryHost: String, registryPort: Int, val workerId: WorkerId) : WorkerRmi {
    private val workersManager: WorkersManagerRmi

    init {
        val registry = LocateRegistry.getRegistry(registryHost.takeUnless { registryHost == "localhost" }, registryPort)
        workersManager = registry.lookup(WM_REGISTRY_KEY) as WorkersManagerRmi
    }

    private val stub by lazy { UnicastRemoteObject.exportObject(this, 0) as WorkerRmi }

    fun run(): Int {
        Logger.getLogger(Main.javaClass.name).log(INFO, "Registering...")
        val task = workersManager.registerWorker(workerId, stub)
        if (task == null) {
            System.err.println("The task is not found")
            return 3
        }
        LOGGER.log(INFO, "Registered")

        when (task) {
            is MapTask -> {
                val mapper = task.mapper.load().newInstance() as Mapper<*, *>
                LOGGER.log(INFO, "Starting map task for file '${task.mapInputPath}' ...")
                using {
                    val input = Files.newInputStream(Paths.get(task.mapInputPath)).autoClose()
                    val context = WorkerContext(workerId, Paths.get(task.mapOutputDir), task.groupCnt,
                            CBORStreamSerializer).autoClose()
                    mapper.map(input, context)
                }
            }
            is ReduceTask -> {
                val reducer = task.reducer.load().newInstance() as Reducer<*, *, *, *>
                LOGGER.log(FINE, "Starting reduce task for directory '${task.reduceInputDir}'")
                using {
                    val files = Files.newDirectoryStream(Paths.get(task.reduceInputDir)).autoClose()
                    val inputStreams = files.map { Files.newInputStream(it).autoClose() }
                    val context = WorkerContext(workerId, Paths.get(task.outputDir),/*groupCnt=*/0,
                            JSONStreamSerializer).autoClose()
                    reducer.reduce(inputStreams, context)
                }
            }
        }
        workersManager.workerFinished(workerId)

        LOGGER.log(INFO, "Successfully finished")
        // Expect resource manager to kill this process within 10 seconds
        Thread.sleep(TimeUnit.SECONDS.toMillis(10))

        LOGGER.log(WARNING, "Seems that resource manager has failed to kill this process")
        return 0
    }

    override fun healthCheck() {}

    companion object {
        private val LOGGER = Logger.getLogger(Worker::class.java.name)
    }
}
