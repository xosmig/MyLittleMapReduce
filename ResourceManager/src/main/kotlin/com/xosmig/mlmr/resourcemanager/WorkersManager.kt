package com.xosmig.mlmr.resourcemanager

import com.xosmig.mlmr.WorkerId
import com.xosmig.mlmr.util.startThread
import com.xosmig.mlmr.util.withDefer
import com.xosmig.mlmr.worker.*
import kotlinx.serialization.json.JSON
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.rmi.RemoteException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean

internal class WorkersManager(val registryHost: String, val registryPort: Int): WorkersManagerRmi {
    private var workerCounter = 0
    private var maxWorkerCnt = 10
    private val workerCounterLock = Object()

    private val idGenerator = WorkerId.Generator()
    private val workers = ConcurrentHashMap<WorkerId, WorkerState>()

    override fun registerWorker(id: WorkerId, workerRmi: WorkerRmi): WorkerTask? {
        println("Worker $id is here for registration")
        val workerState = workers[id] ?: return null
        synchronized(workerState.lock) {
            // note that the worker might have already been destroyed
            workerState.registered = true
            workerState.workerRmi = workerRmi
            workerState.lock.notify()
            println("Worker $id has been registered")
            return workerState.task
        }
    }

    override fun workerFinished(id: WorkerId) {
        val workerState = workers[id] ?: return
        println("Worker $id has finished successfully")
        workerState.finished.set(true)
    }

    /**
     * Blocks until there is a place for a new worker.
     */
    fun startMapWorker(job: JobState, inputFile: Path) {
        startWorker { runMapWorker(job, inputFile) }
    }

    /**
     * Blocks until there is a place for a new worker.
     */
    fun startReduceWorker(job: JobState, reduceInputDirs: List<Path>) {
        startWorker { runReduceWorker(job, reduceInputDirs) }
    }

    inline private fun startWorker(crossinline run: () -> Unit) {
        getPlaceForNewWorker()
        startThread {
            var exception: Exception? = null
            for (i in 1..5) {
                try {
                    run()
                } catch (e: Exception) {
                    exception = e
                }

                if (exception == null) {
                    break
                }
            }

            if (exception != null) {
                throw exception
            }
        }
    }

    @Throws(Exception::class)
    private fun runMapWorker(job: JobState, inputPath: Path) {
        val workerId = idGenerator.next()
        println("Creating map worker $workerId for file: '$inputPath'")

        val mapOutputDir = job.tmpDir.resolve("map.output")
        val logFile = job.tmpDir.resolve("map.log").resolve("Worker$workerId")
        Files.createDirectories(mapOutputDir)
        Files.createDirectories(logFile.parent)

        val workerState = WorkerState(workerId, MapTask(
                mapper = job.config.mapper,
                combiner = job.config.combiner,
                mapInputPath = inputPath.toString(),
                mapOutputDir = mapOutputDir.toString()
        ))

        try {
            runWorker(workerState, logFile)
        } catch (e: Exception) {
            // TODO: deleteWorkerOutput()
        }

        job.runningWorkers.countDown()
    }

    @Throws(Exception::class)
    private fun runReduceWorker(job: JobState, reduceInputDirs: List<Path>) {
        val workerId = idGenerator.next()
        println("Creating reduce worker $workerId")

        val outputDir = Paths.get(job.config.outputDir)
        val logFile = job.tmpDir.resolve("reduce.log").resolve("Worker$workerId")
        Files.createDirectories(logFile.parent)

        val workerState = WorkerState(workerId, ReduceTask(
                reducer = job.config.reducer,
                reduceInputDirs = reduceInputDirs.map { it.toString() }.toTypedArray(),
                outputDir = outputDir.toString()
        ))

        try {
            runWorker(workerState, logFile)
        } catch (e: Exception) {
            // TODO: deleteWorkerOutput()
        }

        job.runningWorkers.countDown()
    }

    @Throws(Exception::class)
    private fun runWorker(workerState: WorkerState, logFile: Path) = withDefer {
        workers.put(workerState.id, workerState)
        defer { workers.remove(workerState.id) }
        val process = startWorkerProcess(workerState.id, logFile)
        defer {
            process.destroy()
            process.waitFor()
        }

        waitForRegistration(workerState)
        waitForWorker(workerState)
    }

    @Throws(TimeoutException::class)
    private fun waitForRegistration(workerState: WorkerState) {
        synchronized(workerState.lock) {
            println("Waiting for worker ${workerState.id} to register...")
            if (!workerState.registered) {
                workerState.lock.wait(WORKER_REGISTRATION_TIMEOUT)
            }
            if (!workerState.registered) {
                println("Worker ${workerState.id} registration time out")
                throw TimeoutException()
            }
        }
    }

    @Throws(RemoteException::class)
    private fun waitForWorker(workerState: WorkerState) {
        // periodic health and status checks
        while (true) {
            workerState.workerRmi.healthCheck()

            Thread.sleep(400)
            if (workerState.finished.get()) {
                return
            }
        }
    }

    private fun getPlaceForNewWorker() {
        synchronized(workerCounterLock) {
            while (workerCounter == maxWorkerCnt) {
                println("Waiting for a place for a new worker...")
                workerCounterLock.wait()
            }
            workerCounter += 1
        }
    }

    @Throws(IOException::class)
    private fun startWorkerProcess(id: WorkerId, logFile: Path): Process {
        println("Starting worker $id")
        // arguments passed as a json string to the only CLI parameter of the new process
        val processConfig = JSON.stringify(WorkerProcessConfig(registryHost, registryPort, id, logFile.toString()))
        return startProcess(com.xosmig.mlmr.worker.Main::class.java, processConfig)
    }

    private class WorkerState(val id: WorkerId, val task: WorkerTask) {
        var registered = false  // protected by `lock`
        val finished = AtomicBoolean(false)
        val lock = Object()
        lateinit var workerRmi: WorkerRmi
    }

    companion object {
        val WORKER_REGISTRATION_TIMEOUT = TimeUnit.SECONDS.toMillis(5)
    }
}
