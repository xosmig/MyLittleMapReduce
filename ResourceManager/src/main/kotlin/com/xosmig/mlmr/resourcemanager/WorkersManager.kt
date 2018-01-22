package com.xosmig.mlmr.resourcemanager

import com.xosmig.mlmr.WorkerId
import com.xosmig.mlmr.util.startProcess
import com.xosmig.mlmr.util.startThread
import com.xosmig.mlmr.util.withDefer
import com.xosmig.mlmr.worker.*
import kotlinx.serialization.json.JSON
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.IOFileFilter
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.rmi.RemoteException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Level.*
import java.util.logging.Logger

internal class WorkersManager(val registryHost: String, val registryPort: Int): WorkersManagerRmi {
    private var workerCounter = 0
    private var maxWorkerCnt = 10
    private val workerCounterLock = Object()

    private val idGenerator = WorkerId.Generator()
    private val workers = ConcurrentHashMap<WorkerId, WorkerState>()

    override fun registerWorker(id: WorkerId, workerRmi: WorkerRmi): WorkerTask? {
        LOGGER.log(FINE, "Worker $id is here for registration")
        val workerState = workers[id] ?: return null
        synchronized(workerState.lock) {
            // note that the worker might have already been destroyed
            workerState.registered = true
            workerState.workerRmi = workerRmi
            workerState.lock.notify()
            LOGGER.log(FINE, "Worker $id has been registered")
            return workerState.task
        }
    }

    override fun workerFinished(id: WorkerId) {
        val workerState = workers[id] ?: return
        LOGGER.log(FINE, "Worker $id has finished successfully")
        workerState.finished.set(true)
    }

    /**
     * Blocks until there is a place for a new worker.
     */
    fun startMapWorker(job: JobState, inputFile: Path) {
        startWorker(job) { runMapWorker(job, inputFile) }
    }

    /**
     * Blocks until there is a place for a new worker.
     */
    fun startReduceWorker(job: JobState, reduceInputDir: Path) {
        startWorker(job) { runReduceWorker(job, reduceInputDir) }
    }

    inline private fun startWorker(job: JobState, crossinline run: () -> Unit) {
        getPlaceForNewWorker()
        startThread {
            try {
                var exception: Exception? = null
                for (i in 1..5) {
                    try {
                        run()
                    } catch (ex: Exception) {
                        exception = ex
                    }

                    if (exception == null) {
                        break
                    }
                }

                if (exception != null) {
                    LOGGER.log(WARNING, "Job${job.id} failed")
                    job.applicationMaster.jobFailed(job.id)
                }
            } finally {
                releasePlaceForWorker()
            }
        }
    }

    @Throws(Exception::class)
    private fun runMapWorker(job: JobState, inputPath: Path) {
        val workerId = idGenerator.next()
        LOGGER.log(FINE, "Creating map worker $workerId for file: '$inputPath'")

        val mapOutputDir = job.tmpDir.resolve("map.output")
        Files.createDirectories(mapOutputDir)
        try {
            val logFile = job.tmpDir.resolve("map.log").resolve("Worker$workerId")
            Files.createDirectories(logFile.parent)

            val workerState = WorkerState(workerId, MapTask(
                    mapper = job.config.mapper,
                    mapInputPath = inputPath.toString(),
                    mapOutputDir = mapOutputDir.toString(),
                    groupCnt = job.inputSize
            ))

            runWorker(workerState, logFile)
            job.runningWorkers.countDown()
        } catch (ex: Exception) {
            LOGGER.log(WARNING, "Map worker $workerId failed", ex)
            deleteWorkerOutput(workerId, mapOutputDir)
            throw ex
        }
    }

    @Throws(Exception::class)
    private fun runReduceWorker(job: JobState, reduceInputDir: Path) {
        val workerId = idGenerator.next()
        LOGGER.log(FINE, "Creating reduce worker $workerId for dir '$reduceInputDir'")

        val outputDir = Paths.get(job.config.outputDir)
        try {
            val logFile = job.tmpDir.resolve("reduce.log").resolve("Worker$workerId")
            Files.createDirectories(logFile.parent)

            val workerState = WorkerState(workerId, ReduceTask(
                    reducer = job.config.reducer,
                    reduceInputDir = reduceInputDir.toString(),
                    outputDir = outputDir.toString()
            ))

            runWorker(workerState, logFile)
            job.runningWorkers.countDown()
        } catch (ex: Exception) {
            LOGGER.log(WARNING, "Reduce worker $workerId failed", ex)
            deleteWorkerOutput(workerId, outputDir)
            throw ex
        }
    }

    @Throws(IOException::class)
    private fun deleteWorkerOutput(workerId: WorkerId, outputDir: Path) {
        val pattern = "Worker$workerId"
        val fileFilter = object: IOFileFilter {
            override fun accept(file: File): Boolean = file.name.contains(pattern)
            override fun accept(dir: File?, name: String): Boolean = name.contains(pattern)
        }
        // Filter out log directories
        val directoryFilter = object: IOFileFilter {
            override fun accept(file: File): Boolean = !file.name.contains("log")
            override fun accept(dir: File?, name: String): Boolean = !name.contains("log")
        }
        val fileIter = FileUtils.iterateFiles(outputDir.toFile(), fileFilter, directoryFilter)
        for (file in fileIter) {
            file.delete()
        }
    }

    @Throws(Exception::class)
    private fun runWorker(workerState: WorkerState, logFile: Path) = withDefer {
        workers.put(workerState.id, workerState)
        defer { workers.remove(workerState.id) }
        val process = startWorkerProcess(workerState.id, logFile)
        defer {
            process.destroy()
            LOGGER.log(FINER, "Waiting for process of worker${workerState.id} to finish ...")
            process.waitFor()
            LOGGER.log(FINER, "Process of worker${workerState.id} has finished")
        }

        waitForRegistration(workerState)
        waitForWorker(workerState)
    }

    @Throws(TimeoutException::class)
    private fun waitForRegistration(workerState: WorkerState) {
        synchronized(workerState.lock) {
            LOGGER.log(FINE, "Waiting for worker ${workerState.id} to register...")
            if (!workerState.registered) {
                workerState.lock.wait(WORKER_REGISTRATION_TIMEOUT)
            }
            if (!workerState.registered) {
                LOGGER.log(WARNING, "Worker ${workerState.id} registration time out")
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
                LOGGER.log(FINE, "Waiting for a place for a new worker...")
                workerCounterLock.wait()
            }
            workerCounter += 1
        }
    }

    private fun releasePlaceForWorker() {
        synchronized(workerCounterLock) {
            workerCounter -= 1
            assert(workerCounter >= 0)
            workerCounterLock.notify()
        }
    }

    @Throws(IOException::class)
    private fun startWorkerProcess(id: WorkerId, logFile: Path): Process {
        LOGGER.log(FINE, "Starting worker$id process ...")
        // arguments passed as a json string to the only CLI parameter of the new process
        val processConfig = JSON.stringify(WorkerProcessConfig(registryHost, registryPort, id, logFile.toString()))
        return startProcess(com.xosmig.mlmr.worker.Main.javaClass, processConfig)
    }

    private class WorkerState(val id: WorkerId, val task: WorkerTask) {
        var registered = false  // protected by `lock`
        val finished = AtomicBoolean(false)
        val lock = Object()
        lateinit var workerRmi: WorkerRmi
    }

    companion object {
        val WORKER_REGISTRATION_TIMEOUT = TimeUnit.SECONDS.toMillis(5)
        private val LOGGER = Logger.getLogger(WorkersManager::class.java.name)
    }
}
