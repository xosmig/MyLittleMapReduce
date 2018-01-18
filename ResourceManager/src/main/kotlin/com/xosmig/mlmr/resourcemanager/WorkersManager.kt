package com.xosmig.mlmr.resourcemanager

import com.xosmig.mlmr.WorkerId
import com.xosmig.mlmr.worker.*
import kotlinx.serialization.json.JSON
import org.apache.commons.io.FileUtils
import java.io.IOException
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

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

    override fun workerFinished(id: WorkerId, success: Boolean) {
        val workerState = workers[id] ?: TODO()
        synchronized(workerState.lock) {
            workerState.finished = true
            workerState.success = success
            workerState.lock.notify()
        }
    }

    /**
     * Blocks until there is a place for a new worker.
     */
    fun startMapWorker(jobState: JobState, inputFile: Path) {
        getPlaceForNewWorker()
        Thread {
            var ok = false
            for (i in 1..10) {
                ok = tryRunMapWorker(jobState, inputFile)
                if (ok) {
                    break
                }
            }

            if (!ok) {
                TODO()
            }
        }.start()
    }

    /**
     * @return false if failed to complete the task. True otherwise.
     */
    private fun tryRunMapWorker(job: JobState, inputPath: Path): Boolean {
        println("Creating map worker for file: '$inputPath'")

        val workerId = idGenerator.next()
        val mapOutputDir = job.tmpDir.resolve("Worker${workerId.value}")

        val workerState = WorkerState(job, MapTask(
                mapper = job.config.mapper,
                combiner = job.config.combiner,
                mapInputPath = inputPath.toString(),
                mapOutputDir = mapOutputDir.toString()
        ))

        workers.put(workerId, workerState)

        try {
            val process = startWorkerProcess(workerId)
            synchronized(workerState.lock) {
                println("Waiting for worker $workerId to register...")
                if (!workerState.registered) {
                    workerState.lock.wait(WORKER_REGISTRATION_TIMEOUT)
                }
                if (!workerState.registered) {
                    println("Worker $workerId registration time out")
                    process.destroy()
                    return false
                }
            }

            // periodic health and status checks
            while (true) {
                val status = try {
                    workerState.workerRmi.check()
                } catch (e: Throwable) {
                    process.destroy()
                    return false
                }

                synchronized(workerState.lock) {
                    workerState.lock.wait(300)
                    if (workerState.finished && workerState.success) {
                        job.completedTasks?.countDown() ?: TODO()  // TODO
                        return true
                    }
                    if (workerState.finished && workerState.success) {
                        FileUtils.deleteDirectory(mapOutputDir.toFile())
                        return false
                    }
                }
            }
        } finally {
            workers.remove(workerId)
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
    private fun startWorkerProcess(id: WorkerId): Process {
        println("Starting worker $id")
        // arguments passed as a json string to the only CLI parameter of the new process
        val processConfig = JSON.stringify(WorkerProcessConfig(registryHost, registryPort, id))
        return startProcess(com.xosmig.mlmr.worker.Main::class.java, processConfig)
    }

    private class WorkerState(val job: JobState, val task: WorkerTask) {
        var registered = false  // protected by `lock`
        var finished = false    // protected by `lock`
        var success = false     // protected by `lock`
        val lock = Object()
        lateinit var workerRmi: WorkerRmi
    }

    companion object {
        val WORKER_REGISTRATION_TIMEOUT = TimeUnit.SECONDS.toMillis(5)
    }
}
