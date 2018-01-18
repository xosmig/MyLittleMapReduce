package com.xosmig.mlmr.resourcemanager

import com.xosmig.mlmr.*
import com.xosmig.mlmr.applicationmaster.ApplicationMasterRmi
import com.xosmig.mlmr.applicationmaster.ResourceManagerRmiForApplicationMaster
import com.xosmig.mlmr.worker.*
import kotlinx.serialization.json.JSON
import org.apache.commons.io.FileUtils
import java.io.*
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

internal class ResourceManager(
        private val host: String,
        private val port: Int,
        private var maxWorkerCnt: Int = 10):
        ResourceManagerRmiForApplicationMaster,
        ResourceManagerRmiForWorker {

    private val jobIdGenerator = JobId.Generator()
    private val workerIdGenerator = WorkerId.Generator()
    private val jobQueue = LinkedBlockingQueue<JobState>()

    private var workerCounter = 0
    private val workerCounterLock = Object()
    private val workers = ConcurrentHashMap<WorkerId, WorkerState>()

    override fun startJob(applicationMasterStub: ApplicationMasterRmi, config: CompiledJobConfig): JobId {
        try {
            val id = jobIdGenerator.next()

            val outputDir = Paths.get(config.outputDir)
            Files.createDirectory(outputDir)

            val tmpDir = outputDir.resolve(".mlmr.tmp")
            Files.createDirectory(tmpDir)

            try {
                jobQueue.add(JobState(applicationMasterStub, config, id, tmpDir))
                return id
            } finally {
                FileUtils.deleteDirectory(tmpDir.toFile())
            }
        } catch (e: Exception) {
            throw e // all exception are handled by the ApplicationMaster
        }
    }

    fun run(): Int {
        // Make the resource manager visible
        val registry = LocateRegistry.createRegistry(port)
        val stub = UnicastRemoteObject.exportObject(this, 0)
        registry.bind(RM_REGISTRY_KEY, stub)

        // schedule jobs
        while (true) {
            val job = jobQueue.poll()
            assert(job != null)

            if (job.mapStageDone) {
                // TODO: startReduce(job)
            } else {
                startMap(job)
            }
        }
    }

    private fun startReduce(job: JobState) {
        TODO()
    }

    private fun getPlaceForNewWorker() {
        synchronized(workerCounterLock) {
            while (workerCounter == maxWorkerCnt) {
                workerCounterLock.wait()
            }
            workerCounter += 1
        }
    }

    private fun runMapNode(job: JobState, inputPath: Path) {
        // TODO: more adequate strategy
        var ok = false
        for (i in 1..10) {
            ok = tryRunMapNode(job, inputPath)
            if (ok) {
                break
            }
        }

        if (!ok) {
            TODO()
        }
    }

    /**
     * @return false if failed to complete the task. True otherwise.
     */
    private fun tryRunMapNode(job: JobState, inputPath: Path): Boolean {
        val workerId = workerIdGenerator.next()
        val mapOutputDir = job.tmpDir.resolve("Worker${workerId.value}")

        val workerState = WorkerState(job, MapTask(
                mapper = job.config.mapper,
                combiner = job.config.combiner,
                mapInputPath = inputPath.toString(),
                mapOutputDir = mapOutputDir.toString()
        ))

        workers.put(workerId, workerState)

        try {
            val process = startMapProcess(workerId)
            synchronized(workerState.lock) {
                if (!workerState.registered) {
                    workerState.lock.wait(WORKER_REGISTRATION_TIMEOUT)
                }
                if (!workerState.registered) {
                    // Assume timeout. Clean and retry
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

    override fun registerWorker(id: WorkerId, workerRmi: WorkerRmi): WorkerTask? {
        val workerState = workers[id] ?: return null
        synchronized(workerState.lock) {
            // note that the worker might have already been destroyed
            workerState.registered = true
            workerState.workerRmi = workerRmi
            workerState.lock.notify()
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

    private fun startMap(job: JobState) {
        val inputFiles = Files.newDirectoryStream(Paths.get(job.config.inputDir)).toList()
        job.completedTasks = CountDownLatch(inputFiles.size)
        for (inputFile in inputFiles) {
            getPlaceForNewWorker()
            Thread { runMapNode(job, inputFile) }.start()
        }
        Thread {
            job.completedTasks?.await() ?: TODO()  // TODO
            // Map finished. Schedule reduce
            job.mapStageDone = true
            jobQueue.put(job)
        }.start()
    }

    @Throws(IOException::class)
    private fun startMapProcess(id: WorkerId): Process {
        // arguments passed as a json string to the only CLI parameter of the new process
        val processConfig = JSON.stringify(WorkerProcessConfig(host, port, id))
        return startProcess(com.xosmig.mlmr.worker.Main::class.java, processConfig)
    }

    private class WorkerState(val job: JobState, val task: WorkerTask) {
        var registered = false  // protected by `lock`
        var finished = false    // protected by `lock`
        var success = false     // protected by `lock`
        val lock = Object()
        lateinit var workerRmi: WorkerRmi
    }

    private class JobState(
            val applicationMaster: ApplicationMasterRmi,
            val config: CompiledJobConfig,
            val id: JobId,
            val tmpDir: Path) {

        var mapStageDone = false
        var completedTasks: CountDownLatch? = null
    }

    companion object {
        val WORKER_REGISTRATION_TIMEOUT =  TimeUnit.SECONDS.toMillis(5)

        @Throws(IOException::class)
        private fun startProcess(klass: Class<*>, vararg args: String): Process {
            val javaHome = System.getProperty("java.home")
            val javaBin = javaHome + File.separator + "bin" + File.separator + "java"
            val classpath = System.getProperty("java.class.path")
            val className = klass.canonicalName

            val builder = ProcessBuilder(javaBin, "-cp", classpath, className, *args)

            return builder.start()
        }
    }
}
