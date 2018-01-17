package com.xosmig.mlmr.resourcemanager

import com.xosmig.mlmr.*
import com.xosmig.mlmr.applicationmaster.ApplicationMasterRmi
import com.xosmig.mlmr.applicationmaster.ResourceManagerRmiForApplicationMaster
import com.xosmig.mlmr.mapnode.MapNodeRmi
import com.xosmig.mlmr.mapnode.ResourceManagerRmiForMapNode
import com.xosmig.mlmr.mapnode.WorkerProcessConfig
import kotlinx.serialization.json.JSON
import java.io.*
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.LinkedBlockingQueue

internal class ResourceManager(
        private val host: String,
        private val port: Int,
        private var maxWorkerCnt: Int = 10):
        ResourceManagerRmiForApplicationMaster,
        ResourceManagerRmiForMapNode {

    private class JobStatus(
            val applicationMaster: ApplicationMasterRmi,
            val config: CompiledJobConfig,
            val id: JobId) {
        var mapStateDone = false
    }

    private val jobIdGenerator = JobId.Generator()
    private val workerIdGenerator = WorkerId.Generator()
    private val jobQueue = LinkedBlockingQueue<JobStatus>()

    private var workerCounter = 0
    private val workerCounterLock = Object()

    override fun startJob(applicationMasterStub: ApplicationMasterRmi, config: CompiledJobConfig): JobId {
        val id = jobIdGenerator.next()
        jobQueue.add(JobStatus(applicationMasterStub, config, id))
        return id
    }

    fun run(): Int {
        // Make the resource manager visible
        val registry = LocateRegistry.createRegistry(port)
        val stub = UnicastRemoteObject.exportObject(this, 0)
        registry.bind(RM_REGISTRY_KEY, stub)

        while (!Thread.interrupted()) {
            val job = jobQueue.poll()
            assert(job != null)

            if (job.mapStateDone) {
                startReduce(job)
            } else {
                startMap(job)
            }
        }
        TODO()
    }

    private fun startReduce(job: JobStatus) {
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

    private fun runMapNode(inputFile: Path) {
        val workerId = workerIdGenerator.next()
        val process = startMapProcess(workerId)

        ObjectInputStream(BufferedInputStream(process.inputStream)).use { ins ->
            ObjectOutputStream(BufferedOutputStream(process.outputStream)).use { outs ->
                TODO()
            }
        }
    }

    private fun startMap(job: JobStatus) {
        for (inputFile in Files.newDirectoryStream(Paths.get(job.config.inputDir))) {
            getPlaceForNewWorker()
            Thread { runMapNode(inputFile) }.start()
        }
    }

    override fun getTask(id: WorkerId, stub: MapNodeRmi): MapConfig {
        TODO()
    }

    @Throws(IOException::class)
    private fun startMapProcess(id: WorkerId): Process {
        // arguments passed as a json string to the only CLI parameter of the new process
        val processConfig = JSON.stringify(WorkerProcessConfig(host, port, id))
        return startProcess(com.xosmig.mlmr.mapnode.Main::class.java, processConfig)
    }

    companion object {

        @Throws(IOException::class)
        private fun startProcess(klass: Class<*>, vararg args: String): Process {
            val javaHome = System.getProperty("java.home")
            val javaBin = javaHome +
                    File.separator + "bin" +
                    File.separator + "java"
            val classpath = System.getProperty("java.class.path")
            val className = klass.canonicalName

            val builder = ProcessBuilder(javaBin, "-cp", classpath, className, *args)

            return builder.start()
        }
    }
}
