package com.xosmig.mlmr.resourcemanager

import com.xosmig.mlmr.*
import com.xosmig.mlmr.applicationmaster.ApplicationMasterRmi
import com.xosmig.mlmr.applicationmaster.ResourceManagerRmiForApplicationMaster
import com.xosmig.mlmr.worker.*
import org.apache.commons.io.FileUtils
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue

internal class ResourceManager(
        private val thisHost: String,
        private val registryPort: Int):
        ResourceManagerRmiForApplicationMaster,
        ResourceManagerRmiForWorker {

    private val jobIdGenerator = JobId.Generator()
    private val jobQueue = LinkedBlockingQueue<JobState>()
    private val workersManager = WorkersManager(thisHost, registryPort)

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
        val registry = LocateRegistry.createRegistry(registryPort)
        val stub = UnicastRemoteObject.exportObject(this, 0)
        registry.bind(RM_REGISTRY_KEY, stub)

        // schedule jobs
        while (true) {
            val job = jobQueue.take()
            assert(job != null)

            if (job.mapStageDone) {
                // TODO: startReduce(job)
            } else {
                startMap(job)
            }
        }
    }

    fun startMap(job: JobState) {
        val inputFiles = Files.newDirectoryStream(Paths.get(job.config.inputDir)).toList()
        job.completedTasks = CountDownLatch(inputFiles.size)
        for (inputFile in inputFiles) {
            workersManager.startMapWorker(job, inputFile)
        }
        Thread {
            job.completedTasks?.await() ?: TODO()  // TODO
            // Map finished. Schedule reduce
            job.mapStageDone = true
            jobQueue.put(job)
        }.start()
    }

    override fun workersManager(): WorkersManagerRmi = workersManager
}
