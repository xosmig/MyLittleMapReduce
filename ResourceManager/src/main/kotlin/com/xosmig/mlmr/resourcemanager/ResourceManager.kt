package com.xosmig.mlmr.resourcemanager

import com.xosmig.mlmr.*
import com.xosmig.mlmr.applicationmaster.ApplicationMasterRmi
import com.xosmig.mlmr.applicationmaster.ResourceManagerRmiForApplicationMaster
import com.xosmig.mlmr.util.startThread
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.logging.Level.INFO
import java.util.logging.Level.WARNING
import java.util.logging.Logger

class ResourceManager(
        private val thisHost: String,
        private val registryPort: Int) :
        ResourceManagerRmiForApplicationMaster {

    private val jobIdGenerator = JobId.Generator()
    private val jobQueue = LinkedBlockingQueue<JobState>()
    private val workersManager = WorkersManager(thisHost, registryPort)

    override fun startJob(applicationMasterStub: ApplicationMasterRmi, config: CompiledJobConfig): JobId {
        val id = jobIdGenerator.next()
        try {
            LOGGER.log(INFO, "Got a job: jobId=$id")
            val outputDir = Paths.get(config.outputDir)
            Files.createDirectory(outputDir)

            val tmpDir = outputDir.resolve(".mlmr.tmp")
            Files.createDirectory(tmpDir)

            jobQueue.add(JobState(applicationMasterStub, config, id, tmpDir))
            return id
        } catch (ex: Exception) {
            LOGGER.log(WARNING, "Failed to start job $id", ex)
            throw ex // All exception should be handled by the ApplicationMaster
        }
    }

    fun run(): Int {
        // Make the resource manager visible
        val registry = LocateRegistry.createRegistry(registryPort)
        registry.bind(RM_REGISTRY_KEY, UnicastRemoteObject.exportObject(this, 0))
        registry.bind(WM_REGISTRY_KEY, UnicastRemoteObject.exportObject(workersManager, 0))

        // schedule jobs
        while (true) {
            val job = jobQueue.take()

            if (job.mapStageDone) {
                startReduce(job)
            } else {
                startMap(job)
            }
        }
    }

    private fun startMap(job: JobState) {
        val inputFiles = Files.newDirectoryStream(Paths.get(job.config.inputDir)).toList()
        job.inputSize = inputFiles.size
        job.runningWorkers = CountDownLatch(job.inputSize)
        for (inputFile in inputFiles) {
            workersManager.startMapWorker(job, inputFile)
        }
        startThread {
            job.runningWorkers.await()
            // Map finished. Schedule reduce
            job.mapStageDone = true
            jobQueue.put(job)
        }
    }

    private fun startReduce(job: JobState) {
        val mapOutputDir = job.tmpDir.resolve("map.output")
        val inputDirs = Files.newDirectoryStream(mapOutputDir).toList()
        job.runningWorkers = CountDownLatch(inputDirs.size)
        for (dir in inputDirs) {
            workersManager.startReduceWorker(job, dir)
        }
        startThread {
            job.runningWorkers.await()
            // Reduce finished. Notify the application master
            job.applicationMaster.jobComplete(job.id)
        }
    }

    companion object {
        private val LOGGER = Logger.getLogger(ResourceManager::class.java.name)
    }
}
