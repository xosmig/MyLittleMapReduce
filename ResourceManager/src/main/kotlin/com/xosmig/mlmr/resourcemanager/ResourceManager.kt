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

internal class ResourceManager(
        private val thisHost: String,
        private val registryPort: Int):
        ResourceManagerRmiForApplicationMaster {

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

            jobQueue.add(JobState(applicationMasterStub, config, id, tmpDir))
            return id
        } catch (e: Exception) {
            throw e // all exception are handled by the ApplicationMaster
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
        val reducersCnt = Math.min(job.inputSize, inputDirs.size)
        job.runningWorkers = CountDownLatch(reducersCnt)
        if (reducersCnt > 0) {
            for (reducerInputDirs in inputDirs.chunked(inputDirs.size / reducersCnt)) {
                workersManager.startReduceWorker(job, reducerInputDirs)
            }
        }
        startThread {
            job.runningWorkers.await()
            // Reduce finished. Notify the application master
            job.applicationMaster.jobComplete(job.id)
        }
    }
}
