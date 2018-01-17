package com.xosmig.mlmr.resourcemanager

import com.xosmig.mlmr.CompiledJobConfig
import com.xosmig.mlmr.DEFAULT_REGISTRY_PORT
import com.xosmig.mlmr.RM_REGISTRY_KEY
import com.xosmig.mlmr.applicationmaster.ApplicationMasterRmi
import com.xosmig.mlmr.applicationmaster.ResourceManagerRmiForApplicationMaster
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject
import java.util.concurrent.TimeUnit

class ResourceManager: ResourceManagerRmiForApplicationMaster {

    override fun startJob(stub: ApplicationMasterRmi, config: CompiledJobConfig): Int {
        // TODO
        println("Got a job!")
        Thread {
            println("start doing the job")
            Thread.sleep(TimeUnit.SECONDS.toMillis(5))
            println("finish the job")
            stub.taskComplete(17)
        }.start()
        return 17
    }

    fun run(port: Int = DEFAULT_REGISTRY_PORT): Int {
        // Make the resource manager visible
        val registry = LocateRegistry.createRegistry(port)
        val stub = UnicastRemoteObject.exportObject(this, 0)
        registry.bind(RM_REGISTRY_KEY, stub)

        // TODO
        while (true) {
            Thread.sleep(Long.MAX_VALUE)
        }
    }
}
