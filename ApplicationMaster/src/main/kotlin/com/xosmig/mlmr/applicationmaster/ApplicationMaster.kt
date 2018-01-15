package com.xosmig.mlmr.applicationmaster

import com.xosmig.mlmr.DEFAULT_REGISTRY_PORT
import com.xosmig.mlmr.RM_REGISTRY_KEY
import java.rmi.registry.LocateRegistry

abstract class ApplicationMaster(registryHost: String? = null, registryPort: Int = DEFAULT_REGISTRY_PORT) {
    private val resourceManager = LocateRegistry.getRegistry(registryHost, registryPort).lookup(RM_REGISTRY_KEY)

    abstract fun run()

    fun runJob(config: JobConfig) {

    }
}
