package com.xosmig.mlmr.applicationmaster

import com.xosmig.mlmr.*
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject

abstract class ApplicationMaster(registryHost: String?, registryPort: Int): ApplicationMasterRmi {
    private val resourceManager = LocateRegistry.getRegistry(registryHost, registryPort).lookup(RM_REGISTRY_KEY)
            as ResourceManagerRmiForApplicationMaster
    private val stub by lazy { UnicastRemoteObject.exportObject(this, 0) as ApplicationMasterRmi }

    abstract fun run()

    fun startJob(config: JobConfig): JobId {
        return resourceManager.startJob(stub, CompiledJobConfig(config))
    }
}
