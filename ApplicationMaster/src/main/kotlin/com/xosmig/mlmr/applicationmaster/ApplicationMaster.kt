package com.xosmig.mlmr.applicationmaster

import com.xosmig.mlmr.DEFAULT_REGISTRY_PORT
import com.xosmig.mlmr.RM_REGISTRY_KEY
import java.rmi.registry.LocateRegistry
import java.rmi.server.UnicastRemoteObject

abstract class ApplicationMaster
        (registryHost: String? = null, registryPort: Int = DEFAULT_REGISTRY_PORT): RM2AMRmi {
    private val resourceManager =
            LocateRegistry.getRegistry(registryHost, registryPort).lookup(RM_REGISTRY_KEY) as AM2RMRmi
    private val stub by lazy { UnicastRemoteObject.exportObject(this, 0) as RM2AMRmi }

    abstract fun run()

    fun startJob(config: JobConfig): JobId {
        return JobId(resourceManager.startJob(stub, CompiledJobConfig(config)))
    }

    override final fun shutdown(reason: String?) {
        TODO()
    }

    override final fun taskComplete(id: Int) {
        TODO()
    }

    fun waitForJob(id: JobId) {

    }

    // TODO?: use java Feature? Or something similar?
    class JobId internal constructor(private val id: Int) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is JobId) return false

            if (id != other.id) return false

            return true
        }

        override fun hashCode(): Int {
            return id
        }
    }
}
