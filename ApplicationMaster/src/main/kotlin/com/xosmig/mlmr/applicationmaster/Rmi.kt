package com.xosmig.mlmr.applicationmaster

import com.xosmig.mlmr.CompiledJobConfig
import java.rmi.Remote
import java.rmi.RemoteException

interface ResourceManagerRmiForApplicationMaster: Remote {
    @Throws(RemoteException::class)
    fun startJob(stub: ApplicationMasterRmi, config: CompiledJobConfig): Int
}

interface ApplicationMasterRmi: Remote {
    @Throws(RemoteException::class)
    fun shutdown(reason: String?)

    @Throws(RemoteException::class)
    fun taskComplete(id: Int)
}
