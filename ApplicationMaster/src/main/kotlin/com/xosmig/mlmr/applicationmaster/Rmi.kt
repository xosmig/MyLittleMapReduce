package com.xosmig.mlmr.applicationmaster

import com.xosmig.mlmr.CompiledJobConfig
import com.xosmig.mlmr.JobId
import java.rmi.Remote
import java.rmi.RemoteException

interface ResourceManagerRmiForApplicationMaster: Remote {
    @Throws(RemoteException::class)
    fun startJob(applicationMasterStub: ApplicationMasterRmi, config: CompiledJobConfig): JobId
}

interface ApplicationMasterRmi: Remote {
    @Throws(RemoteException::class)
    fun jobComplete(id: JobId)
}
