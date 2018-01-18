package com.xosmig.mlmr.resourcemanager

import com.xosmig.mlmr.CompiledJobConfig
import com.xosmig.mlmr.JobId
import com.xosmig.mlmr.applicationmaster.ApplicationMasterRmi
import java.nio.file.Path
import java.util.concurrent.CountDownLatch

internal class JobState(
        val applicationMaster: ApplicationMasterRmi,
        val config: CompiledJobConfig,
        val id: JobId,
        val tmpDir: Path) {

    var mapStageDone = false
    var completedTasks: CountDownLatch? = null
}
