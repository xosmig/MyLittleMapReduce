package com.xosmig.mlmr.mapnode

import com.xosmig.mlmr.WorkerId
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JSON
import kotlin.system.exitProcess

@Serializable
data class WorkerProcessConfig(val registryHost: String, val registryPort: Int, val id: WorkerId)

object Main {
    val helpMessage: String = "Expected single parameter: JSON representation of `WorkerProcessConfig`"

    @JvmStatic
    fun main(args: Array<String>) {
        if (args.size != 1) {
            System.err.println(helpMessage)
            exitProcess(2)
        }
        val processConfig = try {
            JSON.Companion.parse<WorkerProcessConfig>(args[0])
        } catch (_: Exception) {
            System.err.println("Invalid parameter")
            System.err.println(helpMessage)
            exitProcess(3)
        }

        MapNode(processConfig.registryHost, processConfig.registryPort, processConfig.id).run()
    }
}
