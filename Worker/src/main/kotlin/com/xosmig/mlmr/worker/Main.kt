package com.xosmig.mlmr.worker

import com.xosmig.mlmr.WorkerId
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JSON
import java.util.*
import java.util.logging.FileHandler
import java.util.logging.Level.INFO
import java.util.logging.Level.SEVERE
import java.util.logging.Logger
import java.util.logging.SimpleFormatter
import kotlin.system.exitProcess

@Serializable
data class WorkerProcessConfig(val registryHost: String,
                               val registryPort: Int,
                               val id: WorkerId,
                               val logFile: String)

object Main {
    private val helpMessage: String = "Expected single parameter: JSON representation of `WorkerProcessConfig`"

    @JvmStatic
    fun main(args: Array<String>) {
        if (args.size != 1) {
            System.err.println(helpMessage)
            exitProcess(2)
        }
        val processConfig = try {
            JSON.parse<WorkerProcessConfig>(args[0])
        } catch (_: Exception) {
            System.err.println("Invalid parameter")
            System.err.println(helpMessage)
            exitProcess(3)
        }

        Logger.getLogger("").addHandler(FileHandler(processConfig.logFile).apply { formatter = SimpleFormatter() })
        val logger = Logger.getLogger(Main.javaClass.name)
        logger.log(INFO, "Process started")

        try {
            val workerClass = Worker(processConfig.registryHost, processConfig.registryPort, processConfig.id)
            exitProcess(workerClass.run())
        } catch (e: Throwable) {
            logger.log(SEVERE, "", e)
            throw e
        }
    }
}
