package com.xosmig.mlmr.resourcemanager

import com.xosmig.mlmr.DEFAULT_REGISTRY_PORT
import java.util.logging.Level.FINE
import java.util.logging.Level.INFO
import java.util.logging.Logger

object Main {
    @JvmStatic
    fun main(args: Array<String>) {
        Logger.getLogger("").level = FINE
        Logger.getLogger(Main::class.java.name).log(INFO, "Resource Manager started")
        ResourceManager("localhost", DEFAULT_REGISTRY_PORT).run()
    }
}
