package com.xosmig.mlmr.resourcemanager

import com.xosmig.mlmr.DEFAULT_REGISTRY_PORT

object Main {
    @JvmStatic
    fun main(args: Array<String>) {
        println("Resource Manager started")
        ResourceManager("localhost", DEFAULT_REGISTRY_PORT).run()
    }
}
