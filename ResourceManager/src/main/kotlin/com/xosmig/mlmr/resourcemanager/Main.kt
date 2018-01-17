package com.xosmig.mlmr.resourcemanager

import com.xosmig.mlmr.DEFAULT_REGISTRY_PORT

fun main(args: Array<String>) {
    ResourceManager("localhost", DEFAULT_REGISTRY_PORT).run()
}
