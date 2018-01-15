package com.xosmig.mlmr

import java.io.Serializable
import java.net.URL

const val DEFAULT_REGISTRY_PORT = 16621
const val RM_REGISTRY_KEY = "ResourceManager"

/**
 * Used to pass classes via RMI
 */
class ClassRef(cl: Class<*>): Serializable {
    val classPath: URL = cl.protectionDomain.codeSource.location
    val className: String = cl.canonicalName
}
