package com.xosmig.mlmr

import java.io.Serializable
import java.net.URL
import java.net.URLClassLoader

const val DEFAULT_REGISTRY_PORT = 16621
const val RM_REGISTRY_KEY = "ResourceManager"
const val WM_REGISTRY_KEY = "WorkersManager"

/**
 * Used to pass classes via RMI
 */
class ClassRef(cl: Class<*>) : Serializable {
    val classPath: URL = cl.protectionDomain.codeSource.location
    val className: String = cl.canonicalName

    fun load(): Class<*> {
        val classLoader = URLClassLoader(arrayOf(classPath))
        return classLoader.loadClass(className)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ClassRef) return false
        return classPath == other.classPath && className == other.className
    }

    override fun hashCode(): Int {
        return classPath.hashCode() * 31 + className.hashCode()
    }

    override fun toString(): String {
        return "ClassRef(classPath=$classPath, className='$className')"
    }
}
