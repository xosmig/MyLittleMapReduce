package com.xosmig.mlmr.util

import java.io.Closeable

class ResourceHolder: Closeable {
    private val resources = ArrayList<Closeable>(1)

    fun <T: Closeable?> T.autoClose(): T = addResource(this)

    @Synchronized
    fun<T: Closeable?> addResource(resource: T): T {
        if (resource != null) {
            resources.add(resource)
            // to avoid memory allocations in `resources.add`
            resources.ensureCapacity(resources.size + 1)
        }
        return resource
    }

    @Synchronized
    @Throws(Exception::class)
    override fun close() {
        var exception: Exception? = null
        for (i in resources.size - 1 downTo 0) {
            try {
                resources[i].close()
            } catch (ex: Exception) {
                if (exception == null) {
                    exception = ex
                } else {
                    exception.addSuppressed(ex)
                }
            }
        }
        if (exception != null) {
            throw exception
        }

        resources.clear()
        resources.ensureCapacity(1)
    }
}

fun<R> using(block: ResourceHolder.() -> R): R {
    ResourceHolder().use {
        return it.block()
    }
}
