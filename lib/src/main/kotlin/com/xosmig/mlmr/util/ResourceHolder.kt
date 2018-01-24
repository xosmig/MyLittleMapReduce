package com.xosmig.mlmr.util

import java.io.Closeable
import java.io.IOException

class ResourceHolder : Closeable {
    private val resources = ArrayList<Closeable>(1)

    fun <T : Closeable?> T.autoClose(): T = addResource(this)

    @Synchronized
    fun <T : Closeable?> addResource(resource: T): T {
        if (resource != null) {
            try {
                resources.add(resource)
            } catch (th: Throwable) {
                resource.close()
                throw th
            }
        }
        return resource
    }

    inline fun defer(crossinline block: () -> Unit) = addResource(Closeable { block() })

    @Synchronized
    @Throws(IOException::class)
    override fun close() {
        var throwable: Throwable? = null
        var i = resources.size - 1
        while (i >= 0) {
            try {
                resources[i].close()
            } catch (th: Throwable) {
                if (throwable == null) {
                    throwable = th
                } else {
                    throwable.addSuppressed(th)
                }
            }
            i -= 1
        }
        if (throwable != null) {
            throw throwable
        }
    }
}

inline fun <R> using(block: ResourceHolder.() -> R): R = ResourceHolder().use(block)
