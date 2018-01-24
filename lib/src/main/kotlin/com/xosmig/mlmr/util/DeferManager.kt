package com.xosmig.mlmr.util

class DeferManager {
    private val deferred = ArrayList<() -> Unit>(1)

    @Synchronized
    fun defer(block: () -> Unit) {
        deferred.add(block)
        // to avoid memory allocations in `deferred.add`
        deferred.ensureCapacity(deferred.size + 1)
    }

    @Synchronized
    @Throws(Exception::class)
    fun runDeferred() {
        var exception: Exception? = null
        for (i in deferred.size - 1 downTo 0) {
            try {
                deferred[i]()
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

        deferred.clear()
        deferred.ensureCapacity(1)
    }
}

inline fun <R> withDefer(block: DeferManager.() -> R): R {
    val handler = DeferManager()
    try {
        return handler.block()
    } finally {
        handler.runDeferred()
    }
}
