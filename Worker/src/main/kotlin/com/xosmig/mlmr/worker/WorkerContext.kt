package com.xosmig.mlmr.worker

import com.xosmig.mlmr.Context
import com.xosmig.mlmr.KVPair
import kotlinx.serialization.KSerializer
import writeCBORObject
import java.io.Closeable
import java.io.IOException
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path

class WorkerContext(private val outputDir: Path): Context, Closeable {
    private val outputStreams = HashMap<Any, OutputStream>()

    @Synchronized
    override fun <K: Any, V: Any> output(key: K, value: V, serializer: KSerializer<KVPair<K, V>>) {
        val outs = outputStreams[key] ?: run {
            Files.newOutputStream(outputDir.resolve(key.hashCode().toString()))
                    .apply { outputStreams.put(key, this) }
        }
        outs.writeCBORObject(KVPair(key, value), serializer)
    }

    @Throws(IOException::class)
    override fun close() {
        var exception: Exception? = null
        for ((_, stream) in outputStreams) {
            try {
                stream.close()
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
    }
}
