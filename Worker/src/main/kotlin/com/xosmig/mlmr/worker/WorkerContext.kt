package com.xosmig.mlmr.worker

import com.xosmig.mlmr.Context
import com.xosmig.mlmr.KVPair
import com.xosmig.mlmr.WorkerId
import kotlinx.serialization.KSerializer
import writeCBORObject
import java.io.Closeable
import java.io.IOException
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path

class WorkerContext(private val workerId: WorkerId, private val outputDir: Path): Context, Closeable {
    private val outputStreams = HashMap<Any, OutputStream>()

    @Synchronized
    override fun <K: Any, V: Any> output(key: K, value: V,
                                         keySerializer: KSerializer<K>, valueSerializer: KSerializer<V>) {
        val outs = outputStreams[key] ?: newOutputStream(key, keySerializer)
        outs.writeCBORObject(value, valueSerializer)
    }

    fun<K: Any> newOutputStream(key: K, serializer: KSerializer<K>): OutputStream {
        val hash = key.hashCode()
        val dir = outputDir.resolve(hash.toString())
        Files.createDirectories(dir)
        val lastIdx = Files.newDirectoryStream(dir)
                .map { it.fileName.toString().toInt() }
                .max() ?: -1
        val filename = "Worker${workerId}_key#${lastIdx + 1}"
        val outs = Files.newOutputStream(dir.resolve(filename)) as OutputStream
        outputStreams.put(key, outs)
        outs.writeCBORObject(key, serializer)
        return outs
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
