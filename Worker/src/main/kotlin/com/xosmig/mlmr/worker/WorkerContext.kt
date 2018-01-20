package com.xosmig.mlmr.worker

import com.xosmig.mlmr.Context
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
    private val resourceManager = ResourceManager()

    @Synchronized
    override fun <K: Any, V: Any> output(key: K, value: V,
                                         keySerializer: KSerializer<K>, valueSerializer: KSerializer<V>) {
        val outs = outputStreams.getOrPut(key) {
            val hash = key.hashCode()
            val dir = outputDir.resolve(hash.toString())
            Files.createDirectories(dir)
            val lastIdx = Files.newDirectoryStream(dir).use {
                it.map { it.fileName.toString().toInt() }.max() ?: -1
            }
            val filename = "Worker${workerId}_key#${lastIdx + 1}"
            val outs = resourceManager.addResource(Files.newOutputStream(dir.resolve(filename)))
            outs.writeCBORObject(key, keySerializer)
            return@getOrPut outs
        }
        outs.writeCBORObject(value, valueSerializer)
    }

    @Throws(IOException::class)
    override fun close() = resourceManager.close()
}
