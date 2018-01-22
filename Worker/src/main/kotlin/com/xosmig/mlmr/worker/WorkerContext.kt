package com.xosmig.mlmr.worker

import com.xosmig.mlmr.Context
import com.xosmig.mlmr.WorkerId
import com.xosmig.mlmr.util.ResourceHolder
import kotlinx.serialization.KSerializer
import writeCBORObject
import java.io.Closeable
import java.io.IOException
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path

class WorkerContext(private val workerId: WorkerId,
                    private val outputDir: Path,
                    private val groupCnt: Int): Context, Closeable {
    private val outputStreams = HashMap<Any, OutputStream>()
    private val resourceHolder = ResourceHolder()
    private var nextKeyIdx = 1

    @Synchronized
    override fun <K: Any, V: Any> output(key: K, value: V,
                                         keySerializer: KSerializer<K>,
                                         valueSerializer: KSerializer<V>) {
        val outs = outputStreams.getOrPut(key) {
            val dir = if (groupCnt != 0) {
                val group = Math.floorMod(key.hashCode(), groupCnt)
                outputDir.resolve(group.toString()).apply { Files.createDirectories(this) }
            } else {
                outputDir
            }
            val filename = "Worker${workerId}_key#$nextKeyIdx"
            nextKeyIdx += 1
            val outs = resourceHolder.addResource(Files.newOutputStream(dir.resolve(filename)))
            outs.writeCBORObject(key, keySerializer)
            return@getOrPut outs
        }
        outs.writeCBORObject(value, valueSerializer)
    }

    @Throws(IOException::class)
    override fun close() = resourceHolder.close()
}
