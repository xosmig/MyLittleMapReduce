package com.xosmig.mlmr.examples

import com.xosmig.mlmr.*
import com.xosmig.mlmr.applicationmaster.ApplicationMaster
import com.xosmig.mlmr.JobConfig
import kotlinx.serialization.Serializable
import org.apache.commons.io.FileUtils
import java.io.BufferedInputStream
import java.io.File
import java.io.InputStream
import java.util.*

@Serializable
data class MyOwnKeyClass(val key: String) {
    override fun toString(): String = key
}

class WordCountMapper: InputStreamMapper<MyOwnKeyClass, SInt>(MyOwnKeyClass::class, SInt::class) {
    override fun map(input: InputStream, context: NodeContext<MyOwnKeyClass, SInt>) {
        BufferedInputStream(input).use { bufferedInput ->
            Scanner(bufferedInput).use { scanner ->
                while (scanner.hasNext()) {
                    context.output(MyOwnKeyClass(scanner.next()), SInt(1))
                }
            }
        }
    }
}

class WordCountApp: ApplicationMaster("localhost", DEFAULT_REGISTRY_PORT) {
    override fun run() {
        val config = JobConfig(
                WordCountMapper::class.java,
                WordCountMapper::class.java,
                WordCountMapper::class.java,
                "/home/andrey/tmp/mlmr/word_count/input",
                "/home/andrey/tmp/mlmr/word_count/output"
        )
        val id = startJob(config)
        while (true) {
            Thread.sleep(Long.MAX_VALUE)
        }
    }
}

fun main(args: Array<String>) {
    FileUtils.deleteDirectory(File("/home/andrey/tmp/mlmr/word_count/output"))
    com.xosmig.mlmr.examples.WordCountApp().run()
}