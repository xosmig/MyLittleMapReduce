package com.xosmig.mlmr.examples

import com.xosmig.mlmr.*
import com.xosmig.mlmr.applicationmaster.ApplicationMaster
import com.xosmig.mlmr.JobConfig
import kotlinx.serialization.Serializable
import org.apache.commons.io.FileUtils
import java.io.BufferedInputStream
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess


@Serializable
data class MyOwnKeyClass(val key: Char) {
    override fun toString(): String = key.toString()
}

class WordCountMapper: Mapper<MyOwnKeyClass, SInt>(MyOwnKeyClass::class, SInt::class) {
    override fun map(input: InputStream, context: NodeContext<MyOwnKeyClass, SInt>) {
        BufferedInputStream(input).use { bufferedInput ->
            Scanner(bufferedInput).use { scanner ->
                while (scanner.hasNext()) {
                    val word = scanner.next()
                    context.output(MyOwnKeyClass(word[0]), SInt.one)
                }
            }
        }
    }
}

class WordCountReducer: Reducer<MyOwnKeyClass, SInt, MyOwnKeyClass, SInt>
(MyOwnKeyClass::class, SInt::class, MyOwnKeyClass::class, SInt::class) {

    override fun reduce(key: MyOwnKeyClass, values: Sequence<SInt>, context: NodeContext<MyOwnKeyClass, SInt>) {
        context.output(key, values.fold(SInt.zero, SInt::plus))
    }
}

class WordCountApp(private val inputDir: Path,
                   private val outputDir: Path): ApplicationMaster("localhost", DEFAULT_REGISTRY_PORT) {
    private val jobComplete = CountDownLatch(1)
    private var success = false

    override fun run() {
        val config = JobConfig.create(
                WordCountMapper::class.java,
                WordCountReducer::class.java,
                inputDir.toString(),
                outputDir.toString()
        )
        println("Sending job config ...")
        val id = startJob(config)
        println("Job config sent: job id = $id")

        jobComplete.await()
        if (success) {
            println("Success")
            exitProcess(0)
        } else {
            println("Fail")
            exitProcess(3)
        }
    }

    override fun jobComplete(id: JobId) {
        success = true
        jobComplete.countDown()
    }

    override fun jobFailed(id: JobId) {
        success = false
        jobComplete.countDown()
    }
}

object WordCountMain {
    @JvmStatic
    fun main(args: Array<String>) {
        if (args.size != 2) {
            println("Expected 2 parameters: inputDir outputDir")
            exitProcess(2)
        }
        println("WordCountMain started")
        val inputDir = Paths.get(args[0])
        val outputDir = Paths.get(args[1])

        Files.createDirectories(outputDir.parent)
        FileUtils.deleteDirectory(outputDir.toFile())
        WordCountApp(inputDir, outputDir).run()
    }

}
