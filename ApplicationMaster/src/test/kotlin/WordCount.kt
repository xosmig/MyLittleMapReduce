import com.xosmig.mlmr.*
import com.xosmig.mlmr.applicationmaster.ApplicationMaster
import com.xosmig.mlmr.applicationmaster.JobConfig
import java.io.InputStream

class WordCountMapper: InputStreamMapper<SString, SInt>(SString::class, SInt::class) {
    override fun map(input: InputStream, context: NodeContext<SString, SInt>) {
        TODO()
    }
}

class WordCountApp: ApplicationMaster() {
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
    WordCountApp().run()
}
