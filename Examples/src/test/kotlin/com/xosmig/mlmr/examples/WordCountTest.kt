package com.xosmig.mlmr.examples

import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import com.xosmig.mlmr.util.startProcess
import com.xosmig.mlmr.util.startThread
import org.apache.commons.io.FileUtils
import org.junit.Test
import java.io.*


const val INPUT_DIR = "src/main/resources/WordCount/input"
const val OUTPUT_DIR_1 = "WordCount/test-output/WordCount1"
const val OUTPUT_DIR_2 = "WordCount/test-output/WordCount2"
const val EXPECTED_OUTPUT_PATH = "src/test/resources/WordCount/ExpectedOutput.txt"

class WordCountTest {
    @Test
    fun wordCountTest() {
        val rmProcess = startProcess(com.xosmig.mlmr.resourcemanager.Main.javaClass)
        printProcessOutput(rmProcess, "RM")
        val healthCheckThread =  watchProcessHealth(rmProcess)

        Thread.sleep(3 * 1000)

        val wcProcess1 = startProcess(com.xosmig.mlmr.examples.WordCountMain.javaClass, INPUT_DIR, OUTPUT_DIR_1)
        printProcessOutput(wcProcess1, "WC1")
        val wcProcess2 = startProcess(com.xosmig.mlmr.examples.WordCountMain.javaClass, INPUT_DIR, OUTPUT_DIR_2)
        printProcessOutput(wcProcess2, "WC2")

        assert.that(wcProcess1.waitFor(), equalTo(0))
        assert.that(wcProcess2.waitFor(), equalTo(0))

        healthCheckThread.interrupt()
        healthCheckThread.join()
        rmProcess.destroy()

        val expectedOutput = File(EXPECTED_OUTPUT_PATH).reader()
                .readLines()
                .chunked(2)
        checkOutput(File(OUTPUT_DIR_1), expectedOutput)
        checkOutput(File(OUTPUT_DIR_2), expectedOutput)
    }

    private fun checkOutput(dir: File, expectedOutput: Iterable<List<String>>) {
        // Filter out log directories
        val actual = FileUtils.listFiles(dir, null, false)
                .map { it.reader().readLines() }
        assert.that(actual.toSet(), equalTo(expectedOutput.toSet()))
    }

    private fun printProcessOutput(process: Process, prefix: String) = startThread {
        BufferedReader(InputStreamReader(process.inputStream)).use { rmOut ->
            while (true) {
                val line = rmOut.readLine() ?: break
                println("$prefix: $line")
                System.out.flush()
            }
        }
    }

    private fun watchProcessHealth(process: Process) = startThread {
        while (!Thread.interrupted()) {
            if (!process.isAlive) {
                throw Exception("Process is dead")
            }
        }
    }
}
