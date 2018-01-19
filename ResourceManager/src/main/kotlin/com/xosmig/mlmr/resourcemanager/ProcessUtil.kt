package com.xosmig.mlmr.resourcemanager

import java.io.File
import java.io.IOException

@Throws(IOException::class)
internal fun startProcess(klass: Class<*>, vararg args: String): Process {
    val javaHome = System.getProperty("java.home")
    val javaBin = javaHome + File.separator + "bin" + File.separator + "java"
    val classpath = System.getProperty("java.class.path")
    val className = klass.canonicalName

    val builder = ProcessBuilder(javaBin, "-classpath", classpath, className, *args)

    return builder.start()
}
