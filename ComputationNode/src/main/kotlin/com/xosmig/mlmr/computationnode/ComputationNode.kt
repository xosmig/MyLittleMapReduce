package com.xosmig.mlmr.computationnode

import com.xosmig.mlmr.*
import java.net.URLClassLoader

private fun loadClass(classRef: ClassRef): Class<*> {
    val classLoader = URLClassLoader(arrayOf(classRef.classPath))
    return classLoader.loadClass(classRef.className)
}

class ComputationNode: ComputationNodeRmi {
    override fun startMap(config: MapConfig) {
        val mapperClass = loadClass(config.mapper)
        val mapper = mapperClass.newInstance() as Mapper
        // TODO
        mapper.map(System.`in`, StdoutContext())
    }
}

fun main(args: Array<String>) {

}
