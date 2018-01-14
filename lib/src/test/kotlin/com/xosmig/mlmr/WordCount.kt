package com.xosmig.mlmr

import java.io.InputStream
import java.util.*

class WordCountMapper: InputStreamMapper<SString, SInt>() {
    override fun map(input: InputStream, context: MapperContext<SString, SInt>) {
        Scanner(input).use { scanner ->
            for (token in scanner) {
                context.output(SString(token), SInt.one)
            }
        }
    }
}
