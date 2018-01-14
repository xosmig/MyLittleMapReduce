package com.xosmig.mlmr

import java.io.InputStream
import java.util.*

class WordCountMapper: InputStreamMapper<String, Int> {
    override fun map(input: InputStream, context: MapperContext<String, Int>) {
        Scanner(input).use { scanner ->
            for (token in scanner) {
                context.output(token, 1)
            }
        }
    }
}

class WordCountReducer: Reducer<String, Int, String, Int> {
    override fun reduce(key: String, values: Iterable<Int>, context: ReducerContext<String, Int>) {
        context.output(key, values.count())
    }
}
