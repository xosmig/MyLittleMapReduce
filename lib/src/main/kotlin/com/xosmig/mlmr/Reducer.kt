package com.xosmig.mlmr

interface ReducerContext<OK, OV> {
    fun output(key: OK, value: OV)
}

// TODO: probably, should be an abstract class, just like Mapper
interface Reducer<IK, IV, OK, OV> {
    fun reduce(key: IK, values: Iterable<IV>, context: ReducerContext<OK, OV>)
}
