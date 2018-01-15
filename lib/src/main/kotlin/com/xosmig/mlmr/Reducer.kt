package com.xosmig.mlmr

sealed class Reducer

abstract class KeyValueReducer<IK: Any, IV: Any, OK: Any, OV: Any> {
    abstract fun reduce(key: IK, values: Iterable<IV>, context: NodeContext<OK, OV>)
}
