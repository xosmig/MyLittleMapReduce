package com.xosmig.mlmr

import java.io.InputStream

interface MapperContext<OK, OV> {
    fun output(key: OK, value: OV)
}

interface MapperBase<OK, OV> {
    fun map(input: InputStream, context: MapperContext<OK, OV>)
}

interface KeyValueMapper<IK, IV, OK, OV>: MapperBase<OK, OV> {
    fun map(key: IK, value: IV, context: MapperContext<OK, OV>)

    override fun map(input: InputStream, context: MapperContext<OK, OV>) {
        // TODO
    }
}

interface InputStreamMapper<OK, OV>: MapperBase<OK, OV>
