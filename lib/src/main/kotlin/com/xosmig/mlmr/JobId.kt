package com.xosmig.mlmr

import java.io.Serializable
import java.util.concurrent.atomic.AtomicLong

@kotlinx.serialization.Serializable
class JobId private constructor(val value: Long) : Serializable {

    class Generator {
        private val nextId = AtomicLong(1)
        fun next(): JobId = JobId(nextId.getAndIncrement())
    }

    override fun equals(other: Any?): Boolean = this === other || (other is JobId && value == other.value)
    override fun hashCode(): Int = value.hashCode()
    override fun toString(): String = "#$value"
}

@kotlinx.serialization.Serializable
class WorkerId private constructor(val value: Long) : Serializable {
    class Generator {
        private val nextId = AtomicLong(1)
        fun next(): WorkerId = WorkerId(nextId.getAndIncrement())
    }

    override fun equals(other: Any?): Boolean = this === other || (other is WorkerId && value == other.value)
    override fun hashCode(): Int = value.hashCode()
    override fun toString(): String = "#$value"
}

