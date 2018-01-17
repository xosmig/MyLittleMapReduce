package com.xosmig.mlmr

import java.util.concurrent.atomic.AtomicLong

class JobId private constructor(val value: Long) {

    class Generator {
        private val nextId = AtomicLong(1)
        fun next(): JobId = JobId(nextId.getAndIncrement())
    }

    override fun equals(other: Any?): Boolean = this === other || (other is JobId && value == other.value)
    override fun hashCode(): Int = value.hashCode()

}

class WorkerId private constructor(val value: Long) {

    class Generator {
        private val nextId = AtomicLong(1)
        fun next(): WorkerId = WorkerId(nextId.getAndIncrement())
    }

    override fun equals(other: Any?): Boolean = this === other || (other is WorkerId && value == other.value)
    override fun hashCode(): Int = value.hashCode()
}

