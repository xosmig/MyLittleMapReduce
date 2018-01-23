package com.xosmig.mlmr

import kotlinx.serialization.Serializable

// Kotlin serialization doesn't seem to work with strings and primitive types
// Thus, have to wrap them in data classes

@Serializable
data class SInt(val value: Int) {
    companion object {
        val zero = SInt(0)
        val one = SInt(1)
    }

    override fun toString(): String = value.toString()
    operator fun plus(other: SInt) = SInt(value + other.value)
}

@Serializable
data class SString(val value: String) {
    override fun toString(): String = value
}
