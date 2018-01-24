package com.xosmig.mlmr.util

fun <T> List<Sequence<T>>.chainAll(): Sequence<T> = asSequence().flatten()
