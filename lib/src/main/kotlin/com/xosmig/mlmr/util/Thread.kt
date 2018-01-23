package com.xosmig.mlmr.util

import kotlin.concurrent.thread

fun startThread(block: () -> Unit): Thread = thread(start = true, block = block)
