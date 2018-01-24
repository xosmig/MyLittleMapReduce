import kotlinx.io.ByteBuffer
import kotlinx.serialization.KSerializer
import kotlinx.serialization.cbor.CBOR
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream

@Throws(IOException::class)
fun <T : Any> InputStream.readCBORObject(serializer: KSerializer<T>): T? {
    val objSizeBuf = ByteBuffer.allocate(4)
    val read = read(objSizeBuf.array())
    if (read <= 0) {
        return null  // end of stream
    }
    if (read < 4) {
        throw IOException("Invalid stream format: not a CBOR stream")
    }
    val objSize = objSizeBuf.getInt()

    val objectBuf = ByteArray(objSize)
    if (read(objectBuf) < objSize) {
        throw IOException("Invalid stream format: not a CBOR stream")
    }
    return CBOR.load(serializer, objectBuf)
}

@Throws(IOException::class)
fun <T : Any> OutputStream.writeCBORObject(obj: T, serializer: KSerializer<T>) {
    val objectBuf = CBOR.dump(serializer, obj)
    val objSizeBuf = ByteBuffer.allocate(4).putInt(objectBuf.size)
    write(objSizeBuf.array())
    write(objectBuf)
}
