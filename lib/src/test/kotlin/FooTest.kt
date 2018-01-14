import com.natpryce.hamkrest.assertion.assert
import com.natpryce.hamkrest.equalTo
import org.junit.Test

class FooTest {
    @Test
    fun fooTest() {
        assert.that(2 + 2, equalTo(4))
    }
}
