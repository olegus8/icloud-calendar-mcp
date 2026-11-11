package org.onekash.mcp.calendar.integration

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.File
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class StdioLifecycleTest {

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    fun `packaged server serves requests until stdin closes then exits`() {
        withServer { process ->
            val input = process.outputStream.bufferedWriter()
            val output = process.inputStream.bufferedReader()

            fun send(message: String) {
                input.write(message)
                input.newLine()
                input.flush()
            }

            fun reply(id: Int) {
                val line = CompletableFuture.supplyAsync {
                    output.readLine()
                }.get(5, TimeUnit.SECONDS)
                assertNotNull(line, "server closed stdout before replying")
                val message = Json.parseToJsonElement(line).jsonObject
                assertEquals(
                    "2.0", message["jsonrpc"]?.jsonPrimitive?.content
                )
                assertEquals("$id", message["id"]?.jsonPrimitive?.content)
                assertNotNull(message["result"], line)
            }

            send(
                """{"jsonrpc":"2.0","id":1,"method":"initialize",""" +
                    """"params":{""" +
                    """"protocolVersion":"2025-06-18","capabilities":{},""" +
                    """"clientInfo":{"name":"stdio-lifecycle-test",""" +
                    """"version":"1.0.0"}}}"""
            )
            reply(1)
            send(
                """{"jsonrpc":"2.0","method":"notifications/initialized"}"""
            )
            send("""{"jsonrpc":"2.0","id":2,"method":"tools/list"}""")
            reply(2)
            assertTrue(process.isAlive, "server exited while stdin was open")

            input.close()
            assertTrue(
                process.waitFor(5, TimeUnit.SECONDS),
                "server did not exit after stdin closed"
            )
            assertEquals(0, process.exitValue())
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    fun `packaged server exits when stdin closes before any request`() {
        withServer { process ->
            process.outputStream.close()
            assertTrue(
                process.waitFor(15, TimeUnit.SECONDS),
                "server did not exit after stdin closed"
            )
            assertEquals(0, process.exitValue())
        }
    }

    private fun withServer(block: (Process) -> Unit) {
        val javaBin = File(
            File(System.getProperty("java.home"), "bin"), "java"
        ).absolutePath
        val builder = ProcessBuilder(
            javaBin, "-jar", System.getProperty("calendar.jar")
        ).redirectError(ProcessBuilder.Redirect.DISCARD)
        builder.environment().apply {
            remove("ICLOUD_USERNAME")
            remove("ICLOUD_PASSWORD")
            remove("ICLOUD_APP_PASSWORD")
        }
        val process = builder.start()
        try {
            block(process)
        } finally {
            if (process.isAlive) {
                process.destroyForcibly()
                process.waitFor(5, TimeUnit.SECONDS)
            }
        }
    }
}
