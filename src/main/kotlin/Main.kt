import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import net.dv8tion.jda.api.JDABuilder
import net.dv8tion.jda.api.requests.GatewayIntent
import net.dv8tion.jda.api.utils.ChunkingFilter
import net.dv8tion.jda.api.utils.MemberCachePolicy
import net.dv8tion.jda.api.utils.cache.CacheFlag
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.ZoneOffset

private val logger = LoggerFactory.getLogger("Main")


fun main() {
    println("Enter token")
    val token = readln()
    val jda = JDABuilder.createDefault(token)
        .enableIntents(GatewayIntent.MESSAGE_CONTENT, GatewayIntent.GUILD_MEMBERS)
        .setChunkingFilter(ChunkingFilter.NONE)
        .setMemberCachePolicy(MemberCachePolicy.NONE)
        .disableCache(
            CacheFlag.ACTIVITY,
            CacheFlag.CLIENT_STATUS,
            CacheFlag.VOICE_STATE,
            CacheFlag.EMOJI,
            CacheFlag.STICKER
        )
        .build()

    jda.awaitReady()

    val lastYear = LocalDate
        .of(2024, 12, 31)
        .atTime(23, 59, 59)
        .toInstant(ZoneOffset.UTC)

    val until = LocalDate
        .of(2025, 12, 31)
        .atTime(23, 59, 59)
        .toInstant(ZoneOffset.UTC)

    val devden = jda.getGuildById(SERVER_ID)!!

    val mainScope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    // Launch the finder
    val job = mainScope.launch {
        MessageFinder.run(devden, lastYear, until)
    }

    Runtime.getRuntime().addShutdownHook(Thread {
        if (job.isActive) {
            println("\n\n⚠️ INTERRUPT SIGNAL RECEIVED. FLUSHING DB... DO NOT KILL PROCESS ⚠️\n")

            // 1. Cancel the job (stops crawlers)
            job.cancel()

            // 2. Block the Shutdown Hook thread until cleanups finish
            // We use runBlocking here because we are inside a Java Thread, not a coroutine
            runBlocking {
                job.join()
            }
            println("✅ Graceful shutdown complete.")
        }
    })

    runBlocking {
        job.join()
    }
}