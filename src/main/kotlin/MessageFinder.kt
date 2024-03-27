import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flattenConcat
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.stream.consumeAsFlow
import net.dv8tion.jda.api.entities.Guild
import net.dv8tion.jda.api.entities.Message
import net.dv8tion.jda.api.entities.channel.attribute.IThreadContainer
import net.dv8tion.jda.api.entities.channel.concrete.ForumChannel
import net.dv8tion.jda.api.entities.channel.concrete.ThreadChannel
import net.dv8tion.jda.api.entities.channel.middleman.MessageChannel
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Stream
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

private val cache = ConcurrentHashMap<Long, List<Message>>()

private val threadCache = ConcurrentHashMap<Long, Set<ThreadChannel>>()

private val logger = LoggerFactory.getLogger("MessageFinder")

private suspend fun IThreadContainer.getAllThreads(): Set<ThreadChannel> {
    val defaults = threadChannels union retrieveArchivedPublicThreadChannels().await()
    // forum channel cant have private threads
    return if (this is ForumChannel) defaults else defaults union retrieveArchivedPrivateThreadChannels().await()
}


suspend fun getThreads(
    entity: IThreadContainer,
    since: Instant,
    filter: (ThreadChannel) -> Boolean = { true }
): Set<ThreadChannel> = threadCache.getOrPut(entity.idLong) {
    entity.getAllThreads()
        .filter { it.timeCreated.toInstant().isAfter(since) }
        .filter(filter)
        .toSet()
}



suspend fun getAllMessages(
    entity: MessageChannel,
    since: Instant,
    filter: (Message) -> Boolean = { true }
): Stream<Message> = cache[entity.idLong]?.parallelStream() ?: run {

    val x = coroutineScope {
        val (all, time) = measureTimedValue {
            logger.info("Fetching messages for ${entity.name}")

            val messages = async {
                entity.iterableHistory
                    .parallelStream()
                    .takeWhile {
                        it.timeCreated.toInstant().isAfter(since)
                    }
                    .filter(filter)
            }
            val threads = async {
                if (entity is IThreadContainer) getThreads(entity, since)
                    .parallelStream()
                    .flatMap {
                        runBlocking {
                            getAllMessages(it, since, filter)
                        }
                    }
                else Stream.of()
            }


            Stream.concat(messages.await(), threads.await())
                .toList()
        }
        logger.info("Found ${all.size} matching messages for ${entity.name} in $time")
        all
    }
    cache[entity.idLong] = x
    x.parallelStream()
}

@OptIn(FlowPreview::class)
suspend fun getAllMessages(guild: Guild, until: Instant, filter: (Message) -> Boolean = { true }): List<Message> {
    val channels = guild.textChannels

    val messages = channels
        .parallelStream()
        .consumeAsFlow()

        .filter { c -> c.parentCategoryIdLong !in SKIP_CATEGORIES }
        .mapParallel(scope) {
            getAllMessages(it, until, filter)
                .consumeAsFlow()
        }
        .flattenConcat()

    return messages.toList()
}

