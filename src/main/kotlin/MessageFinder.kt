import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import net.dv8tion.jda.api.entities.Guild
import net.dv8tion.jda.api.entities.Message
import net.dv8tion.jda.api.entities.MessageType
import net.dv8tion.jda.api.entities.channel.attribute.IThreadContainer
import net.dv8tion.jda.api.entities.channel.concrete.ThreadChannel
import net.dv8tion.jda.api.entities.channel.middleman.MessageChannel
import net.dv8tion.jda.api.requests.restaction.pagination.MessagePaginationAction
import org.slf4j.LoggerFactory
import java.sql.DriverManager
import java.sql.Types
import java.time.Instant
import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicInteger

data class MessageData(
    val id: Long,
    val authorId: Long,
    val channelId: Long,
    val content: String,
    val timestamp: Instant,
    val typeId: MessageType,
    val isThreadStarter: Boolean, // did this message start a thread?
    val attachmentCount: Int, // number of attachments
    val referencedMessageId: Long?, // replied to message ID
    val mentionedRoles: List<Long>, // mentioned role IDs
    val mentionedUsers: List<Long>, // mentioned user IDs
    val reactions: List<ReactionData>,
    val embeds: List<String> // embeds as json
)

data class ReactionData(
    val name: String,
    val count: Int
)


class DuckDBManager(private val dbPath: String = "server_stats.duckdb") {
    private val writerConn = DriverManager.getConnection("jdbc:duckdb:$dbPath")
    private val logger = LoggerFactory.getLogger(DuckDBManager::class.java)

    init {
        writerConn.createStatement().execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id BIGINT PRIMARY KEY,
                author_id BIGINT,
                channel_id BIGINT,
                content VARCHAR,
                timestamp TIMESTAMP,
                type_id INTEGER,
                is_thread_starter BOOLEAN,
                attachment_count INTEGER,
                referenced_message_id BIGINT,
                mentioned_roles BIGINT[], 
                mentioned_users BIGINT[],
                reactions STRUCT(name VARCHAR, count INTEGER)[],
                embeds JSON[]
            )
        """
        )

        writerConn.autoCommit = false
    }

    fun close() {
        try {
            if (!writerConn.isClosed) {
                writerConn.close()
                logger.info("Database connection closed cleanly.")
            }
        } catch (e: Exception) {
            logger.error("Error closing database", e)
        }
    }

    // Returns (Oldest_ID, Newest_ID) or null if empty
    fun getChannelBounds(channelId: Long): Pair<Long, Long>? {
        // Open a temporary connection just for this check
        DriverManager.getConnection("jdbc:duckdb:$dbPath").use { readConn ->
            val sql = "SELECT MIN(id), MAX(id) FROM messages WHERE channel_id = ?"
            readConn.prepareStatement(sql).use { stmt ->
                stmt.setLong(1, channelId)
                val rs = stmt.executeQuery()
                if (rs.next()) {
                    val min = rs.getLong(1)
                    val max = rs.getLong(2)
                    if (min == 0L || max == 0L) return null
                    return Pair(min, max)
                }
            }
        }
        return null
    }

    private val insertStmt = writerConn.prepareStatement(
        """
            INSERT INTO messages (
                id, author_id, channel_id, content, timestamp, type_id, is_thread_starter,
                attachment_count, referenced_message_id, mentioned_roles, mentioned_users, reactions, embeds
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::STRUCT(name VARCHAR, count INTEGER)[], from_json(?, '["JSON"]'))
            ON CONFLICT (id) DO NOTHING
        """
    )


    @Synchronized
    fun insertBatch(batch: List<MessageData>) {
        if (batch.isEmpty()) return
        val start = System.currentTimeMillis()
        try {
            for (msg in batch) {
                insertStmt.setLong(1, msg.id)
                insertStmt.setLong(2, msg.authorId)
                insertStmt.setLong(3, msg.channelId)
                insertStmt.setString(4, msg.content)
                insertStmt.setObject(5, OffsetDateTime.ofInstant(msg.timestamp, OffsetDateTime.now().offset))
                insertStmt.setInt(6, msg.typeId.ordinal)
                insertStmt.setBoolean(7, msg.isThreadStarter)
                insertStmt.setInt(8, msg.attachmentCount)
                if (msg.referencedMessageId != null) {
                    insertStmt.setLong(9, msg.referencedMessageId)
                } else {
                    insertStmt.setNull(9, Types.BIGINT)
                }
                insertStmt.setArray(10, writerConn.createArrayOf("BIGINT", msg.mentionedRoles.toTypedArray()))
                insertStmt.setArray(11, writerConn.createArrayOf("BIGINT", msg.mentionedUsers.toTypedArray()))

                if (msg.reactions.isEmpty()) {
                    insertStmt.setString(12, "[]")
                } else {
                    // We want to generate: [{'name': 'star', 'count': 5}, {'name': 'heart', 'count': 1}]
                    val structString = msg.reactions.joinToString(prefix = "[", postfix = "]") { r ->
                        // 1. Escape single quotes for SQL (' -> '')
                        val safeName = r.name.replace("'", "''")

                        // 2. Use the DuckDB Struct Literal syntax: {'key': 'value'}
                        "{'name': '$safeName', 'count': ${r.count}}"
                    }

                    // Pass as a simple String
                    insertStmt.setString(12, structString)
                }
                val jsonArrayString = if (msg.embeds.isEmpty()) {
                    "[]"
                } else {
                    msg.embeds.joinToString(separator = ",", prefix = "[", postfix = "]")
                }
                insertStmt.setString(13, jsonArrayString)

                insertStmt.addBatch()
            }
            insertStmt.executeBatch()
            writerConn.commit()
            val duration = System.currentTimeMillis() - start


            if (duration > 1000) {
                logger.warn("Slow insert! Batch of ${batch.size} took ${duration}ms")
            } else {
                logger.debug("Inserted ${batch.size} messages in ${duration}ms")
            }
        } catch (e: Exception) {
            logger.error("Failed to insert batch of ${batch.size} messages!", e)
            try {
                writerConn.rollback()
            } catch (e2: Exception) {
                logger.error("Rollback failed", e2)
            }
            throw e
        }
    }
}

object MessageFinder {
    private val logger = LoggerFactory.getLogger(MessageFinder::class.java)

    suspend fun run(guild: Guild, since: Instant, until : Instant?) = coroutineScope {
        val db = DuckDBManager()
        val totalProcessed = AtomicInteger(0)
        val writeChannel = Channel<List<MessageData>>(capacity = 50)
        val startTime = System.currentTimeMillis()

        val tickerJob = launch(Dispatchers.IO) {
            var lastCount = 0
            while (isActive) {
                delay(5000) // Wait 5s
                val current = totalProcessed.get()
                val diff = current - lastCount
                val speed = diff / 5
                val elapsed = (System.currentTimeMillis() - startTime) / 1000

                logger.info("STATS: $current msgs | Speed: $speed/sec | Time: ${elapsed}s")
                lastCount = current
            }
        }

        val writerJob = launch(Dispatchers.IO) {
            for (batch in writeChannel) {
                db.insertBatch(batch)
                val current = totalProcessed.addAndGet(batch.size)
                if (current % 10000 == 0) logger.info("Persisted $current messages...")
            }
        }
        val semaphore = Semaphore(20)

        val allChannels = guild.textChannels + guild.forumChannels + guild.voiceChannels

        logger.info("Starting crawl on ${allChannels.size} channels...")
        try {
            val jobs = allChannels.map { channel ->
                launch(Dispatchers.IO) {
                    semaphore.withPermit {

                        if (channel is MessageChannel) {
                            crawlSingleChannel(db, channel, since, until, writeChannel)
                        }


                        if (channel is IThreadContainer) {
                            val threads = getThreadsSafe(channel, since)
                            threads.forEach { thread ->
                                crawlSingleChannel(db, thread, since, until,writeChannel)
                            }
                        }
                    }
                }
            }

            jobs.joinAll()
            tickerJob.cancel()
            writeChannel.close()
            writerJob.join()

            logger.info("Crawl Complete! Total Messages: ${totalProcessed.get()}")
        } catch (e: CancellationException) {
            logger.warn("🛑 Crawl cancelled! Flushing remaining data...")
        } catch (e: Exception) {
            logger.error("💥 Crawl crashed!", e)
        } finally {
            logger.info("Change in power detected. Shutting down systems...")

            tickerJob.cancel()

            writeChannel.close()

            withContext(NonCancellable) {
                logger.info("Waiting for database writer to finish...")
                writerJob.join()
                db.close()
            }

            logger.info("✅ DONE! Total Persisted: ${totalProcessed.get()}")
        }
    }

    private suspend fun crawlSingleChannel(
        db: DuckDBManager,
        channel: MessageChannel,
        since: Instant,
        until: Instant?,
        output: Channel<List<MessageData>>
    ) {
        logger.info("Crawling channel ${channel.name} (${channel.id})")

        val bounds = db.getChannelBounds(channel.idLong)
        if (bounds == null) {
            logger.info("New Channel detected: ${channel.name}")
            crawlRange(channel.iterableHistory, since, until, null, output)
            return
        }

        val (oldestIdInDb, newestIdInDb) = bounds

        logger.info("Catching up on #${channel.name} (Stopping at ID $newestIdInDb)")
        crawlRange(channel.iterableHistory, since, until, newestIdInDb, output)

        logger.info("Resuming history on #${channel.name} (Starting before ID $oldestIdInDb)")
        try {
            val history = channel.iterableHistory.skipTo(oldestIdInDb)
            crawlRange(history, since, until, null, output)
        } catch (e: Exception) {
            logger.warn("Could not resume history for #${channel.name}: ${e.message}")
        }
    }

    private suspend fun crawlRange(
        history: MessagePaginationAction,
        since: Instant,
        until: Instant?,
        stopAtId: Long?, // Stop if we hit this ID (overlap)
        output: Channel<List<MessageData>>
    ) {
        val batch = ArrayList<MessageData>(1000)
        val iterator = history.iterator()

        while (iterator.hasNext()) {
            val msg = iterator.next()

            // check we are still within the desired time range
            if (msg.timeCreated.toInstant().isBefore(since)) break

            if (until != null && msg.timeCreated.toInstant().isAfter(until)) break

            // check for overlap
            if (stopAtId != null && msg.idLong <= stopAtId) {
                // reached already known messages
                break
            }

            batch.add(mapToData(msg))

            if (batch.size >= 1000) {
                logger.info("Sending batch of ${batch.size} messages from channel ${msg.channel}")
                output.send(ArrayList(batch))
                batch.clear()
            }
        }

        logger.info("Finished crawling channel ${history.channel.id}, sending final batch of ${batch.size} messages")
        if (batch.isNotEmpty()) output.send(batch)
    }

    private fun mapToData(msg: Message): MessageData {
        val isThreadStarter = msg.channel is ThreadChannel
                && (msg.idLong == msg.channel.idLong)
        return MessageData(
            id = msg.idLong,
            authorId = msg.author.idLong,
            channelId = msg.channel.idLong,
            content = msg.contentRaw,
            timestamp = msg.timeCreated.toInstant(),
            typeId = msg.type,
            isThreadStarter = isThreadStarter,
            attachmentCount = msg.attachments.size,
            referencedMessageId = msg.referencedMessage?.idLong,
            mentionedRoles = msg.mentions.roles.map { it.idLong },
            mentionedUsers = msg.mentions.users.map { it.idLong },
            reactions = msg.reactions.map { ReactionData(it.emoji.name, it.count) },
            embeds = msg.embeds.map { it.toData().toJson().toString(Charsets.UTF_8) }
        )
    }

    private suspend fun getThreadsSafe(container: IThreadContainer, since: Instant): List<MessageChannel> {
        logger.info("Retrieving threads for channel ${container.id}")
        return try {
            val active = container.threadChannels
            val archivedPublic = container.retrieveArchivedPublicThreadChannels().await()
            val archivedPrivate = runCatching {
                container.retrieveArchivedPrivateThreadChannels().await()
            }.getOrDefault(emptyList())

            logger.info("Found ${active.size} active, ${archivedPublic.size} archived public, ${archivedPrivate.size} archived private threads in channel ${container.id}")

            (active + archivedPublic + archivedPrivate)
                .filter { it.timeCreated.toInstant().isAfter(since) }
        } catch (_: Exception) {
            logger.warn("Failed to retrieve threads for channel ${container.id}, skipping.")
            emptyList()
        }
    }
}



