import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import net.dv8tion.jda.api.JDABuilder
import net.dv8tion.jda.api.entities.channel.concrete.ForumChannel
import net.dv8tion.jda.api.entities.channel.concrete.TextChannel
import net.dv8tion.jda.api.requests.GatewayIntent
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.time.ExperimentalTime

private val logger = LoggerFactory.getLogger("Main")



fun main() {
    println("Enter token")
    val token = readln()
    val jda = JDABuilder.createDefault(token)
        .enableIntents(GatewayIntent.MESSAGE_CONTENT, GatewayIntent.GUILD_MEMBERS)
        .build()

    jda.awaitReady()

    val lastYear = LocalDate
        .of(2022, 12, 31)
        .atTime(23, 59, 59)
        .toInstant(ZoneOffset.UTC)

    GlobalScope.async {
        val devden = jda.getGuildById(SERVER_ID)!!

        val staffUsers = devden.findMembersWithRoles(devden.getRoleById(STAFF_ROLE_ID)!!)
            .await()
            .map { it.user }
            .toSet()
        logger.info("Staff: {}", staffUsers.map { it.name })

        val index = getAllMessages(devden, lastYear)

        val intros = async {
            getAllMessages(
                devden.getChannelById(TextChannel::class.java, 888142541592076347)!!,
                lastYear
            )
        }

        val staffMentions = async {
            getAllMessages(
                devden,
                lastYear
            ) { m -> m.mentions.users.any { it in staffUsers } }
        }

        val showcaseReactions = async {
            val showcases = getThreads(
                devden.getChannelById(ForumChannel::class.java, 1115235973622661150)!!,
                lastYear
            )
            showcases.size to showcases.sumOf { it.retrieveStartMessage().await().reactions.size }
        }

        val starboardReactions = async {
            getAllMessages(
                devden.getChannelById(TextChannel::class.java, 975786395211816980)!!,
                lastYear
            ).mapToInt { it.reactions.size }.sum()
        }

        val notAllowedEmojis = async {
            val emojis = setOf(
                "<:norust:981216362267562024>",
                "<:nokotlin:1039633895173398558>",
                "<:nokotlin:936218114411085844>",
                "<:nosemicolon:936219580462948392>",
                "<:nowindows:1001064848509112360>",
                "<:nopython:936219553275445260>",
                "<:nocpp:936219632346497024>",
                "<:nocsharp:936219620807942146>"
            )
            getAllMessages(
                devden,
                lastYear
            ) { m ->
                m.mentions.customEmojis.any { it.asMention in emojis } ||
                        emojis.any { it in m.contentRaw }
            }
        }

        val forumPosts = async {
            SUPPORT_FORUMS
                .map { devden.getChannelById(ForumChannel::class.java, it)!! }
                .flatMap { getThreads(it, lastYear) }
                .size
        }


        logger.info(
            """
            Found ${intros.await().count()} intros
            Found ${staffMentions.await().size} staff mentions
            Found ${index.size} overall messages
            Found ${showcaseReactions.await().second} showcase reactions across ${showcaseReactions.await().first} messages
            Found ${starboardReactions.await()} starboard reactions
            Found ${notAllowedEmojis.await().size} messages with not allowed emojis
            Found ${forumPosts.await()} forum posts
            """.trimIndent()
        )
    }.let {
        runBlocking {
            it.await()
        }
    }


}