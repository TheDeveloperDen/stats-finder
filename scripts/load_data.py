import re
import sys
from contextlib import contextmanager
from typing import Generator, List, Tuple

import duckdb
# New imports for WordCloud
import matplotlib.pyplot as plt
import questionary
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from wordcloud import WordCloud, STOPWORDS

# Config
DB_PATH = "../server_stats.duckdb"
console = Console()

IGNORE_IDS = [904478222455029821, 1211781489931452447, 510789298321096704, 302050872383242240,
              # devdenbot, wordle bot, texit, disboard
              1346858090078666852, 815018615434641428, 1204801693448146974, 1405032473972183130  # random scammers
    , 1021235469742247936]  # people who arent in the server anymore
IGNORE_STR = ", ".join(str(id) for id in IGNORE_IDS)

print(IGNORE_STR)

TARGET_YEAR = 2025

CHANNELS = {
    "random": 932661343520194640,
    "introductions": 888142541592076347,
    "showcase": 1115235973622661150,
    "starboard": 975786395211816980
}

STAFF_IDS = [
    302897522194513931,  # Admin 1
    266973575225933824,  # Mod 1
    199036109760495616,
    245994206965792780
]

QUERIES = {
    "general": f"""
               SELECT COUNT(*)                                                       as total_msgs,
                      COUNT(DISTINCT author_id)                                      as users,
                      SUM(attachment_count)                                          as files,
                      SUM(len(content))                                              as chars,
                      (SELECT COUNT(*) FROM messages 
                       WHERE is_thread_starter = TRUE 
                       AND author_id NOT IN ({IGNORE_STR}))                          as threads,
                      CAST(AVG(len(content)) AS INTEGER)                             as avg_len
               FROM messages
               WHERE author_id NOT IN ({IGNORE_STR})
               """,
    "reactions": f"""
                 WITH expanded AS (
                    SELECT unnest(reactions) as r 
                    FROM messages 
                    WHERE author_id NOT IN ({IGNORE_STR})
                 )
                 SELECT r.name, SUM(r.count) as total
                 FROM expanded
                 GROUP BY r.name
                 ORDER BY total DESC
                 LIMIT 15
                 """,
    "hourly": f"""
              SELECT hour(timestamp) as h, COUNT(*) as c
              FROM messages
              WHERE author_id NOT IN ({IGNORE_STR})
              GROUP BY h
              ORDER BY h ASC
              """,
    "mentions": f"""
                WITH expanded AS (
                    SELECT author_id, unnest(mentioned_users) as target
                    FROM messages
                    WHERE author_id NOT IN ({IGNORE_STR})
                )
                SELECT author_id, target, COUNT(*) as count
                FROM expanded
                WHERE author_id != target 
                AND target NOT IN ({IGNORE_STR}) -- Don't count pings TO the bots either
                GROUP BY author_id, target
                ORDER BY count DESC
                LIMIT 10
                """,
    "yappers": f"""
               SELECT author_id, COUNT(*) as msgs, CAST(AVG(len(content)) AS INTEGER) as avg_len
               FROM messages
               WHERE author_id NOT IN ({IGNORE_STR})
               GROUP BY author_id
               ORDER BY msgs DESC
               LIMIT 10
               """,
    "ghosts": f"""
              SELECT author_id, COUNT(*) as msgs
              FROM messages
              WHERE author_id NOT IN ({IGNORE_STR})
              GROUP BY author_id
              HAVING msgs < 5
              ORDER BY msgs ASC
              LIMIT 10
              """,
    "weekend_warriors": f"""
                        SELECT CASE WHEN dayofweek(timestamp) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END as day_type,
                               COUNT(*)                                                                   as count,
                               CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM messages WHERE author_id NOT IN ({IGNORE_STR})) AS INTEGER) as pct
                        FROM messages
                        WHERE author_id NOT IN ({IGNORE_STR})
                        GROUP BY day_type
                        """,
    "files": f"""
             SELECT author_id, SUM(attachment_count) as total_files
             FROM messages
             WHERE author_id NOT IN ({IGNORE_STR})
             GROUP BY author_id
             HAVING total_files > 0
             ORDER BY total_files DESC
             LIMIT 10
             """,
    "wordcloud": f"""
             SELECT string_agg(content, ' ') 
             FROM messages 
             WHERE content IS NOT NULL 
             AND len(content) > 0
             AND author_id NOT IN ({IGNORE_STR})
             """
}


@contextmanager
def db_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Safe read-only context manager."""
    conn = None
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        yield conn
    except Exception as e:
        console.print(f"[bold red]DB Error:[/bold red] {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()


def print_banner():
    console.clear()
    console.print(Panel(
        Text("DevDen Stats", justify="center", style="bold cyan"),
        border_style="cyan",
        expand=False
    ))


def render_overview(res: Tuple):
    """Renders the main dashboard grid."""
    total, users, files, chars, threads, avg_len = res

    # Grid Layout
    grid = Table.grid(expand=True, padding=(0, 2))
    grid.add_column(justify="center", ratio=1)
    grid.add_column(justify="center", ratio=1)
    grid.add_column(justify="center", ratio=1)

    def metric_panel(val, label, color="green"):
        return Panel(
            f"[bold {color}]{val:,}[/]",
            title=f"[dim]{label}[/]",
            border_style="dim"
        )

    grid.add_row(
        metric_panel(total, "Messages", "cyan"),
        metric_panel(users, "Active Members", "yellow"),
        metric_panel(files, "Attachments", "magenta")
    )

    console.print(grid)
    console.print(f"\n[dim italic]avg msg len: {avg_len} chars • threads created: {threads}[/]\n", justify="center")


def render_table(title: str, columns: List[str], rows: List[Tuple]):
    """Generic table renderer."""
    t = Table(title=title, box=box.SIMPLE, title_style="bold")
    for col in columns:
        t.add_column(col)

    for row in rows:
        t.add_row(*[str(r) for r in row])

    console.print(t)


def render_bar_chart(title: str, data: List[Tuple]):
    """Renders an ASCII bar chart for time data."""
    console.print(f"[bold]{title}[/]")
    if not data: return

    max_val = max(r[1] for r in data)
    for label, val in data:
        bar_len = int((val / max_val) * 50)
        bar = "█" * bar_len
        # Format label (assuming integer hour 0-23)
        lbl_str = f"{label:02d}:00" if isinstance(label, int) else str(label)
        console.print(f" {lbl_str} [blue]{bar}[/] [dim]{val:,}[/]")
    console.print()


def show_deep_dive(con):
    """Calculates specific channel and staff stats for the target year."""
    console.print(f"[bold underline]🕵️ Deep Dive Stats ({TARGET_YEAR})[/]")

    # 1. Channel Specifics
    # We build a grid of stats for the specific channels defined in Config
    grid = Table.grid(expand=True, padding=(0, 2))
    grid.add_column(justify="left", ratio=1)
    grid.add_column(justify="right", ratio=1)

    # Helper to run count query
    def count_in_channel(cid, label):
        if not cid: return
        sql = f"SELECT COUNT(*) FROM messages WHERE channel_id = {cid} AND year(timestamp) = {TARGET_YEAR}"
        count = con.sql(sql).fetchone()[0]
        grid.add_row(f"{label}", f"[bold cyan]{count:,}[/]")

    count_in_channel(CHANNELS["random"], "🔥 Hot Takes (#random)")
    count_in_channel(CHANNELS["introductions"], "👋 New Intros (#introductions)")
    count_in_channel(CHANNELS["starboard"], "⭐ Starboard Entries")

    # Showcase is special (reactions)
    if CHANNELS["showcase"]:
        sql = f"""
            WITH posts AS (
                SELECT reactions FROM messages 
                WHERE channel_id = {CHANNELS["showcase"]} AND year(timestamp) = {TARGET_YEAR}
            ),
            reacts AS (SELECT unnest(reactions) as r FROM posts)
            SELECT (SELECT COUNT(*) FROM posts), SUM(r.count) FROM reacts
        """
        posts, reacts = con.sql(sql).fetchone()
        grid.add_row("💫 Showcase Activity", f"[bold magenta]{reacts or 0:,}[/] reacts on {posts or 0} posts")

    # 2. Staff Pings
    if STAFF_IDS:
        staff_str = ",".join(str(s) for s in STAFF_IDS)
        sql = f"""
            WITH mentions AS (
                SELECT unnest(mentioned_users) as target FROM messages
                WHERE year(timestamp) = {TARGET_YEAR}
            )
            SELECT COUNT(*) FROM mentions WHERE target IN ({staff_str})
        """
        pings = con.sql(sql).fetchone()[0]
        grid.add_row("🚨 Staff Pings", f"[bold red]{pings:,}[/]")

    # 3. Support Threads
    sql = f"""
        SELECT COUNT(*) FROM messages 
        WHERE is_thread_starter = TRUE 
        AND year(timestamp) = {TARGET_YEAR}
        AND author_id NOT IN ({IGNORE_STR})
    """
    threads = con.sql(sql).fetchone()[0]
    grid.add_row("🆘 Support Threads", f"[bold green]{threads:,}[/]")

    # 4. Code Blocks
    sql = f"SELECT COUNT(*) FROM messages WHERE content LIKE '%```%' AND year(timestamp) = {TARGET_YEAR}"
    code = con.sql(sql).fetchone()[0]
    grid.add_row("💻 Code Blocks Shared", f"[bold yellow]{code:,}[/]")

    console.print(Panel(grid, title="Yearly Specifics", border_style="cyan"))


def show_creative_stats(con):
    console.print(f"[bold underline]🧪 Experimental Stats ({TARGET_YEAR})[/]")
    table = Table(box=box.HEAVY_EDGE, show_header=True)
    table.add_column("Award Category", style="bold yellow")
    table.add_column("Winner", style="cyan")
    table.add_column("The Data", justify="right")

    # 1. THE RIDDLER (Highest % of messages containing '?')
    # Filter: Must have sent at least 100 messages to qualify
    sql = f"""
        SELECT author_id, 
               (SUM(CASE WHEN content LIKE '%?%' THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100 as pct,
               COUNT(*) as total
        FROM messages 
        WHERE year(timestamp) = {TARGET_YEAR} AND author_id NOT IN ({IGNORE_STR})
        GROUP BY author_id 
        HAVING total > 100 
        ORDER BY pct DESC LIMIT 1
    """
    res = con.sql(sql).fetchone()
    if res:
        table.add_row("❓ The Riddler", f"<@{res[0]}>", f"{res[1]:.1f}% of msgs are questions")

    # 2. THE CANADIAN (Highest % of messages with polite words)
    sql = f"""
        SELECT author_id, 
               (SUM(CASE WHEN (lower(content) LIKE '%thank%' OR lower(content) LIKE '%please%' OR lower(content) LIKE '%sorry%' OR lower(content) LIKE '%thx%' OR lower(content) like '%sry%') THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100 as pct,
               COUNT(*) as total
        FROM messages 
        WHERE year(timestamp) = {TARGET_YEAR} AND author_id NOT IN ({IGNORE_STR})
        GROUP BY author_id 
        HAVING total > 100 
        ORDER BY pct DESC LIMIT 1
    """
    res = con.sql(sql).fetchone()
    if res:
        table.add_row("🇨🇦 The Canadian", f"<@{res[0]}>", f"{res[1]:.1f}% polite ({int(res[1] / 100 * res[2])} times)")

    # 3. THE ECHO CHAMBER (Highest % of self-replies)
    # Using a JOIN to check if reply target is self
    sql = f"""
        SELECT m1.author_id,
               (COUNT(*)::FLOAT / (SELECT COUNT(*) FROM messages WHERE author_id = m1.author_id AND year(timestamp) = {TARGET_YEAR})) * 100 as pct,
               COUNT(*) as self_replies
        FROM messages m1 
        JOIN messages m2 ON m1.referenced_message_id = m2.id 
        WHERE year(m1.timestamp) = {TARGET_YEAR} 
        AND m1.author_id = m2.author_id 
        AND m1.author_id NOT IN ({IGNORE_STR})
        GROUP BY m1.author_id
        HAVING self_replies > 10
        ORDER BY pct DESC LIMIT 1
    """
    res = con.sql(sql).fetchone()
    if res:
        table.add_row("🗣️ The Echo Chamber", f"<@{res[0]}>", f"{res[1]:.1f}% self-replies")

    # 4. ONE-WORD WONDER (Avg length < 10, but > 1 to avoid empty embeds/images)
    sql = f"""
        SELECT author_id, 
               CAST(AVG(len(content)) AS INTEGER) as avg_len,
               COUNT(*) as c
        FROM messages 
        WHERE year(timestamp) = {TARGET_YEAR} 
        AND author_id NOT IN ({IGNORE_STR})
        AND len(content) > 0  -- Fix for 0 char bug
        GROUP BY author_id 
        HAVING c > 150 
        ORDER BY avg_len LIMIT 1
    """
    res = con.sql(sql).fetchone()
    if res:
        table.add_row("🆗 One-Word Wonder", f"<@{res[0]}>", f"Avg {res[1]} chars/msg")

    # 5. UNDERRATED GENIUS (High Avg Length, Low React Ratio)
    # Using the fixed JOIN strategy
    sql_underrated = f"""
        WITH 
        msg_stats AS (
            SELECT author_id, COUNT(*) as msgs, AVG(len(content)) as avg_len
            FROM messages 
            WHERE year(timestamp) = {TARGET_YEAR} AND author_id NOT IN ({IGNORE_STR})
            GROUP BY author_id
        ),
        react_stats AS (
            SELECT author_id, SUM(r.count) as total_reacts
            FROM (
                SELECT author_id, unnest(reactions) as r
                FROM messages
                WHERE year(timestamp) = {TARGET_YEAR} AND author_id NOT IN ({IGNORE_STR})
            )
            GROUP BY author_id
        )
        SELECT m.author_id, m.avg_len, COALESCE(r.total_reacts, 0)
        FROM msg_stats m
        LEFT JOIN react_stats r ON m.author_id = r.author_id
        WHERE m.msgs > 50 
        AND (COALESCE(r.total_reacts, 0)::FLOAT / m.msgs) < 0.2
        ORDER BY m.avg_len DESC LIMIT 1
    """
    res = con.sql(sql_underrated).fetchone()
    if res:
        table.add_row("🧠 Underrated Genius", f"<@{res[0]}>", f"Avg {int(res[1])} chars (No love 😔)")

    # 6. THE MEDIA MOGUL (New: Highest % of files/attachments)
    sql = f"""
        SELECT author_id, 
               (SUM(attachment_count)::FLOAT / COUNT(*)) * 100 as pct,
               SUM(attachment_count) as files
        FROM messages
        WHERE year(timestamp) = {TARGET_YEAR} AND author_id NOT IN ({IGNORE_STR})
        GROUP BY author_id
        HAVING files > 20
        ORDER BY pct DESC LIMIT 1
    """
    res = con.sql(sql).fetchone()
    if res:
        table.add_row("📸 The Media Mogul", f"<@{res[0]}>", f"{res[1]:.1f}% msgs have files")

    console.print(table)


def generate_word_cloud(text_data: str):
    """Generates and displays a cleaner, more readable word cloud."""
    if not text_data:
        console.print("[red]No text data found![/]")
        return

    with console.status("[bold green]Cleaning text and generating image...[/]"):
        # 1. Cleaning
        # Remove URLs
        clean_text = re.sub(r'http\S+', '', text_data)
        # Remove Discord Mentions/Channels/Emojis (<...>)
        clean_text = re.sub(r'<[@#:][^>]+>', '', clean_text)
        # Remove code blocks and backticks
        clean_text = clean_text.replace('```', '').replace('`', '')

        clean_text = clean_text.replace("bl oat", "bloat").replace("bl0at", "bloat").replace("boat", "bloat")

        # 2. Define Stopwords (Noise filter)
        # We start with the library's default list and add Discord-specific noise
        custom_stopwords = set(STOPWORDS)
        custom_stopwords.update([
            "yes", "one",
            "yeah", "want", "need", "time", "even", "now", "still", "going",
            "really", "think", "make", "see", "look", "much", "good", "well",
            "thing", "things", "stuff", "bit", "lot", "way", "something",
            "someone", "anyone", "everyone", "people", "person", "maybe",
            "probably", "actually", "sure", "never", "always", "usually",
            "getting", "trying", "said", "say", "tell", "ask", "back",
            "right", "wrong", "idea", "point", "part", "use", "using", "used",
            "know", "thats",
            "find", "found", "seem", "seems", "guess", "mean", "literally",
            "though", "thought", "wait", "wont", "dont", "cant", "didnt", "will", "know"
                                                                                  "thats", "theres", "isnt", "arent",
            "hes", "shes", "ive", "im",
            "thank", "thanks", "sorry", "please", "hello", "hi", "hey",
            "day", "week", "year", "ago", "later", "already", "pretty", "quite",

            # Vague Nouns (Boring)
            "thing", "things", "stuff", "bit", "lot", "way", "part", "point",
            "guy", "guys", "man", "dude", "bro", "person", "people", "someone",
            "something", "anything", "everything", "nothing", "everyone", "anyone",
            "day", "week", "year", "time", "hour", "minute", "moment",

            # Vague Verbs/Adjectives
            "good", "bad", "better", "best", "worse", "worst", "great", "nice", "cool",
            "real", "true", "false", "wrong", "right", "different", "same",
            "work", "working", "worked", "play", "playing", "played", "game",
            "look", "looking", "looked", "see", "seeing", "seen", "saw",
            "try", "trying", "tried", "use", "using", "used", "make", "making", "made",
            "want", "wanted", "need", "needed", "know", "knew", "think", "thought",
            "go", "going", "gone", "come", "coming", "came", "get", "getting", "got",
            "say", "saying", "said", "talk", "talking", "talked", "tell", "told",
            "really", "actually", "literally", "probably", "maybe", "sure",
            "first", "least", "take", "might", "give", "feel", "put", "many", "let",
            "kind", "reason", "fine", "every", "name", "show",

            # Internet Slang / Noise
            "idk", "tbh", "btw", "imo", "imho",
            "yeah", "yep", "nah", "nope", "pls", "plz", "thx", "thanks", "thank",
            "gonna", "wanna", "gotta", "dont", "cant", "wont", "didnt", "isnt",
            "thats", "theres", "whats", "hes", "shes", "ive", "im",
        ])

        # 3. Generate with Spacing
        wc = WordCloud(
            width=1600,
            height=900,
            background_color='#1a1a1a',
            colormap='Pastel1',
            max_words=30,
            min_word_length=3,
            margin=15,
            relative_scaling=0.5,
            stopwords=custom_stopwords,
            prefer_horizontal=0.9,
            collocation_threshold=5,
            collocations=True
        ).generate(clean_text)

        # 4. Show
        plt.figure(figsize=(16, 9), facecolor='#1a1a1a')
        plt.imshow(wc, interpolation='bilinear')
        plt.axis('off')
        plt.tight_layout(pad=0)

        output_file = "server_wordcloud.png"
        wc.to_file(output_file)

    console.print(f"[bold green]✅ Saved cleaner image to {output_file}[/]")
    console.print("[dim]Opening preview...[/]")
    plt.show()


# --- Main Logic ---

def run_menu():
    with db_connection() as con:
        while True:
            print_banner()

            action = questionary.select(
                "Select Intelligence Module:",
                choices=[
                    questionary.Choice("⚡  General Overview", value="general"),
                    questionary.Choice("🤿  Deep Dive (2025 Specifics)", value="deep_dive"),  # New Option
                    questionary.Choice("⏰  Activity Heatmap", value="hourly"),
                    questionary.Choice("☁️  Word Cloud Generator", value="wordcloud"),
                    questionary.Choice("🤣  Emoji Leaderboard", value="reactions"),
                    questionary.Choice("📢  The Yap List (Top Talkers)", value="yappers"),
                    questionary.Choice("🗣️  Interaction Network", value="mentions"),
                    questionary.Choice("📎  File Hoarders", value="files"),
                    questionary.Choice("👻  Lurkers & Ghosts", value="ghosts"),
                    questionary.Choice("🧪  Experimental / Fun Stats", value="creative"),
                    questionary.Separator(),
                    questionary.Choice("❌  Exit", value="exit")
                ],
                style=questionary.Style([('pointer', 'fg:cyan bold')])
            ).ask()

            if action == "exit" or action is None:
                console.print("[dim]Session terminated.[/]")
                break

            console.rule(style="dim")

            if action == "deep_dive":
                show_deep_dive(con)
            elif action == "creative":
                show_creative_stats(con)
            elif action == "wordcloud":
                # Fetch text first
                res = con.sql(QUERIES["wordcloud"]).fetchall()
                if res and res[0][0]:
                    generate_word_cloud(res[0][0])
                else:
                    console.print("[red]No message content found to analyze.[/]")
            else:
                # Standard Table Dispatcher
                res = con.sql(QUERIES[action]).fetchall()

                if action == "general":
                    render_overview(res[0])
                elif action == "reactions":
                    render_table("Top Reactions", ["Emoji", "Count"], res)
                elif action == "hourly":
                    render_bar_chart("Activity by Hour (UTC)", res)
                elif action == "mentions":
                    render_table("Top Conversations", ["Source ID", "Target ID", "Pings"], res)
                elif action == "yappers":
                    render_table("Most Active Users", ["User ID", "Msg Count", "Avg Len"], res)
                elif action == "ghosts":
                    render_table("Ghost Users (< 5 msgs)", ["User ID", "Total Msgs"], res)
                elif action == "files":
                    render_table("Top Uploaders", ["User ID", "Files Sent"], res)

            console.print("\n[dim]Press Enter to continue...[/]", end="")
            input()


if __name__ == "__main__":
    try:
        run_menu()
    except KeyboardInterrupt:
        print("\nBye.")
