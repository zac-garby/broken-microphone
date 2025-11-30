import os
import json
import re
import random
import aiohttp
import discord
import asyncio
import yt_dlp

from typing import Callable, Dict, Any, List, Optional, Type
from discord.ext import commands


# ============================================================
# GLOBALS, CONSTANTS and ENVIRONMENT VARIABLES
# ============================================================

def env[T = str](var: str, default: T | None = None, conv: Callable[[str], T] = str) -> T:
    str_val = os.getenv(var)

    try:
        if str_val is None:
            if default:
                val = default
            else:
                raise RuntimeError(f"env var '{var}' isn't set. did you load .env?")
        else:
            val = conv(str_val)

        print(f"{var}: {val}")
        return val
    except TypeError as e:
        raise TypeError(f"env var '{var}' = '{str_val}' has the wrong type") from e

TOKEN = env("BM_DISCORD_TOKEN")
YT_API_KEY = env("BM_YT_API_KEY")
COMMAND_PREFIX = env("BM_COMMAND_PREFIX", ";")
DEBUG = env("BM_DEBUG", False, lambda x: x != "no")
AUDIO_DIR = env("BM_AUDIO_DIR", "bm_audio")
STATE_FILE = env("BM_STATE_FILE", "bm_state.json")
MAX_AUDIO_MB = env("BM_MAX_AUDIO_MB", 128, int)


os.makedirs(AUDIO_DIR, exist_ok=True)

intents = discord.Intents.default()
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix=COMMAND_PREFIX, intents=intents)


# ============================================================
# STATE MANAGEMENT
# ============================================================

def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {}

def save_state():
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

state: Dict[str, Any] = load_state()

# Tracks users awaiting URL or description
# Format: { user_id: "awaiting_url" | "awaiting_description" }
pending_submission: Dict[int, str] = {}

def gstate(guild_id: int) -> Dict[str, Any]:
    gid = str(guild_id)
    if gid not in state:
        state[gid] = {
            "players": [],
            "bot_channel": None,
            "current_round": None,
            "queue": [],
            "queue_shuffle": False,
        }
    else:
        # Ensure new keys exist for older state files
        gs = state[gid]
        if "queue" not in gs:
            gs["queue"] = []
        if "queue_shuffle" not in gs:
            gs["queue_shuffle"] = False
    return state[gid]


def extract_youtube_id(url: str) -> str:
    m = re.search(r"youtu\.be/([A-Za-z0-9_\-]{6,})", url)
    if m:
        return m.group(1)
    m = re.search(r"v=([A-Za-z0-9_\-]{6,})", url)
    if m:
        return m.group(1)
    m = re.search(r"shorts/([A-Za-z0-9_\-]{6,})", url)
    if m:
        return m.group(1)
    m = re.search(r"/([A-Za-z0-9_\-]{6,})$", url)
    if m:
        return m.group(1)
    return ""


def playlist_url(submissions: List[Dict[str, Any]]) -> str:
    ids = [extract_youtube_id(s["url"]) for s in submissions]
    ids = [i for i in ids if i]
    if not ids:
        return "No valid submissions."
    return f"https://www.youtube.com/watch_videos?video_ids={','.join(ids)}"


def pretty_link(text: str, url: str) -> str:
    """
    Make a masked link that also uses <url> to suppress Discord previews.
    """
    return f"[{text}](<{url}>)"


# ============================================================
# YOUTUBE TITLE FETCH
# ============================================================

async def fetch_youtube_title(video_id: str) -> str:
    """
    Fetches the video title via the YouTube Data API.
    Returns a placeholder title if the API fails.
    """
    if not YT_API_KEY:
        return "<unknown title>"

    url = (
        "https://www.googleapis.com/youtube/v3/videos"
        f"?part=snippet&id={video_id}&key={YT_API_KEY}"
    )

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                items = data.get("items", [])
                if items:
                    return items[0]["snippet"]["title"]
    except Exception:
        pass

    return "<unknown title>"


def get_text_channel(guild: discord.Guild, channel_id: int) -> Optional[discord.TextChannel]:
    """
    Returns a TextChannel if the ID corresponds to one.
    Returns None otherwise (including missing, wrong type, or deleted channel).
    """
    channel = guild.get_channel(channel_id)
    if isinstance(channel, discord.TextChannel):
        return channel
    return None


# ============================================================
# CORE HELPERS (used by commands + auto-advance)
# ============================================================

async def close_submissions_core(ctx: commands.Context, guild_id: int) -> Optional[str]:
    """
    End submission phase and begin voting.
    Returns an error message if it cannot be done, otherwise None.
    """
    guild = bot.get_guild(guild_id)
    if guild is None:
        return "Internal error: guild not found."

    gs = gstate(guild_id)
    r = gs.get("current_round")

    if r is None or r["status"] != "collecting":
        return "No collecting round."

    if not r["submissions"]:
        return "Cannot close submissions: nobody has submitted yet."

    r["status"] = "voting"
    save_state()

    # Clear any pending URL/description prompts for this guild's players
    for pid in gs["players"]:
        pending_submission.pop(pid, None)

    channel_id = gs.get("bot_channel")
    if channel_id is None:
        return f"Bot channel not set. Use {COMMAND_PREFIX}set_channel."

    channel = get_text_channel(guild, channel_id)
    if channel is None:
        return f"The configured bot channel is invalid or not a text channel. Re-run {COMMAND_PREFIX}set_channel."

    subs: List[Dict[str, Any]] = r["submissions"]
    purl = playlist_url(subs)
    playlist_link = pretty_link("View the playlist here!", purl)

    # Build numbered submissions as a LIST (index = entry_id - 1)
    numbered: List[Dict[str, Any]] = []
    for sub in subs:
        numbered.append({
            "player_id": sub["player_id"],
            "url": sub["url"],
            "video_id": sub["video_id"],
            "title": sub["title"],
            "description": sub.get("description", ""),
        })
    r["numbered_submissions"] = numbered
    save_state()

    msg = f"🎵 **Submissions closed!** Voting begins. 🎺 {playlist_link} 🪉"
    for i, sub in enumerate(numbered, start=1):
        sub_link = pretty_link(sub['title'], sub['url'])
        msg += f"\n- **{i}**: {sub_link}"
    await channel.send(msg)

    # DM players with voting instructions
    for pid in gs["players"]:
        user = guild.get_member(pid)
        if not user:
            continue
        try:
            msg = "🎵 Voting time!\nYou must distribute **10 points** across the following songs:\n\n"

            for idx, sub in enumerate(numbered, start=1):
                title = sub["title"]
                desc = sub.get("description", "")
                msg += f"**{idx}. {title}**\n"
                if desc:
                    msg += f"_{desc}_\n"
                msg += f"{pretty_link('Open on YouTube', sub['url'])}\n\n"

            msg += (
                "\nSubmit your vote using:\n"
                f"`{COMMAND_PREFIX}vote <entry_id>:<points> <entry_id>:<points> ...`\n"
                f"Example: `{COMMAND_PREFIX}vote 1:5 3:3 5:2`"
            )
            await user.send(msg)
        except Exception:
            pass

    await channel.send("Pre-downloading submission audio...")

    for idx, sub in enumerate(numbered, start=1):
        url = sub["url"]

        await channel.send(f"Downloading audio for submission {idx}: **{sub['title']}**")

        # Use a guild-specific basename
        basename = f"{guild.id}_{idx}"
        filepath = await download_audio(url, basename)

        if filepath is None:
            await channel.send(f"Failed to download audio for: **{sub['title']}**")
            continue

    await channel.send("All available audio downloaded. 🎸")

    return None


async def finish_round_core(guild_id: int) -> Optional[str]:
    """
    End voting phase, post results, and clear the round.
    Returns an error message if it cannot be done, otherwise None.
    """
    guild = bot.get_guild(guild_id)
    if guild is None:
        return "Internal error: guild not found."

    gs = gstate(guild_id)
    r = gs.get("current_round")

    if r is None or r["status"] != "voting":
        return "No voting round to finish."

    if not r["votes"]:
        return "Cannot finish voting: nobody has voted yet."

    subs: List[Dict[str, Any]] = r["numbered_submissions"]
    n = len(subs)
    scores = [0] * n  # index = entry_id - 1

    for v in r["votes"]:
        for sid, pts in v["distribution"].items():
            idx = sid - 1
            if 0 <= idx < n:
                scores[idx] += pts

    ordered = sorted(
        [(sid, scores[sid - 1]) for sid in range(1, n + 1)],
        key=lambda x: x[1],
        reverse=True,
    )

    channel_id = gs.get("bot_channel")
    if channel_id is None:
        return f"Bot channel not set. Use {COMMAND_PREFIX}set_channel."

    channel = get_text_channel(guild, channel_id)
    if channel is None:
        return f"The configured bot channel is invalid or not a text channel. Re-run {COMMAND_PREFIX}set_channel."

    msg = "🎤 **Broken Microphone – Round Results**\n"
    msg += f"Prompt: **{r['prompt']}**\n\n"

    for rank, (sid, pts) in enumerate(ordered, start=1):
        sub = subs[sid - 1]
        url = sub["url"]
        title = sub["title"]
        description = sub.get("description", None)
        submitter_member = guild.get_member(sub["player_id"])
        submitter_name = submitter_member.display_name if submitter_member else f"User {sub['player_id']}"

        msg += (
            f"**{rank}. {pts} pts — {submitter_name}**\n"
            f"**{title}**\n"
            f"{pretty_link('Open on YouTube', url)}\n"
        )
        if description:
            msg += f"_{description}_\n"
        msg += "\n"

    await channel.send(msg)

    gs["current_round"] = None
    save_state()
    return None


# ============================================================
# BOT EVENTS / COMMANDS
# ============================================================

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    await reset_status()


async def reset_status():
    await bot.change_presence(activity=discord.Activity(
        type=discord.ActivityType.listening,
        name=f"🎸 Say {COMMAND_PREFIX}help for usage info"
    ))

async def playing_status(sub):
    await bot.change_presence(activity=discord.Activity(
        type=discord.ActivityType.listening,
        name=f"🎶 Listening to {sub['title']} 🎶",
        details=sub['description'],
        url=sub['url']
    ))


# --------------------------------------------
# PLAYERS JOIN / LEAVE
# --------------------------------------------

@bot.command(help="Join the Broken Microphone league in this server.")
async def join(ctx: commands.Context):
    if ctx.guild is None:
        await ctx.send("This command can only be used in a server.")
        return

    gs = gstate(ctx.guild.id)
    if ctx.author.id not in gs["players"]:
        gs["players"].append(ctx.author.id)
        save_state()
        await ctx.send("You have joined the league.")
    else:
        await ctx.send("You are already a player.")


@bot.command(help="Leave the Broken Microphone league in this server.")
async def leave(ctx: commands.Context):
    if ctx.guild is None:
        await ctx.send("This command can only be used in a server.")
        return

    gs = gstate(ctx.guild.id)
    if ctx.author.id in gs["players"]:
        gs["players"].remove(ctx.author.id)
        save_state()
        await ctx.send("You have left the league.")
    else:
        await ctx.send("You were not a player.")


# --------------------------------------------
# STATUS
# --------------------------------------------

@bot.command(help="Show the current round status and who still needs to submit or vote.")
async def status(ctx: commands.Context):
    if ctx.guild is None:
        await ctx.send("This command can only be used in a server.")
        return

    gs = gstate(ctx.guild.id)
    r = gs.get("current_round")

    if r is None:
        await ctx.send("No round is currently running.")
        return

    players = gs["players"]
    submissions = {s["player_id"] for s in r.get("submissions", [])}
    voters = {v["voter_id"] for v in r.get("votes", [])}

    missing_submissions = [p for p in players if p not in submissions]
    missing_votes = [p for p in players if p not in voters]

    def fmt_users(ids: List[int]) -> str:
        if ctx.guild is None:
            return "(Error! Not in a server)"
        if not ids:
            return "None"
        names = []
        for uid in ids:
            m = ctx.guild.get_member(uid)
            names.append(m.display_name if m else f"User {uid}")
        return ", ".join(names)

    msg = f"**Round status**\nPrompt: **{r['prompt']}**\nStatus: **{r['status']}**\n\n"
    msg += f"Players: {len(players)}\n"
    msg += f"Submissions: {len(submissions)}/{len(players)}\n"
    msg += f"Votes: {len(voters)}/{len(players)}\n\n"

    if r["status"] == "collecting":
        msg += f"Players who still need to submit:\n{fmt_users(missing_submissions)}"
    elif r["status"] == "voting":
        msg += f"Players who still need to submit (for completeness):\n{fmt_users(missing_submissions)}\n\n"
        msg += f"Players who still need to vote:\n{fmt_users(missing_votes)}"

    await ctx.send(msg)


# --------------------------------------------
# SET BOT CHANNEL
# --------------------------------------------

@bot.command(help="Set this text channel as the Broken Microphone announcement channel.")
async def set_channel(ctx: commands.Context):
    if ctx.guild is None:
        await ctx.send("This command can only be used in a server.")
        return

    if not isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("This channel cannot be used as the bot channel.")
        return

    gs = gstate(ctx.guild.id)
    gs["bot_channel"] = ctx.channel.id
    save_state()
    await ctx.send("This channel is now the bot's announcement channel.")


# --------------------------------------------
# ROUND QUEUE
# --------------------------------------------

@bot.command(name="queue_add", help="Add a prompt to the round queue.")
async def queue_add(ctx: commands.Context, *, prompt: str):
    if ctx.guild is None:
        await ctx.send("This command can only be used in a server.")
        return
    prompt = prompt.strip()
    if not prompt:
        await ctx.send("Prompt cannot be empty.")
        return
    gs = gstate(ctx.guild.id)
    gs["queue"].append(prompt)
    save_state()
    await ctx.send(f"Added to queue (#{len(gs['queue'])}): {prompt}")


@bot.command(name="queue_view", help="View the current round queue.")
async def queue_view(ctx: commands.Context):
    if ctx.guild is None:
        await ctx.send("This command can only be used in a server.")
        return
    gs = gstate(ctx.guild.id)
    q = gs["queue"]
    shuffle = gs["queue_shuffle"]
    if not q:
        await ctx.send(f"Queue is empty. Shuffle mode is {'ON' if shuffle else 'OFF'}.")
        return
    lines = [f"Queue (shuffle: {'ON' if shuffle else 'OFF'}):"]
    for i, p in enumerate(q, start=1):
        lines.append(f"{i}. {p}")
    await ctx.send("\n".join(lines))


@bot.command(name="queue_remove", help="Remove a prompt from the queue by its index (1-based).")
async def queue_remove(ctx: commands.Context, index: int):
    if ctx.guild is None:
        await ctx.send("This command can only be used in a server.")
        return
    gs = gstate(ctx.guild.id)
    q = gs["queue"]
    if index < 1 or index > len(q):
        await ctx.send("Invalid index.")
        return
    removed = q.pop(index - 1)
    save_state()
    await ctx.send(f"Removed from queue: {removed}")


@bot.command(name="queue_shuffle", help="Turn queue shuffle on/off, or toggle if no argument is given.")
async def queue_shuffle(ctx: commands.Context, mode: str = ""):
    if ctx.guild is None:
        await ctx.send("This command can only be used in a server.")
        return
    gs = gstate(ctx.guild.id)

    if mode.lower() == "on":
        gs["queue_shuffle"] = True
    elif mode.lower() == "off":
        gs["queue_shuffle"] = False
    else:
        # toggle
        gs["queue_shuffle"] = not gs["queue_shuffle"]

    save_state()
    await ctx.send(f"Queue shuffle is now {'ON' if gs['queue_shuffle'] else 'OFF'}.")


# --------------------------------------------
# START ROUND
# --------------------------------------------

@bot.command(
    help=(
        "Start a new round. Either provide a prompt, or leave empty to take one "
        "from the queue (respecting shuffle mode)."
    )
)
async def start_round(ctx: commands.Context, *, prompt: str = ""):
    if ctx.guild is None:
        await ctx.send("This command can only be used in a server.")
        return

    gs = gstate(ctx.guild.id)

    # Delete all old audio files
    for f in os.listdir(AUDIO_DIR):
        try:
            os.remove(os.path.join(AUDIO_DIR, f))
        except Exception:
            pass

    if gs["bot_channel"] is None:
        await ctx.send(f"Set the bot channel first using {COMMAND_PREFIX}set_channel.")
        return

    if gs["current_round"] is not None:
        await ctx.send(f"A round is already running. Use {COMMAND_PREFIX}finish_round first.")
        return

    prompt = prompt.strip()
    if not prompt:
        # Take from queue
        q = gs["queue"]
        if not q:
            await ctx.send("No prompt given and the queue is empty.")
            return
        if gs["queue_shuffle"]:
            idx = random.randrange(len(q))
        else:
            idx = 0
        prompt = q.pop(idx)
        save_state()

    gs["current_round"] = {
        "prompt": prompt,
        "status": "collecting",
        "submissions": [],  # list of {player_id, url, video_id, title, description}
        "votes": [],        # list of {voter_id, distribution: {entry_id: points}}
    }
    save_state()

    channel_id = gs.get("bot_channel")
    if channel_id is None:
        await ctx.send(f"Bot channel not set. Use {COMMAND_PREFIX}set_channel.")
        return

    channel = get_text_channel(ctx.guild, channel_id)
    if channel is None:
        await ctx.send(f"The configured bot channel is invalid or not a text channel. Re-run {COMMAND_PREFIX}set_channel.")
        return

    await channel.send(f"🎵 **New Broken Microphone round started!**\nPrompt: **{prompt}**")

    # DM each player asking for submission
    for pid in gs["players"]:
        user = ctx.guild.get_member(pid)
        if user:
            try:
                await user.send(
                    f"🎵 New Broken Microphone round started!\n"
                    f"Prompt: **{prompt}**\n\n"
                    f"Please submit your song by sending **just a YouTube URL**, OR use:\n"
                    f"`{COMMAND_PREFIX}submit_song <url>`\n\n"
                    f"After submitting a URL, you may send a **short description** (1–3 sentences) explaining your choice."
                )
                pending_submission[user.id] = "awaiting_url"
            except Exception:
                pass


# --------------------------------------------
# SONG SUBMISSION (DM only)
# --------------------------------------------

@bot.command(help="(DM only) Submit your song URL for the current round.")
async def submit_song(ctx: commands.Context, url: str):
    if ctx.guild is not None:
        await ctx.send("Submit songs via DM only.")
        return

    for gid, gs in state.items():
        if not isinstance(gs, dict):
            continue
        if ctx.author.id not in gs.get("players", []):
            continue

        r = gs.get("current_round")
        if r is None or r["status"] != "collecting":
            continue

        video_id = extract_youtube_id(url)
        if not video_id:
            await ctx.send("Invalid YouTube link.")
            return

        # canonical URL
        url = f"https://www.youtube.com/watch?v={video_id}"

        title = await fetch_youtube_title(video_id)

        # store / update
        existing = None
        for s in r["submissions"]:
            if s["player_id"] == ctx.author.id:
                existing = s
                break

        if existing:
            existing["url"] = url
            existing["video_id"] = video_id
            existing["title"] = title
        else:
            r["submissions"].append({
                "player_id": ctx.author.id,
                "url": url,
                "video_id": video_id,
                "title": title,
                "description": "",
            })

        save_state()

        # progress info
        total_players = len(gs["players"])
        submitted_ids = {s["player_id"] for s in r["submissions"]}
        submitted_count = len(submitted_ids)

        msg = (
            f"Song received:\n**{title}**\n"
            f"Now please send a short description.\n\n"
            f"Submissions so far: {submitted_count}/{total_players} players."
        )
        await ctx.send(msg)
        pending_submission[ctx.author.id] = "awaiting_description"

        # auto-close if everyone submitted and at least one submission exists
        if submitted_count == total_players and submitted_count > 0:
            await close_submissions_core(ctx, int(gid))

        return

    await ctx.send("No active collecting round found for you.")


# --------------------------------------------
# on_message: handle URL/description flows in DMs
# --------------------------------------------

@bot.event
async def on_message(message: discord.Message):
    # First let commands run
    await bot.process_commands(message)

    # Only interested in DMs
    if message.guild is not None:
        return

    user = message.author
    if user.bot:
        return

    # If this is a command (starts with prefix), don't treat it as URL/description
    content = message.content.strip()
    if content.startswith(COMMAND_PREFIX):
        return

    # If user is expecting a URL
    if pending_submission.get(user.id) == "awaiting_url":
        url = content

        if "youtu" not in url:
            await user.send(f"Please send a valid YouTube URL, or use `{COMMAND_PREFIX}submit_song <url>`.")
            return

        # Route it to submit_song command
        ctx = await bot.get_context(message)
        cmd = bot.get_command("submit_song")
        if cmd is None:
            await user.send("Internal error: submit_song command not found.")
            return
        await ctx.invoke(cmd, url=url)  # type: ignore[arg-type]
        return

    # If user is expecting a description
    if pending_submission.get(user.id) == "awaiting_description":
        desc = content

        # Store description
        for _, gs in state.items():
            if not isinstance(gs, dict):
                continue
            r = gs.get("current_round")
            if r and user.id in gs.get("players", []) and r["status"] == "collecting":
                for s in r["submissions"]:
                    if s["player_id"] == user.id:
                        s["description"] = desc
                        save_state()
                        break

        await user.send("Description received!")
        del pending_submission[user.id]
        return


# --------------------------------------------
# CLOSE SUBMISSIONS → START VOTING
# --------------------------------------------

@bot.command(help="Manually close submissions and start voting (if possible).")
async def close_submissions(ctx: commands.Context):
    if ctx.guild is None:
        await ctx.send("This command can only be used in a server.")
        return

    err = await close_submissions_core(ctx, ctx.guild.id)
    if err:
        await ctx.send(err)


# --------------------------------------------
# VOTING (DM only)
# --------------------------------------------

@bot.command(help=f"(DM only) Vote by distributing 10 points, e.g. `{COMMAND_PREFIX}vote 1:5 3:3 5:2`.")
async def vote(ctx: commands.Context, *allocations: str):
    if ctx.guild is not None:
        await ctx.send("Vote via DM only.")
        return

    for gid, gs in state.items():
        if not isinstance(gs, dict):
            continue
        r = gs.get("current_round")
        if r and ctx.author.id in gs.get("players", []) and r["status"] == "voting":
            numbered: List[Dict[str, Any]] = r["numbered_submissions"]

            # Parse allocations
            dist: Dict[int, int] = {}
            total = 0

            for a in allocations:
                if ":" not in a:
                    await ctx.send("Invalid format. Use e.g. `1:5 3:2`.")
                    return
                sid_str, pts_str = a.split(":", 1)
                try:
                    sid = int(sid_str)
                    pts = int(pts_str)
                except ValueError:
                    await ctx.send("Invalid number.")
                    return

                if sid < 1 or sid > len(numbered):
                    await ctx.send(f"Invalid entry ID: {sid}")
                    return
                if pts < 0:
                    await ctx.send("Points must be non-negative.")
                    return

                dist[sid] = pts
                total += pts

            if total != 10:
                await ctx.send("Total points must be exactly **10**.")
                return

            # prevent voting for own submission, unless we're in debug mode.
            for sid in dist.keys():
                sub = numbered[sid - 1]
                if sub["player_id"] == ctx.author.id:
                    if DEBUG:
                        await ctx.send("You cannot vote for your own submission, but we're in debug mode so it's okay.")
                    else:
                        await ctx.send("You cannot vote for your own submission.")
                        return

            # Save vote (overwrite previous)
            r["votes"] = [v for v in r["votes"] if v["voter_id"] != ctx.author.id]
            r["votes"].append({
                "voter_id": ctx.author.id,
                "distribution": dist,
            })
            save_state()

            # progress info
            total_players = len(gs["players"])
            voter_ids = {v["voter_id"] for v in r["votes"]}
            voter_count = len(voter_ids)

            # Confirmation
            sorted_items = sorted(dist.items(), key=lambda x: x[1], reverse=True)

            msg = "Your vote has been recorded:\n\n"
            for rank, (sid, pts) in enumerate(sorted_items, start=1):
                sub = numbered[sid - 1]
                title = sub["title"]
                msg += f"{rank}. **{title}** — {pts} point"
                if pts != 1:
                    msg += "s"
                msg += "\n"

            msg += f"\nVotes so far: {voter_count}/{total_players} players."
            await ctx.send(msg)

            # auto-finish if everyone has voted and at least one vote exists
            if voter_count == total_players and voter_count > 0:
                await finish_round_core(int(gid))

            return

    await ctx.send("No active voting round found.")


# --------------------------------------------
# FINISH ROUND → REVEAL RESULTS
# --------------------------------------------

@bot.command(help="Manually finish the voting phase and reveal round results (if possible).")
async def finish_round(ctx: commands.Context):
    if ctx.guild is None:
        await ctx.send("This command can only be used in a server.")
        return

    err = await finish_round_core(ctx.guild.id)
    if err:
        await ctx.send(err)
    else:
        await ctx.send("Round finished.")



# --------------------------------------------
# AUDIO PLAYBACK
# --------------------------------------------

async def download_audio(video_url: str, basename: str) -> Optional[str]:
    """
    Downloads audio via yt-dlp into AUDIO_DIR.
    Rejects downloads larger than MAX_AUDIO_MB.
    Returns filepath or None.
    """

    print(f"Downloading {video_url} to {basename}")
    out_noext = os.path.join(AUDIO_DIR, basename)
    final_path = f"{out_noext}.m4a"

    ydl_opts: dict[str, Any] = {
        "format": "bestaudio/best",
        "outtmpl": out_noext + ".%(ext)s",
        "quiet": True,
        "no_warnings": True,
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "m4a",
            }
        ],
        # ――― Size limit enforcement ―――
        "overwrites": True,
        "file_size_limit": MAX_AUDIO_MB * 1024 * 1024,  # bytes
    }

    def do_download():
        yt_dlp.YoutubeDL(ydl_opts).download([video_url])  # type: ignore

    try:
        loop = asyncio.get_event_loop()

        # yt-dlp raises an exception if the file exceeds file_size_limit
        await loop.run_in_executor(None, do_download)

        # After processing, ensure the file exists
        if os.path.exists(final_path):
            return final_path
        else:
            return None

    except Exception as e:
        print("Audio download error:", e)
        return None

@bot.command(help="Stop playing audio and disconnect the bot from voice.")
async def stop(ctx: commands.Context):
    if ctx.guild is None:
        await ctx.send("This command must be used in the server.")
        return

    vc = ctx.guild.voice_client
    if not isinstance(vc, discord.VoiceClient):
        await ctx.send("The bot isn't in voice chat!")
        return

    if vc.is_playing():
        vc.stop()

    await vc.disconnect()

async def play_audio_in_channel(channel: discord.VoiceChannel, filepath: str):
    """
    Connect (or move) to `channel` and play the given audio file with FFmpeg.
    Assumes ffmpeg is installed and on PATH.
    """
    voice = channel.guild.voice_client

    # If not connected, connect
    if not isinstance(voice, discord.VoiceClient):
        voice = await channel.connect()
    # If connected to a different channel, move
    elif voice.channel != channel:
        await voice.move_to(channel)

    # At this point, `voice` is definitely a VoiceClient
    assert isinstance(voice, discord.VoiceClient)

    if not os.path.exists(filepath):
        raise FileNotFoundError(filepath)

    # Stop anything already playing
    if voice.is_playing():
        voice.stop()

    source = discord.FFmpegPCMAudio(filepath)
    voice.play(source)

    # Wait until finished
    while voice.is_playing():
        await asyncio.sleep(0.5)

@bot.command(help=f"Listen to submissions. Use `{COMMAND_PREFIX}listen` for all or `{COMMAND_PREFIX}listen <index>` for one.")
async def listen(ctx: commands.Context, index: int | None = None):
    if ctx.guild is None:
        await ctx.send("This command must be used in the server.")
        return

    # Must be in a voice channel
    author = ctx.author
    if not isinstance(author, discord.Member):
        await ctx.send("This command must be used in a server.")
        return

    if author.voice is None:
        await ctx.send("You must be in a voice channel to use this.")
        return

    chan = author.voice.channel
    if not isinstance(chan, discord.VoiceChannel):
        await ctx.send("You must be in a regular voice channel (not a stage channel).")
        return

    voice_channel = chan

    gs = gstate(ctx.guild.id)
    r = gs.get("current_round")

    if r is None:
        await ctx.send("No active round.")
        return

    if "numbered_submissions" not in r:
        await ctx.send("Submissions are not yet in playback format (close submissions first).")
        return

    subs = r["numbered_submissions"]

    # Determine play order
    if index is not None:
        if index < 1 or index > len(subs):
            await ctx.send("Invalid submission index.")
            return
        target_indices = [index]
    else:
        target_indices = list(range(1, len(subs) + 1))

    await ctx.send("Preparing audio...")

    # Playback loop
    for sid in target_indices:
        sub = subs[sid - 1]
        title = sub["title"]
        url = sub["url"]


        filepath = os.path.join(AUDIO_DIR, f"{ctx.guild.id}_{sid}.m4a")

        if not os.path.exists(filepath):
            # Fallback: download on demand if missing
            await ctx.send(f"Downloading: **{title}**")
            filepath = await download_audio(url, f"{ctx.guild.id}_{sid}")

        if not filepath or not os.path.exists(filepath):
            await ctx.send(f"Failed to load audio for **{title}**")
            continue

        await playing_status(sub)
        await ctx.send(f"Now playing: **{title}**")
        try:
            await play_audio_in_channel(voice_channel, filepath)
        except Exception as e:
            await ctx.send(f"Playback error for **{title}**: {e}")
            continue
        finally:
            await reset_status()

    await ctx.send("Finished playback.")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    bot.run(TOKEN)
