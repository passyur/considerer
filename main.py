import csv
import os
import random
import sqlite3
from datetime import datetime, timezone

import discord
import yaml
from discord import app_commands
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_USER_IDS = set(
    int(x.strip()) for x in os.getenv("ADMIN_USER_IDS", "").split(",") if x.strip()
)
DB_PATH = os.getenv("DB_PATH", "results.db")
EXPERIMENT_PATH = os.getenv("EXPERIMENT_PATH", "experiment.yaml")

MAX_YAML_BYTES = 64 * 1024  # 64 KB sanity cap on uploaded experiment files


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def db_init():
    with db_connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS responses (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                experiment   TEXT NOT NULL,
                user_id      TEXT NOT NULL,
                username     TEXT NOT NULL,
                variant      TEXT NOT NULL,
                answer       TEXT NOT NULL,
                responded_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_user_experiment
            ON responses(user_id, experiment)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                guild_id TEXT NOT NULL,
                key      TEXT NOT NULL,
                value    TEXT NOT NULL,
                PRIMARY KEY (guild_id, key)
            )
        """)


def has_participated(user_id: int, experiment: str) -> bool:
    with db_connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM responses WHERE user_id=? AND experiment=?",
            (str(user_id), experiment),
        ).fetchone()
    return row is not None


def get_variant_counts(experiment: str) -> dict[str, int]:
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT variant, COUNT(*) as n FROM responses WHERE experiment=? GROUP BY variant",
            (experiment,),
        ).fetchall()
    counts = {"A": 0, "B": 0}
    for row in rows:
        counts[row["variant"]] = row["n"]
    return counts


def assign_variant(experiment: str) -> str:
    counts = get_variant_counts(experiment)
    if counts["A"] < counts["B"]:
        return "A"
    if counts["B"] < counts["A"]:
        return "B"
    return random.choice(["A", "B"])


def record_response(experiment: str, user_id: int, username: str, variant: str, answer: str):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO responses (experiment, user_id, username, variant, answer) VALUES (?,?,?,?,?)",
            (experiment, str(user_id), username, variant, answer),
        )


def get_results(experiment: str) -> list[sqlite3.Row]:
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM responses WHERE experiment=? ORDER BY responded_at",
            (experiment,),
        ).fetchall()


def delete_results(experiment: str):
    with db_connect() as conn:
        conn.execute("DELETE FROM responses WHERE experiment=?", (experiment,))


def get_setting(guild_id: int, key: str) -> str | None:
    with db_connect() as conn:
        row = conn.execute(
            "SELECT value FROM settings WHERE guild_id=? AND key=?",
            (str(guild_id), key),
        ).fetchone()
    return row["value"] if row else None


def set_setting(guild_id: int, key: str, value: str):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO settings (guild_id, key, value) VALUES (?,?,?) "
            "ON CONFLICT(guild_id, key) DO UPDATE SET value=excluded.value",
            (str(guild_id), key, value),
        )


# ---------------------------------------------------------------------------
# Experiment config
# ---------------------------------------------------------------------------

def load_experiment() -> dict:
    with open(EXPERIMENT_PATH) as f:
        return yaml.safe_load(f)


def validate_experiment(data: dict) -> list[str]:
    """Return a list of human-readable error strings; empty means valid."""
    errors = []
    if not isinstance(data, dict):
        return ["File must be a YAML mapping at the top level."]

    for field in ("name", "question"):
        if not data.get(field) or not isinstance(data[field], str):
            errors.append(f"Missing or empty required field: `{field}`")

    answer_type = data.get("answer_type", "freetext")
    if answer_type not in ("choice", "freetext"):
        errors.append("`answer_type` must be `choice` or `freetext`")

    if answer_type == "choice":
        opts = data.get("answer_options")
        if not opts or not isinstance(opts, list) or len(opts) < 2:
            errors.append("`answer_options` must be a list with at least 2 items when `answer_type` is `choice`")
        elif len(opts) > 5:
            errors.append("`answer_options` must have at most 5 items (Discord button limit)")

    variants = data.get("variants")
    if not isinstance(variants, dict):
        errors.append("Missing `variants` mapping")
    else:
        for v_key in ("A", "B"):
            v = variants.get(v_key)
            if not isinstance(v, dict):
                errors.append(f"Missing variant `{v_key}`")
                continue
            for field in ("title", "text"):
                if not v.get(field) or not isinstance(v[field], str):
                    errors.append(f"Variant `{v_key}` is missing required field: `{field}`")

    return errors


# ---------------------------------------------------------------------------
# Permissions
# ---------------------------------------------------------------------------

async def has_experiment_permission(interaction: discord.Interaction) -> bool:
    """True if the user is a hardcoded admin or has the server's experiment-manager role."""
    if interaction.user.id in ADMIN_USER_IDS:
        return True
    if interaction.guild_id is None:
        return False
    role_id = get_setting(interaction.guild_id, "experiment_manager_role_id")
    if role_id and isinstance(interaction.user, discord.Member):
        return any(str(r.id) == role_id for r in interaction.user.roles)
    return False


# ---------------------------------------------------------------------------
# Discord Views
# ---------------------------------------------------------------------------

class AnswerChoiceView(discord.ui.View):
    """Buttons for choice-type answers."""

    def __init__(self, experiment: str, variant: str, options: list[str], user_id: int, username: str):
        super().__init__(timeout=600)
        self.experiment = experiment
        self.variant = variant
        self.user_id = user_id
        self.username = username
        for option in options:
            btn = discord.ui.Button(label=option, style=discord.ButtonStyle.primary)
            btn.callback = self._make_callback(option)
            self.add_item(btn)

    def _make_callback(self, option: str):
        async def callback(interaction: discord.Interaction):
            if interaction.user.id != self.user_id:
                await interaction.response.send_message("This isn't your survey!", ephemeral=True)
                return
            if has_participated(self.user_id, self.experiment):
                await interaction.response.send_message("You've already answered.", ephemeral=True)
                return
            record_response(self.experiment, self.user_id, self.username, self.variant, option)
            self.clear_items()
            await interaction.response.edit_message(
                content=f"**Response recorded:** {option}\n\nThanks for participating!",
                view=self,
            )
        return callback


class FreetextAnswerModal(discord.ui.Modal):
    answer = discord.ui.TextInput(
        label="Your answer",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=1000,
    )

    def __init__(self, question: str, experiment: str, variant: str, user_id: int, username: str):
        super().__init__(title=question[:45])
        self.experiment = experiment
        self.variant = variant
        self.user_id = user_id
        self.username = username

    async def on_submit(self, interaction: discord.Interaction):
        if has_participated(self.user_id, self.experiment):
            await interaction.response.send_message("You've already answered.", ephemeral=True)
            return
        record_response(self.experiment, self.user_id, self.username, self.variant, self.answer.value)
        await interaction.response.send_message(
            "**Response recorded.** Thanks for participating!", ephemeral=True
        )


class OpenModalView(discord.ui.View):
    """Single button that opens the freetext modal."""

    def __init__(self, question: str, experiment: str, variant: str, user_id: int, username: str):
        super().__init__(timeout=600)
        self.question = question
        self.experiment = experiment
        self.variant = variant
        self.user_id = user_id
        self.username = username

    @discord.ui.button(label="Answer", style=discord.ButtonStyle.primary)
    async def open_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This isn't your survey!", ephemeral=True)
            return
        modal = FreetextAnswerModal(
            self.question, self.experiment, self.variant, self.user_id, self.username
        )
        await interaction.response.send_modal(modal)


class ConfirmResetView(discord.ui.View):
    def __init__(self, experiment: str):
        super().__init__(timeout=30)
        self.experiment = experiment

    @discord.ui.button(label="Yes, reset", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        delete_results(self.experiment)
        self.clear_items()
        await interaction.response.edit_message(
            content=f"All responses for **{self.experiment}** have been deleted.", view=self
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.clear_items()
        await interaction.response.edit_message(content="Reset cancelled.", view=self)


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------

intents = discord.Intents.default()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)


@client.event
async def on_ready():
    db_init()
    await tree.sync()
    print(f"Logged in as {client.user}  |  Commands synced")


@tree.command(name="participate", description="Take part in the current thought experiment")
async def participate(interaction: discord.Interaction):
    exp = load_experiment()
    name = exp["name"]

    if has_participated(interaction.user.id, name):
        await interaction.response.send_message(
            "You've already participated in this experiment. Thanks!", ephemeral=True
        )
        return

    variant_key = assign_variant(name)
    variant = exp["variants"][variant_key]
    question = exp["question"]
    answer_type = exp.get("answer_type", "freetext")

    scenario_block = (
        f"## {variant['title']}\n\n"
        f"{variant['text'].strip()}\n\n"
        f"**{question}**"
    )

    if answer_type == "choice":
        options = exp.get("answer_options", ["Yes", "No"])
        view = AnswerChoiceView(name, variant_key, options, interaction.user.id, str(interaction.user))
        await interaction.response.send_message(scenario_block, view=view, ephemeral=True)
    else:
        view = OpenModalView(question, name, variant_key, interaction.user.id, str(interaction.user))
        await interaction.response.send_message(scenario_block, view=view, ephemeral=True)


@tree.command(name="set_experiment_role", description="[Server admin] Set which role can manage experiments")
@app_commands.describe(role="The role to grant experiment-management permissions")
async def set_experiment_role(interaction: discord.Interaction, role: discord.Role):
    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message(
            "You need the **Manage Server** permission to set this.", ephemeral=True
        )
        return
    set_setting(interaction.guild_id, "experiment_manager_role_id", str(role.id))
    await interaction.response.send_message(
        f"Members with the **{role.name}** role can now manage experiments.", ephemeral=True
    )


@tree.command(name="upload_experiment", description="Upload a YAML file to set the active experiment")
@app_commands.describe(file="A .yaml file defining the experiment (variants, question, answer type)")
async def upload_experiment(interaction: discord.Interaction, file: discord.Attachment):
    if not await has_experiment_permission(interaction):
        await interaction.response.send_message(
            "You don't have permission to manage experiments.", ephemeral=True
        )
        return

    if not file.filename.endswith((".yaml", ".yml")):
        await interaction.response.send_message(
            "Please upload a `.yaml` or `.yml` file.", ephemeral=True
        )
        return

    if file.size > MAX_YAML_BYTES:
        await interaction.response.send_message(
            f"File is too large (max {MAX_YAML_BYTES // 1024} KB).", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    raw = await file.read()
    try:
        data = yaml.safe_load(raw.decode("utf-8"))
    except (yaml.YAMLError, UnicodeDecodeError) as exc:
        await interaction.followup.send(f"Could not parse YAML: `{exc}`", ephemeral=True)
        return

    errors = validate_experiment(data)
    if errors:
        error_list = "\n".join(f"• {e}" for e in errors)
        await interaction.followup.send(
            f"The file has {len(errors)} error(s):\n{error_list}", ephemeral=True
        )
        return

    with open(EXPERIMENT_PATH, "wb") as f:
        f.write(raw)

    name = data["name"]
    answer_type = data.get("answer_type", "freetext")
    variant_a = data["variants"]["A"]["title"]
    variant_b = data["variants"]["B"]["title"]

    await interaction.followup.send(
        f"Experiment set!\n"
        f"**{name}**\n"
        f"Answer type: `{answer_type}`\n"
        f"Variant A: {variant_a}\n"
        f"Variant B: {variant_b}",
        ephemeral=True,
    )


@tree.command(name="results", description="[Experiment manager] Show results summary for the current experiment")
async def results(interaction: discord.Interaction):
    if not await has_experiment_permission(interaction):
        await interaction.response.send_message("You don't have permission to view results.", ephemeral=True)
        return

    exp = load_experiment()
    name = exp["name"]
    rows = get_results(name)

    if not rows:
        await interaction.response.send_message(f"No responses yet for **{name}**.", ephemeral=True)
        return

    tally: dict[str, dict[str, int]] = {"A": {}, "B": {}}
    for row in rows:
        v = row["variant"]
        a = row["answer"]
        tally[v][a] = tally[v].get(a, 0) + 1

    total = len(rows)
    lines = [f"**Experiment: {name}** ({total} response{'s' if total != 1 else ''})\n"]

    for v_key in ("A", "B"):
        v_data = exp["variants"].get(v_key)
        if not v_data:
            continue
        v_counts = tally.get(v_key, {})
        v_total = sum(v_counts.values())
        lines.append(f"**Variant {v_key} — {v_data['title']}** ({v_total} response{'s' if v_total != 1 else ''})")
        if v_total == 0:
            lines.append("  *(no responses yet)*")
        else:
            for answer, count in sorted(v_counts.items(), key=lambda x: -x[1]):
                pct = count / v_total * 100
                lines.append(f"  {answer}: {count} ({pct:.0f}%)")
        lines.append("")

    await interaction.response.send_message("\n".join(lines), ephemeral=True)


@tree.command(name="export", description="[Experiment manager] Export results to a CSV file")
async def export(interaction: discord.Interaction):
    if not await has_experiment_permission(interaction):
        await interaction.response.send_message("You don't have permission to export results.", ephemeral=True)
        return

    exp = load_experiment()
    name = exp["name"]
    rows = get_results(name)

    if not rows:
        await interaction.response.send_message(f"No responses yet for **{name}**.", ephemeral=True)
        return

    safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in name)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"results_{safe_name}_{timestamp}.csv"

    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["experiment", "username", "user_id", "variant", "answer", "responded_at"])
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))

    await interaction.response.send_message(
        f"Exported {len(rows)} response(s) to `{filename}`.", ephemeral=True
    )


@tree.command(name="reset", description="[Experiment manager] Delete all responses for the current experiment")
async def reset(interaction: discord.Interaction):
    if not await has_experiment_permission(interaction):
        await interaction.response.send_message("You don't have permission to reset results.", ephemeral=True)
        return

    exp = load_experiment()
    name = exp["name"]
    counts = get_variant_counts(name)
    total = sum(counts.values())

    view = ConfirmResetView(name)
    await interaction.response.send_message(
        f"This will delete **{total}** response(s) for **{name}**. Are you sure?",
        view=view,
        ephemeral=True,
    )


client.run(BOT_TOKEN)
