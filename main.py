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


# ---------------------------------------------------------------------------
# Experiment config
# ---------------------------------------------------------------------------

def load_experiment() -> dict:
    with open(EXPERIMENT_PATH) as f:
        return yaml.safe_load(f)


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


def _is_admin(user_id: int) -> bool:
    return user_id in ADMIN_USER_IDS


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


@tree.command(name="results", description="[Admin] Show results summary for the current experiment")
async def results(interaction: discord.Interaction):
    if not _is_admin(interaction.user.id):
        await interaction.response.send_message("Admin only.", ephemeral=True)
        return

    exp = load_experiment()
    name = exp["name"]
    rows = get_results(name)

    if not rows:
        await interaction.response.send_message(f"No responses yet for **{name}**.", ephemeral=True)
        return

    # tally by variant → answer
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


@tree.command(name="export", description="[Admin] Export results to a CSV file")
async def export(interaction: discord.Interaction):
    if not _is_admin(interaction.user.id):
        await interaction.response.send_message("Admin only.", ephemeral=True)
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


@tree.command(name="reset", description="[Admin] Delete all responses for the current experiment")
async def reset(interaction: discord.Interaction):
    if not _is_admin(interaction.user.id):
        await interaction.response.send_message("Admin only.", ephemeral=True)
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
