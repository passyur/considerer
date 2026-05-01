import csv
import io
import json
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
ALLOWED_GUILD_IDS = set(
    int(x.strip()) for x in os.getenv("ALLOWED_GUILD_IDS", "").split(",") if x.strip()
)
DB_PATH = os.getenv("DB_PATH", "results.db")
EXPERIMENT_PATH = os.getenv("EXPERIMENT_PATH", "experiment.yaml")  # used only for startup seed

MAX_YAML_BYTES = 64 * 1024
MAX_ACTIVE_EXPERIMENTS = 5


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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS experiments (
                name       TEXT PRIMARY KEY,
                config     TEXT NOT NULL,
                active     INTEGER NOT NULL DEFAULT 1,
                started_at TEXT DEFAULT (datetime('now')),
                started_by TEXT NOT NULL DEFAULT 'system'
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
    return {row["variant"]: row["n"] for row in rows}


def assign_variant(experiment: str, variant_keys: list[str]) -> str:
    counts = get_variant_counts(experiment)
    for k in variant_keys:
        counts.setdefault(k, 0)
    min_count = min(counts[k] for k in variant_keys)
    candidates = [k for k in variant_keys if counts[k] == min_count]
    return random.choice(candidates)


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


def count_responses(experiment: str) -> int:
    with db_connect() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM responses WHERE experiment=?", (experiment,)
        ).fetchone()
    return row[0] if row else 0


def update_experiment_config(name: str, config_json: str):
    with db_connect() as conn:
        conn.execute("UPDATE experiments SET config=? WHERE name=?", (config_json, name))


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


def get_active_experiments() -> list[dict]:
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT config FROM experiments WHERE active=1 ORDER BY started_at"
        ).fetchall()
    return [json.loads(row["config"]) for row in rows]


def get_experiment_by_name(name: str) -> dict | None:
    """Returns config regardless of active status (so ended experiments are still queryable)."""
    with db_connect() as conn:
        row = conn.execute(
            "SELECT config FROM experiments WHERE name=?", (name,)
        ).fetchone()
    return json.loads(row["config"]) if row else None


def is_active_experiment(name: str) -> bool:
    with db_connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM experiments WHERE name=? AND active=1", (name,)
        ).fetchone()
    return row is not None


def add_experiment(name: str, config_json: str, started_by: str):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO experiments (name, config, active, started_by) VALUES (?,?,1,?) "
            "ON CONFLICT(name) DO UPDATE SET config=excluded.config, active=1, "
            "started_at=datetime('now'), started_by=excluded.started_by",
            (name, config_json, started_by),
        )


def deactivate_experiment(name: str):
    with db_connect() as conn:
        conn.execute("UPDATE experiments SET active=0 WHERE name=?", (name,))


# ---------------------------------------------------------------------------
# Experiment config
# ---------------------------------------------------------------------------

def normalize_experiment(exp: dict) -> dict:
    """Ensure exp has a 'questions' list. Handles legacy single-question format."""
    if "questions" in exp:
        return exp
    exp = dict(exp)
    exp["questions"] = [{
        "id": "q1",
        "text": exp.get("question", ""),
        "answer_type": exp.get("answer_type", "freetext"),
        "answer_options": exp.get("answer_options", []),
    }]
    return exp


def validate_experiment(data: dict) -> list[str]:
    errors = []
    if not isinstance(data, dict):
        return ["File must be a YAML mapping at the top level."]

    if not data.get("name") or not isinstance(data["name"], str):
        errors.append("Missing or empty required field: `name`")

    if "description" in data and not isinstance(data["description"], str):
        errors.append("`description` must be a string")

    variants = data.get("variants")
    if not isinstance(variants, dict):
        errors.append("Missing `variants` mapping")
    else:
        if len(variants) == 0:
            errors.append("`variants` must have at least one entry")
        else:
            for v_key, v in variants.items():
                if not isinstance(v, dict):
                    errors.append(f"Variant `{v_key}` must be a mapping")
                    continue
                for field in ("title", "text"):
                    if not v.get(field) or not isinstance(v[field], str):
                        errors.append(f"Variant `{v_key}` is missing required field: `{field}`")

    if "questions" in data:
        questions = data["questions"]
        if not isinstance(questions, list) or len(questions) == 0:
            errors.append("`questions` must be a non-empty list")
        else:
            seen_ids: set[str] = set()
            for i, q in enumerate(questions):
                label = q.get("id") or f"#{i + 1}"
                if not isinstance(q, dict):
                    errors.append(f"Question {label} must be a mapping")
                    continue
                q_id = q.get("id")
                if not q_id or not isinstance(q_id, str):
                    errors.append(f"Question {label} is missing a string `id`")
                elif q_id in seen_ids:
                    errors.append(f"Duplicate question id: `{q_id}`")
                else:
                    seen_ids.add(q_id)
                if not q.get("text") or not isinstance(q["text"], str):
                    errors.append(f"Question `{label}` is missing `text`")
                answer_type = q.get("answer_type", "freetext")
                if answer_type not in ("choice", "freetext"):
                    errors.append(f"Question `{label}` has invalid `answer_type`")
                if answer_type == "choice":
                    opts = q.get("answer_options")
                    if not opts or not isinstance(opts, list) or len(opts) < 2:
                        errors.append(f"Question `{label}` needs at least 2 `answer_options`")
                    elif len(opts) > 5:
                        errors.append(f"Question `{label}` has too many `answer_options` (max 5)")
                cond = q.get("if")
                if cond is not None:
                    if not isinstance(cond, dict):
                        errors.append(f"Question `{label}` `if` must be a mapping")
                    else:
                        for ref_id in cond:
                            if ref_id not in seen_ids:
                                errors.append(
                                    f"Question `{label}` `if` references unknown question `{ref_id}` "
                                    f"(must appear before this question)"
                                )
    else:
        # Legacy single-question format
        if not data.get("question") or not isinstance(data["question"], str):
            errors.append("Missing or empty required field: `question`")
        answer_type = data.get("answer_type", "freetext")
        if answer_type not in ("choice", "freetext"):
            errors.append("`answer_type` must be `choice` or `freetext`")
        if answer_type == "choice":
            opts = data.get("answer_options")
            if not opts or not isinstance(opts, list) or len(opts) < 2:
                errors.append("`answer_options` must have at least 2 items")
            elif len(opts) > 5:
                errors.append("`answer_options` must have at most 5 items")

    return errors


# ---------------------------------------------------------------------------
# Permissions
# ---------------------------------------------------------------------------
# Role hierarchy (each level is strictly additive):
#   admin              — hardcoded ADMIN_USER_IDS; full access including host filesystem
#   server_admin       — Discord manage_guild permission; everything except host file I/O
#   experiment_manager — role set via /set_experiment_role; results + upload + export
#   everyone           — /participate only

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_USER_IDS


def is_server_admin(interaction: discord.Interaction) -> bool:
    if is_admin(interaction.user.id):
        return True
    if not isinstance(interaction.user, discord.Member):
        return False
    return interaction.user.guild_permissions.manage_guild


def is_experiment_manager(interaction: discord.Interaction) -> bool:
    if is_admin(interaction.user.id):
        return True
    if interaction.guild_id is None:
        return False
    role_id = get_setting(interaction.guild_id, "experiment_manager_role_id")
    if role_id and isinstance(interaction.user, discord.Member):
        return any(str(r.id) == role_id for r in interaction.user.roles)
    return False


# ---------------------------------------------------------------------------
# Survey session
# ---------------------------------------------------------------------------

class SurveySession:
    def __init__(self, exp: dict, variant_key: str, user_id: int, username: str):
        self.exp = normalize_experiment(exp)
        self.variant_key = variant_key
        self.user_id = user_id
        self.username = username
        self.answers: dict[str, str] = {}
        self.history: list[str] = []

    @property
    def experiment_name(self) -> str:
        return self.exp["name"]

    def _question_by_id(self, q_id: str) -> dict:
        for q in self.exp["questions"]:
            if q["id"] == q_id:
                return q
        raise KeyError(q_id)

    def _condition_met(self, q: dict) -> bool:
        cond = q.get("if")
        if cond is None:
            return True
        return all(self.answers.get(k) == v for k, v in cond.items())

    def current_question(self) -> dict | None:
        answered = set(self.answers)
        for q in self.exp["questions"]:
            if q["id"] not in answered and self._condition_met(q):
                return q
        return None

    def record_answer(self, q_id: str, answer: str):
        self.answers[q_id] = answer
        if q_id not in self.history:
            self.history.append(q_id)

    def go_back(self):
        if not self.history:
            return
        last_id = self.history.pop()
        self.answers.pop(last_id, None)

    def render_content(self) -> str:
        variant = self.exp["variants"][self.variant_key]
        lines = [f"## {variant['title']}", "", variant["text"].strip(), "---", ""]

        for q_id in self.history:
            q = self._question_by_id(q_id)
            ans = self.answers[q_id]
            if ans == "":
                ans_display = "*(skipped)*"
            else:
                truncated = ans if len(ans) <= 80 else ans[:80] + "…"
                ans_display = f"**{truncated}**"
            lines.append(f"{q['text']}  →  {ans_display}")

        current = self.current_question()
        if current:
            if self.history:
                lines.append("")
            lines.append(f"**{current['text']}**")
        else:
            lines.append("")
            lines.append("*All done — submit when ready, or go back to change an answer.*")

        content = "\n".join(lines)
        if len(content) > 2000:
            content = content[:1997] + "…"
        return content


# ---------------------------------------------------------------------------
# Survey views
# ---------------------------------------------------------------------------

def build_survey_view(session: SurveySession) -> discord.ui.View:
    q = session.current_question()
    if q is None:
        return SurveySubmitView(session)
    if q.get("answer_type", "freetext") == "choice":
        return SurveyChoiceView(session, q)
    return SurveyFreetextView(session, q)


class SurveyChoiceView(discord.ui.View):
    def __init__(self, session: SurveySession, question: dict):
        super().__init__(timeout=600)
        self.session = session
        self.question = question

        for option in question.get("answer_options", []):
            btn = discord.ui.Button(label=option, style=discord.ButtonStyle.primary, row=0)
            btn.callback = self._make_answer_callback(option)
            self.add_item(btn)

        if question.get("optional"):
            skip = discord.ui.Button(label="Skip", style=discord.ButtonStyle.secondary, row=1)
            skip.callback = self._skip
            self.add_item(skip)

        if session.history:
            back = discord.ui.Button(label="← Back", style=discord.ButtonStyle.secondary, row=1)
            back.callback = self._go_back
            self.add_item(back)

    def _make_answer_callback(self, option: str):
        async def callback(interaction: discord.Interaction):
            if interaction.user.id != self.session.user_id:
                await interaction.response.send_message("This isn't your survey!", ephemeral=True)
                return
            self.session.record_answer(self.question["id"], option)
            view = build_survey_view(self.session)
            await interaction.response.edit_message(content=self.session.render_content(), view=view)
        return callback

    async def _skip(self, interaction: discord.Interaction):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("This isn't your survey!", ephemeral=True)
            return
        self.session.record_answer(self.question["id"], "")
        view = build_survey_view(self.session)
        await interaction.response.edit_message(content=self.session.render_content(), view=view)

    async def _go_back(self, interaction: discord.Interaction):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("This isn't your survey!", ephemeral=True)
            return
        self.session.go_back()
        view = build_survey_view(self.session)
        await interaction.response.edit_message(content=self.session.render_content(), view=view)


class SurveyFreetextModal(discord.ui.Modal):
    def __init__(
        self,
        session: SurveySession,
        question: dict,
        button_interaction: discord.Interaction,
        previous: str = "",
    ):
        super().__init__(title=question["text"][:45])
        self.session = session
        self.question = question
        self.button_interaction = button_interaction

        self.answer_input = discord.ui.TextInput(
            label="Your answer",
            style=discord.TextStyle.paragraph,
            default=previous or None,
            required=True,
            max_length=1000,
        )
        self.add_item(self.answer_input)

    async def on_submit(self, interaction: discord.Interaction):
        self.session.record_answer(self.question["id"], self.answer_input.value)
        view = build_survey_view(self.session)
        await interaction.response.defer()
        await self.button_interaction.edit_original_response(
            content=self.session.render_content(), view=view
        )


class SurveyFreetextView(discord.ui.View):
    def __init__(self, session: SurveySession, question: dict):
        super().__init__(timeout=600)
        self.session = session
        self.question = question

        answer_btn = discord.ui.Button(label="Answer", style=discord.ButtonStyle.primary, row=0)
        answer_btn.callback = self._open_modal
        self.add_item(answer_btn)

        if question.get("optional"):
            skip_btn = discord.ui.Button(label="Skip", style=discord.ButtonStyle.secondary, row=0)
            skip_btn.callback = self._skip
            self.add_item(skip_btn)

        if session.history:
            back_btn = discord.ui.Button(label="← Back", style=discord.ButtonStyle.secondary, row=0)
            back_btn.callback = self._go_back
            self.add_item(back_btn)

    async def _open_modal(self, interaction: discord.Interaction):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("This isn't your survey!", ephemeral=True)
            return
        previous = self.session.answers.get(self.question["id"], "")
        modal = SurveyFreetextModal(self.session, self.question, interaction, previous)
        await interaction.response.send_modal(modal)

    async def _skip(self, interaction: discord.Interaction):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("This isn't your survey!", ephemeral=True)
            return
        self.session.record_answer(self.question["id"], "")
        view = build_survey_view(self.session)
        await interaction.response.edit_message(content=self.session.render_content(), view=view)

    async def _go_back(self, interaction: discord.Interaction):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("This isn't your survey!", ephemeral=True)
            return
        self.session.go_back()
        view = build_survey_view(self.session)
        await interaction.response.edit_message(content=self.session.render_content(), view=view)


class SurveySubmitView(discord.ui.View):
    def __init__(self, session: SurveySession):
        super().__init__(timeout=600)
        self.session = session

        submit_btn = discord.ui.Button(label="Submit", style=discord.ButtonStyle.success, row=0)
        submit_btn.callback = self._submit
        self.add_item(submit_btn)

        back_btn = discord.ui.Button(label="← Back", style=discord.ButtonStyle.secondary, row=0)
        back_btn.callback = self._go_back
        self.add_item(back_btn)

    async def _submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("This isn't your survey!", ephemeral=True)
            return
        if has_participated(self.session.user_id, self.session.experiment_name):
            await interaction.response.edit_message(
                content="You've already submitted responses for this experiment.", view=None
            )
            return
        record_response(
            self.session.experiment_name,
            self.session.user_id,
            self.session.username,
            self.session.variant_key,
            json.dumps(self.session.answers),
        )
        await interaction.response.edit_message(
            content="**Responses submitted!** Thanks for participating.", view=None
        )

    async def _go_back(self, interaction: discord.Interaction):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("This isn't your survey!", ephemeral=True)
            return
        self.session.go_back()
        view = build_survey_view(self.session)
        await interaction.response.edit_message(content=self.session.render_content(), view=view)


# ---------------------------------------------------------------------------
# Experiment picker (shown when multiple experiments are active)
# ---------------------------------------------------------------------------

class ExperimentPickerView(discord.ui.View):
    def __init__(self, experiments: list[dict], user_id: int, username: str):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.username = username
        for exp in experiments:
            btn = discord.ui.Button(label=exp["name"][:80], style=discord.ButtonStyle.primary)
            btn.callback = self._make_callback(exp)
            self.add_item(btn)

    def _make_callback(self, exp: dict):
        async def callback(interaction: discord.Interaction):
            if interaction.user.id != self.user_id:
                await interaction.response.send_message("This isn't your prompt!", ephemeral=True)
                return
            if has_participated(interaction.user.id, exp["name"]):
                await interaction.response.edit_message(
                    content="You've already participated in that experiment.", view=None
                )
                return
            variant_key = assign_variant(exp["name"], list(exp["variants"].keys()))
            session = SurveySession(exp, variant_key, interaction.user.id, self.username)
            view = build_survey_view(session)
            await interaction.response.edit_message(content=session.render_content(), view=view)
        return callback


# ---------------------------------------------------------------------------
# Admin views
# ---------------------------------------------------------------------------

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


class ConfirmExperimentUpdateView(discord.ui.View):
    def __init__(self, name: str, config_json: str, summary: str):
        super().__init__(timeout=60)
        self.name = name
        self.config_json = config_json
        self.summary = summary

    @discord.ui.button(label="Update config", style=discord.ButtonStyle.primary)
    async def update_only(self, interaction: discord.Interaction, button: discord.ui.Button):
        update_experiment_config(self.name, self.config_json)
        self.clear_items()
        await interaction.response.edit_message(
            content=f"Experiment updated (existing responses preserved).\n{self.summary}",
            view=self,
        )

    @discord.ui.button(label="Update + reset responses", style=discord.ButtonStyle.danger)
    async def update_and_reset(self, interaction: discord.Interaction, button: discord.ui.Button):
        update_experiment_config(self.name, self.config_json)
        delete_results(self.name)
        self.clear_items()
        await interaction.response.edit_message(
            content=f"Experiment updated and all responses reset.\n{self.summary}",
            view=self,
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.clear_items()
        await interaction.response.edit_message(content="Upload cancelled.", view=self)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_answers(answer_str: str) -> dict[str, str]:
    try:
        parsed = json.loads(answer_str)
        if isinstance(parsed, dict):
            return parsed
    except (json.JSONDecodeError, ValueError):
        pass
    return {"q1": answer_str}  # legacy single-answer


def _seed_from_file_if_empty():
    """On first run, load experiment.yaml into the DB if no active experiments exist."""
    if get_active_experiments():
        return
    if not os.path.exists(EXPERIMENT_PATH):
        return
    try:
        with open(EXPERIMENT_PATH) as f:
            data = yaml.safe_load(f)
        if not validate_experiment(data):
            add_experiment(data["name"], json.dumps(data), "startup")
            print(f"Seeded initial experiment from {EXPERIMENT_PATH}: {data['name']}")
    except Exception as exc:
        print(f"Could not seed from {EXPERIMENT_PATH}: {exc}")


# ---------------------------------------------------------------------------
# Autocomplete
# ---------------------------------------------------------------------------

async def active_experiment_autocomplete(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    return [
        app_commands.Choice(name=e["name"], value=e["name"])
        for e in get_active_experiments()
        if not current or current.lower() in e["name"].lower()
    ][:25]


async def any_experiment_autocomplete(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT name, active FROM experiments ORDER BY active DESC, started_at DESC"
        ).fetchall()
    return [
        app_commands.Choice(
            name=row["name"] if row["active"] else f"{row['name']} (ended)",
            value=row["name"],
        )
        for row in rows
        if not current or current.lower() in row["name"].lower()
    ][:25]


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------

class BotCommandTree(app_commands.CommandTree):
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if ALLOWED_GUILD_IDS and interaction.guild_id not in ALLOWED_GUILD_IDS:
            await interaction.response.send_message(
                "This bot is not enabled for this server.", ephemeral=True
            )
            return False
        return True


intents = discord.Intents.default()
client = discord.Client(intents=intents)
tree = BotCommandTree(client)


@client.event
async def on_ready():
    db_init()
    _seed_from_file_if_empty()
    await tree.sync()
    print(f"Logged in as {client.user}  |  Commands synced")


@tree.command(name="participate", description="Take part in a thought experiment")
@app_commands.checks.cooldown(1, 10.0, key=lambda i: i.user.id)
async def participate(interaction: discord.Interaction):
    active = get_active_experiments()

    if not active:
        await interaction.response.send_message(
            "No experiments are currently running.", ephemeral=True
        )
        return

    available = [e for e in active if not has_participated(interaction.user.id, e["name"])]

    if not available:
        n = len(active)
        await interaction.response.send_message(
            f"You've already participated in all {n} running experiment{'s' if n != 1 else ''}. Thanks!",
            ephemeral=True,
        )
        return

    if len(available) == 1:
        exp = available[0]
        variant_key = assign_variant(exp["name"], list(exp["variants"].keys()))
        session = SurveySession(exp, variant_key, interaction.user.id, str(interaction.user))
        view = build_survey_view(session)
        await interaction.response.send_message(session.render_content(), view=view, ephemeral=True)
    else:
        view = ExperimentPickerView(available, interaction.user.id, str(interaction.user))
        await interaction.response.send_message(
            "**Choose an experiment to participate in:**", view=view, ephemeral=True
        )


@participate.error
async def participate_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CommandOnCooldown):
        await interaction.response.send_message(
            f"Please wait {error.retry_after:.0f}s before participating again.", ephemeral=True
        )


@tree.command(name="set_experiment_role", description="[Server admin] Set which role can manage experiments")
@app_commands.describe(role="The role to grant experiment-management permissions")
async def set_experiment_role(interaction: discord.Interaction, role: discord.Role):
    if not is_server_admin(interaction):
        await interaction.response.send_message(
            "You need the **Manage Server** permission to set this.", ephemeral=True
        )
        return
    set_setting(interaction.guild_id, "experiment_manager_role_id", str(role.id))
    await interaction.response.send_message(
        f"Members with the **{role.name}** role can now manage experiments.", ephemeral=True
    )


@tree.command(name="upload_experiment", description="Upload a YAML file to add or update an experiment")
@app_commands.describe(file="A .yaml file defining the experiment")
async def upload_experiment(interaction: discord.Interaction, file: discord.Attachment):
    if not is_experiment_manager(interaction):
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

    name = data["name"]
    active = get_active_experiments()
    is_update = any(e["name"] == name for e in active)

    if not is_update and len(active) >= MAX_ACTIVE_EXPERIMENTS:
        await interaction.followup.send(
            f"There are already {MAX_ACTIVE_EXPERIMENTS} active experiments. "
            f"End one with `/end_experiment` before adding a new one.",
            ephemeral=True,
        )
        return

    normalized = normalize_experiment(data)
    q_count = len(normalized["questions"])
    variant_lines = "\n".join(
        f"Variant {k}: {v['title']}" for k, v in data["variants"].items()
    )
    config_json = json.dumps(data)
    summary = f"**{name}** — {q_count} question{'s' if q_count != 1 else ''}\n{variant_lines}"

    if is_update:
        n = count_responses(name)
        response_word = f"{n} response{'s' if n != 1 else ''}"
        view = ConfirmExperimentUpdateView(name, config_json, summary)
        corruption_warning = (
            "\n⚠️ Keeping existing responses may cause **data corruption**: "
            "old answers were recorded against the previous question and variant "
            "definitions, so mixed results will be unreliable."
            if n > 0 else ""
        )
        await interaction.followup.send(
            f"**{name}** already exists with {response_word}. "
            f"Update the config, or update and reset all responses?"
            f"{corruption_warning}",
            view=view,
            ephemeral=True,
        )
        return

    add_experiment(name, config_json, str(interaction.user.id))
    count_after = len(active) + 1
    await interaction.followup.send(
        f"Experiment added ({count_after}/{MAX_ACTIVE_EXPERIMENTS} active)!\n{summary}",
        ephemeral=True,
    )

    announcement = f"@here {interaction.user.mention} started a thought experiment: **{name}**"
    description = data.get("description")
    if description:
        announcement += f"\nDescription: {description}"
    announcement += "\nParticipate with `/participate`"
    await interaction.channel.send(announcement)


@tree.command(name="end_experiment", description="[Experiment manager] Stop accepting responses for an experiment")
@app_commands.describe(experiment="The experiment to end")
@app_commands.autocomplete(experiment=active_experiment_autocomplete)
async def end_experiment(interaction: discord.Interaction, experiment: str):
    if not (is_server_admin(interaction) or is_experiment_manager(interaction)):
        await interaction.response.send_message(
            "You don't have permission to end experiments.", ephemeral=True
        )
        return

    if not is_active_experiment(experiment):
        await interaction.response.send_message(
            f"**{experiment}** is not currently active.", ephemeral=True
        )
        return

    counts = get_variant_counts(experiment)
    total = sum(counts.values())
    deactivate_experiment(experiment)
    await interaction.response.send_message(
        f"**{experiment}** has ended. "
        f"{total} response{'s' if total != 1 else ''} retained — use `/results` or `/export` to view them.",
        ephemeral=True,
    )


@tree.command(name="results", description="[Admin] Show results summary for an experiment")
@app_commands.describe(experiment="The experiment to show results for")
@app_commands.autocomplete(experiment=any_experiment_autocomplete)
async def results(interaction: discord.Interaction, experiment: str):
    if not (is_server_admin(interaction) or is_experiment_manager(interaction)):
        await interaction.response.send_message("You don't have permission to view results.", ephemeral=True)
        return

    exp_config = get_experiment_by_name(experiment)
    if exp_config is None:
        await interaction.response.send_message(f"No experiment named **{experiment}** found.", ephemeral=True)
        return

    exp = normalize_experiment(exp_config)
    rows = get_results(experiment)

    if not rows:
        await interaction.response.send_message(f"No responses yet for **{experiment}**.", ephemeral=True)
        return

    variant_keys = list(exp["variants"].keys())
    tally: dict[str, dict[str, dict[str, int]]] = {v: {} for v in variant_keys}
    for row in rows:
        v = row["variant"]
        tally.setdefault(v, {})
        for q_id, ans in _parse_answers(row["answer"]).items():
            tally[v].setdefault(q_id, {})
            tally[v][q_id][ans] = tally[v][q_id].get(ans, 0) + 1

    total = len(rows)
    status = "" if is_active_experiment(experiment) else " — ended"
    lines = [f"**{experiment}**{status} ({total} response{'s' if total != 1 else ''})\n"]

    question_texts = {q["id"]: q["text"] for q in exp["questions"]}

    for v_key in variant_keys:
        v_data = exp["variants"].get(v_key)
        if not v_data:
            continue
        respondents = sum(1 for row in rows if row["variant"] == v_key)
        lines.append(
            f"**Variant {v_key} — {v_data['title']}** "
            f"({respondents} respondent{'s' if respondents != 1 else ''})"
        )
        for q in exp["questions"]:
            q_id = q["id"]
            q_counts = tally.get(v_key, {}).get(q_id)
            if q_counts is None:
                continue
            q_total = sum(q_counts.values())
            lines.append(f"  *{question_texts.get(q_id, q_id)}* ({q_total})")
            if q.get("answer_type", "freetext") == "freetext":
                skipped = q_counts.get("", 0)
                responded = q_total - skipped
                lines.append(f"    {skipped}: no response (skipped)")
                lines.append(f"    {responded}: response")
            else:
                for ans, count in sorted(q_counts.items(), key=lambda x: -x[1]):
                    pct = count / q_total * 100
                    lines.append(f"    {ans}: {count} ({pct:.0f}%)")
        lines.append("")

    content = "\n".join(lines)
    if len(content) > 2000:
        content = content[:1997] + "…"
    await interaction.response.send_message(content, ephemeral=True)


@tree.command(name="export", description="[Experiment manager] Download results as a CSV")
@app_commands.describe(experiment="The experiment to export")
@app_commands.autocomplete(experiment=any_experiment_autocomplete)
async def export(interaction: discord.Interaction, experiment: str):
    if not is_experiment_manager(interaction):
        await interaction.response.send_message("You don't have permission to export results.", ephemeral=True)
        return

    exp_config = get_experiment_by_name(experiment)
    if exp_config is None:
        await interaction.response.send_message(f"No experiment named **{experiment}** found.", ephemeral=True)
        return

    rows = get_results(experiment)
    if not rows:
        await interaction.response.send_message(f"No responses yet for **{experiment}**.", ephemeral=True)
        return

    exp = normalize_experiment(exp_config)
    question_ids = [q["id"] for q in exp["questions"]]
    safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in experiment)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"results_{safe_name}_{timestamp}.csv"

    fields = (
        ["experiment", "username", "user_id", "variant"]
        + [f"answer_{q_id}" for q_id in question_ids]
        + ["responded_at"]
    )

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        d = dict(row)
        parsed = _parse_answers(d.pop("answer", "{}"))
        for q_id in question_ids:
            d[f"answer_{q_id}"] = parsed.get(q_id, "")
        writer.writerow(d)

    attachment = discord.File(io.BytesIO(buf.getvalue().encode()), filename=filename)
    await interaction.response.send_message(
        f"{len(rows)} response(s)", file=attachment, ephemeral=True
    )


@tree.command(name="reset", description="[Server admin] Delete all responses for an experiment")
@app_commands.describe(experiment="The experiment to reset")
@app_commands.autocomplete(experiment=any_experiment_autocomplete)
async def reset(interaction: discord.Interaction, experiment: str):
    if not is_server_admin(interaction):
        await interaction.response.send_message("You don't have permission to reset results.", ephemeral=True)
        return

    if get_experiment_by_name(experiment) is None:
        await interaction.response.send_message(f"No experiment named **{experiment}** found.", ephemeral=True)
        return

    counts = get_variant_counts(experiment)
    total = sum(counts.values())
    view = ConfirmResetView(experiment)
    await interaction.response.send_message(
        f"This will delete **{total}** response(s) for **{experiment}**. Are you sure?",
        view=view,
        ephemeral=True,
    )


client.run(BOT_TOKEN)
