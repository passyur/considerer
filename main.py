import csv
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
DB_PATH = os.getenv("DB_PATH", "results.db")
EXPERIMENT_PATH = os.getenv("EXPERIMENT_PATH", "experiment.yaml")

MAX_YAML_BYTES = 64 * 1024


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

async def has_experiment_permission(interaction: discord.Interaction) -> bool:
    if interaction.user.id in ADMIN_USER_IDS:
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
        self.history: list[str] = []  # question ids answered, in order

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
        """Next question to show: not yet answered, condition met."""
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
        """Undo the last recorded answer."""
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
            ans_display = "*(skipped)*" if ans == "" else f"**{ans}**"
            lines.append(f"{q['text']}  →  {ans_display}")

        current = self.current_question()
        if current:
            if self.history:
                lines.append("")
            lines.append(f"**{current['text']}**")
        else:
            lines.append("")
            lines.append("*All done — submit when ready, or go back to change an answer.*")

        return "\n".join(lines)


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
        # Edit the original ephemeral message via the button interaction's webhook token
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


# ---------------------------------------------------------------------------
# Helpers for results/export with multi-question answers
# ---------------------------------------------------------------------------

def _parse_answers(answer_str: str) -> dict[str, str]:
    """Parse the answer column. Returns a dict of question_id -> answer."""
    try:
        parsed = json.loads(answer_str)
        if isinstance(parsed, dict):
            return parsed
    except (json.JSONDecodeError, ValueError):
        pass
    return {"q1": answer_str}  # legacy single-answer


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
    session = SurveySession(exp, variant_key, interaction.user.id, str(interaction.user))
    view = build_survey_view(session)
    await interaction.response.send_message(session.render_content(), view=view, ephemeral=True)


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
@app_commands.describe(file="A .yaml file defining the experiment")
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

    normalized = normalize_experiment(data)
    q_count = len(normalized["questions"])
    variant_a = data["variants"]["A"]["title"]
    variant_b = data["variants"]["B"]["title"]

    await interaction.followup.send(
        f"Experiment set!\n"
        f"**{data['name']}** — {q_count} question{'s' if q_count != 1 else ''}\n"
        f"Variant A: {variant_a}\n"
        f"Variant B: {variant_b}",
        ephemeral=True,
    )


@tree.command(name="results", description="[Experiment manager] Show results summary for the current experiment")
async def results(interaction: discord.Interaction):
    if not await has_experiment_permission(interaction):
        await interaction.response.send_message("You don't have permission to view results.", ephemeral=True)
        return

    exp = normalize_experiment(load_experiment())
    name = exp["name"]
    rows = get_results(name)

    if not rows:
        await interaction.response.send_message(f"No responses yet for **{name}**.", ephemeral=True)
        return

    # tally[variant][question_id][answer] = count
    tally: dict[str, dict[str, dict[str, int]]] = {"A": {}, "B": {}}
    for row in rows:
        v = row["variant"]
        for q_id, ans in _parse_answers(row["answer"]).items():
            tally[v].setdefault(q_id, {})
            tally[v][q_id][ans] = tally[v][q_id].get(ans, 0) + 1

    total = len(rows)
    lines = [f"**Experiment: {name}** ({total} response{'s' if total != 1 else ''})\n"]

    question_texts = {q["id"]: q["text"] for q in exp["questions"]}

    for v_key in ("A", "B"):
        v_data = exp["variants"].get(v_key)
        if not v_data:
            continue
        v_total = sum(sum(a.values()) for a in tally.get(v_key, {}).values())
        # count unique respondents per variant
        respondents = sum(
            1 for row in rows if row["variant"] == v_key
        )
        lines.append(f"**Variant {v_key} — {v_data['title']}** ({respondents} respondent{'s' if respondents != 1 else ''})")

        for q in exp["questions"]:
            q_id = q["id"]
            q_counts = tally.get(v_key, {}).get(q_id)
            if q_counts is None:
                continue
            q_total = sum(q_counts.values())
            lines.append(f"  *{question_texts.get(q_id, q_id)}* ({q_total})")
            for ans, count in sorted(q_counts.items(), key=lambda x: -x[1]):
                pct = count / q_total * 100
                lines.append(f"    {ans}: {count} ({pct:.0f}%)")

        lines.append("")

    await interaction.response.send_message("\n".join(lines), ephemeral=True)


@tree.command(name="export", description="[Experiment manager] Export results to a CSV file")
async def export(interaction: discord.Interaction):
    if not await has_experiment_permission(interaction):
        await interaction.response.send_message("You don't have permission to export results.", ephemeral=True)
        return

    exp = normalize_experiment(load_experiment())
    name = exp["name"]
    rows = get_results(name)

    if not rows:
        await interaction.response.send_message(f"No responses yet for **{name}**.", ephemeral=True)
        return

    question_ids = [q["id"] for q in exp["questions"]]
    safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in name)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"results_{safe_name}_{timestamp}.csv"

    fields = (
        ["experiment", "username", "user_id", "variant"]
        + [f"answer_{q_id}" for q_id in question_ids]
        + ["responded_at"]
    )

    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            d = dict(row)
            parsed = _parse_answers(d.pop("answer", "{}"))
            for q_id in question_ids:
                d[f"answer_{q_id}"] = parsed.get(q_id, "")
            writer.writerow(d)

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
