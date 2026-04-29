# commands & server setup

## server setup (do this once)

invite the bot with `bot` + `applications.commands` scopes ([developer portal](https://discord.com/developers/applications))

then as someone with **Manage Server**:

```
/set_experiment_role @your-role
```

that role can now manage experiments. without it, only the bot owner (hardcoded in `.env`) can.

---

## who can do what

### everyone
| command | what it does |
|---|---|
| `/participate` | join a running experiment — you'll get a picker if multiple are active, otherwise will start if one exists |

responses are ephemeral. you won't see which variant you got until after you answer.

### experiment managers *(the role set above, or bot owner)*
| command | what it does |
|---|---|
| `/upload_experiment` | upload a `.yaml` file to start a new experiment (or replace an existing one's config without losing responses) |
| `/end_experiment` | close an experiment to new responses — data is kept |
| `/results` | see a breakdown of responses by variant and question |
| `/export` | download a csv — sent as a file attachment, not written to the host |

up to 5 experiments can be active at once.

### server admins *(Manage Server permission, or bot owner)*
everything above, plus:

| command | what it does |
|---|---|
| `/set_experiment_role @role` | change which role can manage experiments |
| `/reset` | wipe all responses for an experiment (asks for confirmation) |

---

## experiment yaml

```yaml
name: "my experiment"   # changing this starts a fresh dataset

variants:
  A:
    title: "variant a"
    text: |
      scenario text for variant a
  B:
    title: "variant b"
    text: |
      scenario text for variant b

questions:
  - id: q1
    text: "first question?"
    answer_type: choice        # or: freetext
    answer_options: ["Yes", "No"]

  - id: q2
    text: "follow-up if yes?"
    answer_type: choice
    answer_options: ["reason a", "reason b"]
    if:
      q1: "Yes"              # only shown if q1 was answered "Yes"

  - id: q3
    text: "anything to add?"
    answer_type: freetext
    optional: true           # adds a skip button
```

variants are assigned evenly. answers aren't saved until the respondent hits submit.
