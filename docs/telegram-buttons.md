# Sending Telegram Messages With Interactive Buttons

A practical guide for `python-telegram-bot` (PTB) v20+. Covers inline-keyboard messages, callback handling, routing, common patterns, and the gotchas that actually bite in production. Examples are concrete and copy-pasteable.

---

## 1. The two pieces

A user-tappable Telegram button requires **two** things wired into your bot:

1. A **message** sent with an `InlineKeyboardMarkup` attached (the buttons).
2. A **CallbackQueryHandler** registered on the application (the handler that receives the tap).

Without (2), the buttons render but nothing happens when the user presses them.

---

## 2. Sending a message with buttons

```python
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

keyboard = InlineKeyboardMarkup([
    # Each inner list is a ROW. Two rows here, two buttons per row.
    [
        InlineKeyboardButton("⛔ Stop bot", callback_data="bot:stop:btcbrl-v4"),
        InlineKeyboardButton("⚖️ Rebalance", callback_data="bot:rebal:btcbrl-v4"),
    ],
    [
        InlineKeyboardButton("✅ Acknowledge", callback_data="bot:ack:btcbrl-v4"),
    ],
])

await bot.send_message(
    chat_id=chat_id,
    text="🚨 *Volume drop detected on btcbrl-v4*",
    parse_mode="Markdown",
    reply_markup=keyboard,
)
```

**Anatomy:**
- `InlineKeyboardMarkup([...])` takes a **list of rows**. Each row is a **list of buttons**.
- `InlineKeyboardButton(label, callback_data="…")` — `label` is what the user sees, `callback_data` is the opaque string the bot receives when pressed.
- `reply_markup=` is the parameter on `send_message` that attaches the keyboard.

---

## 3. Handling button presses

When a user taps a button, Telegram delivers an `Update` with `update.callback_query` populated. You register a `CallbackQueryHandler` on the `Application`:

```python
from telegram import Update
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

async def bot_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    # IMPORTANT: always answer the callback or the user sees a spinner forever.
    await query.answer()

    # callback_data is a string — parse it however you want.
    # Convention used in this project: "<namespace>:<action>:<arg>"
    data = query.data  # e.g. "bot:stop:btcbrl-v4"
    parts = data.split(":")
    if len(parts) < 2:
        return
    _ns, action, *args = parts

    if action == "stop":
        bot_name = args[0]
        await stop_bot(bot_name)
        await query.message.edit_text(f"✅ Stopped `{bot_name}`", parse_mode="Markdown")
    elif action == "rebal":
        bot_name = args[0]
        await query.message.edit_text(f"⚖️ Rebalancing `{bot_name}` …", parse_mode="Markdown")
    elif action == "ack":
        # Just drop the buttons — keep the text.
        await query.edit_message_reply_markup(reply_markup=None)


# Register with a `pattern` so this handler only fires for matching callback_data.
application.add_handler(CallbackQueryHandler(bot_callback, pattern="^bot:"))
```

### Why `pattern="^bot:"`?

If you have multiple features each using inline buttons (trading, agent settings, alerts, …) you'll register **one CallbackQueryHandler per namespace**, each with its own regex. Without a pattern, every handler runs for every press and you'll wire the wrong logic. The convention in this codebase is to prefix every `callback_data` with a namespace (`bot:`, `agent:`, `volalert:`, `cex:`, `dex:`) and register handlers like:

```python
application.add_handler(CallbackQueryHandler(agent_callback, pattern="^agent:"))
application.add_handler(CallbackQueryHandler(cex_callback,   pattern="^cex:"))
application.add_handler(CallbackQueryHandler(dex_callback,   pattern="^dex:"))
```

---

## 4. Editing the message after a press

You almost always want to update the UI after a press — either to acknowledge the action, hide the buttons, or move to the next step. PTB gives you a few options:

```python
# Replace text + buttons (most common — "go to next step")
await query.message.edit_text("New text", reply_markup=new_keyboard)

# Replace text, drop buttons
await query.message.edit_text("Done.")

# Keep text, replace buttons only
await query.edit_message_reply_markup(reply_markup=new_keyboard)

# Keep text, drop buttons (ack-style)
await query.edit_message_reply_markup(reply_markup=None)
```

A common mistake: calling `bot.send_message` instead of editing produces a *new* message, leaving the old one with stale buttons that still respond. Edit, don't append, when continuing a flow.

---

## 5. Patterns that show up a lot

### 5.1 Confirmation prompt (Yes / No)

```python
def confirm_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes",    callback_data=f"confirm:yes:{token}"),
        InlineKeyboardButton("❌ Cancel", callback_data=f"confirm:no:{token}"),
    ]])

await bot.send_message(
    chat_id=chat_id,
    text="Close all positions on btcbrl-v4?",
    reply_markup=confirm_keyboard("req-7f3a"),
)
```

Handler:

```python
async def confirm_callback(update, context):
    q = update.callback_query
    await q.answer()
    _, decision, token = q.data.split(":", 2)
    if decision == "yes":
        await execute(token)
        await q.message.edit_text("✅ Confirmed.")
    else:
        await q.message.edit_text("❌ Cancelled.")
```

The `token` lets you correlate the press with whatever pending state the bot was waiting on. Store the pending request in a dict keyed by `token` and pop it on either branch.

### 5.2 Single-select menu

```python
options = ["claude-code", "gemini", "ollama", "openrouter"]
keyboard = InlineKeyboardMarkup([
    [InlineKeyboardButton(label, callback_data=f"llm:set:{label}")]
    for label in options
])
await bot.send_message(chat_id, "Pick an LLM:", reply_markup=keyboard)
```

### 5.3 Paginated picker

When you have more options than fit in one screen, paginate using callback_data to encode the page number:

```python
PAGE_SIZE = 8

def picker(items: list[str], page: int) -> InlineKeyboardMarkup:
    total_pages = (len(items) + PAGE_SIZE - 1) // PAGE_SIZE
    page = max(0, min(page, total_pages - 1))
    start = page * PAGE_SIZE
    rows = [
        [InlineKeyboardButton(item, callback_data=f"pick:item:{i}")]
        for i, item in enumerate(items[start:start + PAGE_SIZE], start=start)
    ]
    # Nav row at the bottom
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("‹ Prev", callback_data=f"pick:page:{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="pick:noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ›", callback_data=f"pick:page:{page+1}"))
    rows.append(nav)
    return InlineKeyboardMarkup(rows)
```

Handler edits the same message in place with a new page:

```python
async def pick_callback(update, context):
    q = update.callback_query
    await q.answer()
    _, action, arg = q.data.split(":", 2)
    if action == "page":
        await q.edit_message_reply_markup(reply_markup=picker(items, int(arg)))
    elif action == "item":
        chosen = items[int(arg)]
        await q.message.edit_text(f"Selected: `{chosen}`", parse_mode="Markdown")
    elif action == "noop":
        pass  # the page indicator is a button but does nothing
```

### 5.4 Multi-select with state

For "pick several, then confirm" flows, store the current selection in `context.user_data` (PTB persists this per-user automatically if you've configured persistence):

```python
async def toggle_callback(update, context):
    q = update.callback_query
    await q.answer()
    _, item = q.data.split(":", 1)
    selected = context.user_data.setdefault("multi_selected", set())
    if item in selected:
        selected.remove(item)
    else:
        selected.add(item)
    # Rebuild the keyboard so checkmarks reflect the new state
    await q.edit_message_reply_markup(reply_markup=multi_keyboard(selected))
```

---

## 6. Gotchas (the things that actually bite)

### 6.1 `callback_data` has a **64-byte** hard limit

Telegram rejects any button whose `callback_data` exceeds 64 bytes. Long IDs (UUIDs, long bot names, slugs) blow this fast. Workarounds:

- Cache the long value somewhere keyed by a short token, send the token in `callback_data`:
  ```python
  token = generate_short_token()  # e.g. 6 chars
  context.bot_data.setdefault("pending", {})[token] = long_payload
  callback_data = f"ns:action:{token}"
  ```
- For paginated lists, send the *index* into a cached list rather than the slug.
- Truncating the slug in `callback_data` will silently break the handler — don't.

### 6.2 Always call `await query.answer()`

Without it, the user sees a perpetual loading spinner on the button. `answer()` can also pop a transient toast:

```python
await query.answer("Saved!", show_alert=False)  # tiny toast
await query.answer("Done", show_alert=True)     # modal dialog
```

### 6.3 `parse_mode="Markdown"` is finicky

If you bold/italic with `*` and `_` and Telegram fails to parse, you get HTTP 400 `can't parse entities`. Either:

- Use **`parse_mode="MarkdownV2"`** (stricter, requires escaping `_ * [ ] ( ) ~ \` > # + - = | { } . !`).
- Use **`parse_mode="HTML"`** (`<b>`, `<i>`, `<code>`) — fewer surprises.
- Or wrap variable content in monospace backticks: ``` `{name}` ``` so underscores in IDs don't trigger italics.

### 6.4 Messages cap at **4096 characters**

A `BadRequest: Message is too long` error means you've exceeded the limit. For long status dumps, split at blank-line boundaries so Markdown spans stay intact:

```python
TG_MAX = 3800  # leave buffer under 4096

def chunk(text: str, limit: int = TG_MAX) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks, rem = [], text
    while len(rem) > limit:
        cut = rem.rfind("\n\n", 0, limit) or rem.rfind("\n", 0, limit) or limit
        chunks.append(rem[:cut].rstrip())
        rem = rem[cut:].lstrip("\n")
    if rem: chunks.append(rem)
    return chunks

chunks = chunk(long_text)
for i, c in enumerate(chunks):
    # Attach buttons only to the last chunk so they appear at the bottom.
    await bot.send_message(chat_id=chat_id, text=c,
        reply_markup=keyboard if i == len(chunks) - 1 else None)
```

### 6.5 Buttons on **expired** messages still trigger callbacks

If your handler relies on state that no longer exists (e.g. an old confirmation token), default to a graceful "expired" response:

```python
pending = context.bot_data.get("pending", {})
payload = pending.pop(token, None)
if payload is None:
    await query.message.edit_text("⏰ Request expired.")
    return
```

### 6.6 Group chats: bot must be admin to edit messages

In groups, the bot needs admin rights (and "Edit messages" permission) to call `edit_text` / `edit_message_reply_markup` on messages it sent. In DMs this Just Works.

### 6.7 URL buttons skip your handler entirely

```python
InlineKeyboardButton("Open dashboard", url="https://example.com/")
```

These open in the user's browser without sending a callback. Use them for external links; don't mix `url=` and `callback_data=` on the same button (only one applies).

---

## 7. Minimal end-to-end working example

```python
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

TOKEN = "YOUR_BOT_TOKEN"


def main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🟢 Start", callback_data="demo:start")],
        [InlineKeyboardButton("⏸ Pause", callback_data="demo:pause"),
         InlineKeyboardButton("⏹ Stop",  callback_data="demo:stop")],
    ])


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Pick an action:", reply_markup=main_keyboard())


async def demo_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    _, action = q.data.split(":", 1)
    await q.message.edit_text(f"You pressed: *{action}*", parse_mode="Markdown")


app = Application.builder().token(TOKEN).build()
app.add_handler(CommandHandler("start", start_cmd))
app.add_handler(CallbackQueryHandler(demo_callback, pattern="^demo:"))
app.run_polling()
```

Drop in a token, run it, send `/start` to the bot, tap a button. Three buttons, one callback handler, working in under 30 lines.

---

## 8. Reference: callback_data conventions used in this project

Codebase uses `<namespace>:<action>:<arg1>:<arg2>` consistently. Picking it up makes patterns greppable:

| Namespace | Owner | Example |
|---|---|---|
| `agent:` | `handlers/agents/__init__.py` | `agent:mode:condor`, `agent:set_llm:claude-code` |
| `cex:`   | `handlers/cex.py`              | `cex:place_order:binance:BTC-USDT` |
| `dex:`   | `handlers/dex.py`              | `dex:swap:solana:SOL-USDC` |
| `volalert:` | `routines/volume_drop_alert.py` | `volalert:stopbot:bot_name`, `volalert:ackbot:bot_name` |

Each owner registers its CallbackQueryHandler with `pattern=f"^{namespace}:"` in `main.py::register_handlers`.

That's the whole story for buttons in PTB.
