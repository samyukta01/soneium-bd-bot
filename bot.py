import os, logging, asyncio
from datetime import datetime
import anthropic
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters
from database import Database

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ANTHROPIC_KEY  = os.environ["ANTHROPIC_API_KEY"]
ADMIN_USER_ID  = int(os.environ["ADMIN_TELEGRAM_USER_ID"])

db = Database(os.environ.get("DB_PATH", "bd_bot.db"))

def claude(): return anthropic.Anthropic(api_key=ANTHROPIC_KEY)
def is_admin(uid): return uid == ADMIN_USER_ID
def admin_only(fn):
    async def w(update, ctx):
        if not is_admin(update.effective_user.id): return
        return await fn(update, ctx)
    return w

async def handle_group_message(update, ctx):
    msg  = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not db.is_permitted_chat(chat.id): return
    db.log_message(
        chat_id=chat.id, chat_name=chat.title or str(chat.id),
        user_id=user.id if user else 0,
        username=(user.username or user.full_name) if user else "unknown",
        text=msg.text or msg.caption or "",
        msg_id=msg.message_id, timestamp=datetime.utcnow().isoformat()
    )
    log.info(f"Logged message from {chat.title} ({chat.id})")

async def bot_added_to_group(update, ctx):
    chat = update.effective_chat
    db.permit_chat(chat.id, chat.title or str(chat.id))
    log.info(f"Auto-permitted: {chat.title} ({chat.id})")
    try:
        await ctx.bot.send_message(chat_id=ADMIN_USER_ID,
            text=f"Now tracking: {chat.title}\nID: {chat.id}")
    except Exception as e:
        log.error(f"Could not notify admin: {e}")

@admin_only
async def cmd_start(update, ctx):
    await update.message.reply_text(
        "Soneium BD Intelligence Bot\n\n"
        "/chats - list all tracked chats\n"
        "/summary <chat_id> - full summary of a chat\n"
        "/status <name> - BD status of any account\n"
        "/query <question> - search across all chats\n"
        "/broadcast - send a message to selected chats\n\n"
        "Or just type any question naturally!")

@admin_only
async def cmd_permit(update, ctx):
    if not ctx.args: await update.message.reply_text("Usage: /permit <chat_id>"); return
    cid = int(ctx.args[0])
    name = " ".join(ctx.args[1:]) or str(cid)
    try: obj = await ctx.bot.get_chat(cid); name = obj.title or name
    except: pass
    db.permit_chat(cid, name)
    await update.message.reply_text(f"Now tracking: {name} ({cid})")

@admin_only
async def cmd_unpermit(update, ctx):
    if not ctx.args: await update.message.reply_text("Usage: /unpermit <chat_id>"); return
    db.unpermit_chat(int(ctx.args[0]))
    await update.message.reply_text(f"Stopped tracking {ctx.args[0]}")

@admin_only
async def cmd_chats(update, ctx):
    chats = db.get_permitted_chats()
    if not chats: await update.message.reply_text("No chats tracked yet."); return
    lines = [f"Tracked chats ({len(chats)}):"]
    for c in chats:
        lines.append(f"- {c['chat_name']} ({c['chat_id']}) - {db.get_message_count(c['chat_id'])} msgs")
    await update.message.reply_text("\n".join(lines))

@admin_only
async def cmd_summary(update, ctx):
    if not ctx.args: await update.message.reply_text("Usage: /summary <chat_id>"); return
    cid = int(ctx.args[0])
    if not db.is_permitted_chat(cid): await update.message.reply_text("Chat not tracked."); return
    t = await update.message.reply_text("Analysing...")
    msgs = db.get_messages(cid, limit=500)
    if not msgs: await t.edit_text("No messages logged yet for this chat."); return
    ans = await ai_chat(msgs, db.get_chat_name(cid), "Give a comprehensive BD status summary of all discussions.")
    await t.edit_text(f"Summary: {db.get_chat_name(cid)}\n\n{ans}")

@admin_only
async def cmd_status(update, ctx):
    if not ctx.args: await update.message.reply_text("Usage: /status <name>"); return
    name = " ".join(ctx.args)
    t = await update.message.reply_text(f"Searching for {name}...")
    results = db.search_messages_across_chats(name, limit=200)
    if not results: await t.edit_text(f"Nothing found mentioning {name}."); return
    ans = await ai_cross(results, f"What is the current BD status for {name}? Summarise all context.")
    await t.edit_text(f"Status: {name}\n\n{ans}")

@admin_only
async def cmd_query(update, ctx):
    if not ctx.args: await update.message.reply_text("Usage: /query <question>"); return
    await _query(update, " ".join(ctx.args))

@admin_only
async def handle_private_message(update, ctx):
    q = update.message.text.strip()
    if q.startswith("/"): return
    await _query(update, q)

async def _query(update, q):
    t = await update.message.reply_text("Searching your BD chats...")
    # First try targeted search
    results = db.search_messages_across_chats(q, limit=300)
    # If nothing found, fall back to all recent messages so AI has context
    if not results:
        results = db.get_all_messages(limit=300)
    if not results:
        await t.edit_text("No messages logged yet. Send some messages in your tracked group chats first."); return
    ans = await ai_cross(results, q)
    await t.edit_text(f"Answer\n\n{ans}")

broadcast_state = {}

@admin_only
async def cmd_broadcast(update, ctx):
    broadcast_state[ADMIN_USER_ID] = {"step": "awaiting_message"}
    await update.message.reply_text("Broadcast Wizard\nStep 1/3: Type your message.")

async def handle_broadcast_steps(update, ctx):
    if update.effective_user.id != ADMIN_USER_ID: return
    state = broadcast_state.get(ADMIN_USER_ID, {})
    if not state: return
    text = update.message.text.strip()
    if state["step"] == "awaiting_message":
        state.update({"message": text, "step": "awaiting_time"})
        await update.message.reply_text("Step 2/3: Send time? Type 'now' or '2024-01-15 14:30' (UTC).")
    elif state["step"] == "awaiting_time":
        try:
            state["send_at"] = None if text.lower() == "now" else datetime.strptime(text, "%Y-%m-%d %H:%M")
        except:
            await update.message.reply_text("Format: now or 2024-01-15 14:30"); return
        state.update({"step": "awaiting_chats", "selected_chats": []})
        chats = db.get_permitted_chats()
        if not chats:
            await update.message.reply_text("No chats tracked.")
            broadcast_state.pop(ADMIN_USER_ID, None); return
        await update.message.reply_text("Step 3/3: Select chats to send to.", reply_markup=_kbd(chats, []))

def _kbd(chats, sel):
    rows = [[InlineKeyboardButton(
        ("✅ " if c["chat_id"] in sel else "⬜ ") + c["chat_name"][:28],
        callback_data=f"bc:{c['chat_id']}")] for c in chats]
    rows += [[InlineKeyboardButton("Select All", callback_data="bc_all"),
              InlineKeyboardButton("🚀 Send", callback_data="bc_send")],
             [InlineKeyboardButton("Cancel", callback_data="bc_cancel")]]
    return InlineKeyboardMarkup(rows)

async def handle_broadcast_cb(update, ctx):
    q = update.callback_query; await q.answer()
    if not is_admin(q.from_user.id): return
    state = broadcast_state.get(ADMIN_USER_ID, {})
    if not state: await q.edit_message_text("Expired. Use /broadcast."); return
    chats = db.get_permitted_chats()
    sel   = state.get("selected_chats", [])
    if q.data.startswith("bc:"):
        cid = int(q.data[3:])
        sel.remove(cid) if cid in sel else sel.append(cid)
        state["selected_chats"] = sel
        await q.edit_message_reply_markup(reply_markup=_kbd(chats, sel))
    elif q.data == "bc_all":
        state["selected_chats"] = [c["chat_id"] for c in chats]
        await q.edit_message_reply_markup(reply_markup=_kbd(chats, state["selected_chats"]))
    elif q.data == "bc_send":
        if not sel: await q.edit_message_text("No chats selected."); broadcast_state.pop(ADMIN_USER_ID, None); return
        msg = state["message"]; send_at = state.get("send_at")
        if send_at:
            delay = (send_at - datetime.utcnow()).total_seconds()
            if delay > 0:
                await q.edit_message_text(f"Scheduled for {send_at} UTC.")
                await asyncio.sleep(delay)
        ok = fail = 0
        for cid in sel:
            try: await ctx.bot.send_message(chat_id=cid, text=msg); ok += 1
            except Exception as e: log.error(f"Broadcast fail {cid}: {e}"); fail += 1
        try: await q.edit_message_text(f"Done! Sent: {ok} | Failed: {fail}")
        except: await ctx.bot.send_message(chat_id=ADMIN_USER_ID, text=f"Done! Sent: {ok} | Failed: {fail}")
        broadcast_state.pop(ADMIN_USER_ID, None)
    elif q.data == "bc_cancel":
        broadcast_state.pop(ADMIN_USER_ID, None); await q.edit_message_text("Cancelled.")

async def ai_chat(messages, chat_name, question):
    ctx = "\n".join(f"[{m['timestamp'][:16]}] {m['username']}: {m['text']}" for m in messages if m["text"])
    r = claude().messages.create(model="claude-opus-4-5", max_tokens=1024,
        messages=[{"role":"user","content":
            f"You are a BD intelligence assistant for Soneium. Chat: {chat_name}.\n\nHistory:\n{ctx}\n\nQuestion: {question}\n\nAnswer concisely based on the chat history."}])
    return r.content[0].text

async def ai_cross(results, question):
    ctx = "\n".join(f"[{m['chat_name']} | {m['timestamp'][:16]}] {m['username']}: {m['text']}" for m in results if m["text"])
    r = claude().messages.create(model="claude-opus-4-5", max_tokens=1024,
        messages=[{"role":"user","content":
            f"You are a BD intelligence assistant for Soneium.\n\nMessages from BD chats:\n{ctx}\n\nQuestion: {question}\n\nAnswer concisely. Note which chat each piece of info came from."}])
    return r.content[0].text

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & ~filters.COMMAND, handle_group_message))
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, bot_added_to_group))
    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("permit",    cmd_permit))
    app.add_handler(CommandHandler("unpermit",  cmd_unpermit))
    app.add_handler(CommandHandler("chats",     cmd_chats))
    app.add_handler(CommandHandler("summary",   cmd_summary))
    app.add_handler(CommandHandler("status",    cmd_status))
    app.add_handler(CommandHandler("query",     cmd_query))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & ~filters.COMMAND,
        lambda u,c: handle_broadcast_steps(u,c)
        if broadcast_state.get(ADMIN_USER_ID,{}).get("step") in ("awaiting_message","awaiting_time")
        else handle_private_message(u,c)
    ))
    app.add_handler(CallbackQueryHandler(handle_broadcast_cb, pattern="^bc"))
    log.info("Soneium BD Bot running...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
