/**
 * The two chat Sources of this deployment, over one CLI (`bin/tgq`): `telegram` reads the real bot, and `local` reads a file queue that
 * drives the same Router, Policy and Workers before cutover. Their defaults receive nothing: `listen` starts off, because a second
 * getUpdates reader on the bot would take messages from the agent that owns it until cutover.
 */
import type { Context } from '@deepseek-ai/cordis'
import { setTimeout as delay } from 'node:timers/promises'
import { z } from 'zod'
import { key, threadKey, type OutboundMessage, type Trigger } from '../core/index.ts'
import { CliError } from '../infra/commands.ts'
import { KV } from '../kv.ts'
import { health, sourceContext, type SourceContext } from './host.ts'
import type { SourceKind } from './kind.ts'

/** The CLI of both kinds; `channel.send` receives no configuration, so its path is fixed per deployment. */
export const TGQ = process.env.ALEXANDER_TGQ ?? '/opt/stack/alexander-sa/bin/tgq'
const FAILURES_BEFORE_ERROR = 3
export type ChatKindName = 'telegram' | 'local'

const ConfigSchema = z.object({
  listen: z.boolean().default(false),
  pollSeconds: z.number().int().min(1).max(3600).default(1),
}).strict()
export type ChatConfig = z.infer<typeof ConfigSchema>

/** One message as `tgq pull` and `tgq thread` print it. */
const MessageSchema = z.looseObject({
  id: z.string().min(1), chat: z.string().min(1), root: z.string().min(1), reply_to: z.string().optional(),
  sender: z.looseObject({ id: z.string(), bot: z.boolean().default(false) }),
  direct: z.boolean().default(false), voice: z.boolean().default(false), at: z.string(), text: z.string(),
})
export type ChatMessage = z.infer<typeof MessageSchema>
const PageSchema = z.looseObject({ me: z.string(), items: z.array(z.unknown()), next: z.number().int() })
const ThreadPageSchema = z.looseObject({ items: z.array(z.looseObject({
  id: z.string(), sender: z.looseObject({ id: z.string(), bot: z.boolean().default(false) }), at: z.string(), text: z.string(),
  reply_to: z.string().optional() })) })

/** The Trigger of one message; the agent's own messages produce none. */
export function chatTrigger(kind: ChatKindName, message: ChatMessage, me: string): Trigger | undefined {
  if (message.sender.id === me) return undefined
  const thread = `${message.chat}:${message.root}`
  return {
    id: key(kind, `${message.chat}:${message.id}`),
    source: kind,
    threads: [{ source: kind, id: thread }],
    actor: { source: kind, id: message.sender.id },
    human: !message.sender.bot,
    ...(message.direct ? { addressed: 'direct' as const } : {}),
    at: message.at,
    text: message.text,
    channel: { source: kind, thread: `${kind}:${thread}`,
      address: { chat: message.chat, message: message.id, ...(message.voice ? { voice: 'true' } : {}) } },
    meta: { chat: message.chat, messageId: message.id, root: message.root, direct: message.direct, voice: message.voice,
      ...(message.reply_to ? { replyTo: message.reply_to } : {}) },
  }
}

/** Replies once per message id, by `tgq`'s key; a message answering a voice note is also spoken. */
export async function sendChat(kind: ChatKindName, host: Pick<SourceContext, 'commands'>, message: OutboundMessage): Promise<'sent' | 'gone'> {
  const { chat, message: to, voice } = message.channel.address
  if (!chat || !to) throw new Error(`Missing ${kind} reply target`)
  try {
    await host.commands.run(TGQ, ['--mode', kind, 'reply', '--chat', chat, '--to', to, '--key', message.id, '--text', message.text,
      ...(voice && message.kind === 'reply' ? ['--voice'] : [])])
    return 'sent'
  } catch (error) {
    if (error instanceof CliError && error.code === 410) return 'gone'
    throw error
  }
}

/** Read the queue after the cursor, take in each message, then move the cursor past them. */
export async function pollChat(kind: ChatKindName, ctx: SourceContext, signal: AbortSignal): Promise<{ read: number; unreadable: number }> {
  const after = ctx.state.get(KV.chat.cursor(kind)) ?? 0
  const output = await ctx.commands.withSignal(signal).run(TGQ, ['--mode', kind, 'pull', '--after', String(after), '--json'])
  const page = PageSchema.parse(JSON.parse(output))
  let unreadable = 0
  for (const item of page.items) {
    if (signal.aborted) throw new Error(`The ${kind} Source stopped during the read`)
    const message = MessageSchema.safeParse(item)
    if (!message.success) { unreadable++; continue }
    const trigger = chatTrigger(kind, message.data, page.me)
    if (!trigger) continue
    await ctx.receive(trigger)
    for (const ref of trigger.threads) ctx.threads.describe(threadKey(ref), { label: message.data.text.slice(0, 60) })
  }
  ctx.state.set(KV.chat.cursor(kind), page.next)
  return { read: page.items.length, unreadable }
}

/** A chat Source kind; `telegram` and `local` differ only in their name and titles. */
export function chatKind(kind: ChatKindName, title: { zh: string; en: string }, description: { zh: string; en: string }): SourceKind<ChatConfig> {
  return {
    kind, title, description,
    schema: ConfigSchema,
    fields: [
      { key: 'listen', type: 'switch', label: { zh: '接收消息', en: 'Receive messages' } },
      { key: 'pollSeconds', type: 'number', advanced: true, min: 1, max: 3600, label: { zh: '轮询间隔 / 秒', en: 'Polling interval / seconds' } },
    ],
    health: [kind],
    // A chat is a person's DM with the bot; groups and channels are recorded as filtered.
    scope: ({ meta }, config) => !config.listen ? { listened: false, reason: 'not_listening' }
      : meta.direct === true ? { listened: true, observed: false, row: 'direct' } : { listened: false, reason: 'not_direct' },
    find: query => ({ threads: [], terms: [...new Set(query.match(/\b[0-9]{3,}\b/g) ?? [])] }),
    repliesTo: trigger => typeof trigger.meta.replyTo === 'string' ? key(kind, `${String(trigger.meta.chat)}:${trigger.meta.replyTo}`) : undefined,
    channel: {
      allowed: (address, config) => config.listen && Boolean(address.chat),
      send: (host, message) => sendChat(kind, host, message),
      link: () => ({ url: '', label: { zh: `${title.zh} 对话`, en: `${title.en} chat` } }),
    },
    threadContext: {
      messageId: trigger => String(trigger.meta.messageId ?? ''),
      // A reply chain is flat: its root and the parent are on its one page.
      read: async (commands, trigger) => {
        const page = ThreadPageSchema.parse(JSON.parse(await commands.run(TGQ, ['--mode', kind, 'thread', '--chat', String(trigger.meta.chat),
          '--root', String(trigger.meta.root), '--json'])))
        const messages = page.items.map(item => ({ id: item.id, from: item.sender.id, at: item.at, text: item.text,
          ...(item.sender.bot ? { bot: true as const } : {}), ...(item.reply_to ? { reply_to: item.reply_to } : {}) }))
        return { messages, body: { messages } }
      },
    },
    policy: {
      schema: z.object({ allowed: z.array(z.object({ id: z.string().min(1), name: z.string().trim().max(100).optional() }).strict()).default([]) }).strict(),
      fields: [{ key: 'allowed', type: 'rows', label: { zh: '能发起工作的发送者', en: 'Senders who may originate work' }, columns: [
        { key: 'id', type: 'text', label: { zh: '发送者', en: 'Sender' } },
        { key: 'name', type: 'text', label: { zh: '名字', en: 'Name' }, optional: true }] }],
      explicit: { zh: '名单里的人私聊智能体。', en: 'Someone on the list writes to the agent directly.' },
      rows: () => [{ id: 'direct', label: { zh: '私聊', en: 'Direct messages' }, note: { zh: '仅私聊', en: 'Direct only' },
        cells: { listedUnaddressed: false, unlistedAddressed: true, unlistedUnaddressed: false } }],
    },
  }
}

export const telegramKind = chatKind('telegram', { zh: 'Telegram', en: 'Telegram' },
  { zh: '接收机器人私聊里的消息。', en: 'Receives the direct messages of the Telegram bot.' })
export const localKind = chatKind('local', { zh: '本地队列', en: 'Local queue' },
  { zh: '接收本地文件队列里的消息，用于测试。', en: 'Receives the messages of the local file queue, for testing.' })

async function watch(kind: ChatKindName, ctx: SourceContext, signal: AbortSignal): Promise<void> {
  await ctx.ready
  // Setup restarts dsh-superagent once it resolves the accounts; until then the Source reads nothing.
  if (!ctx.configured()) { ctx.report(kind, health('paused', 'setupPending')); return }
  let failures = 0
  while (!signal.aborted) {
    const config = ctx.config<ChatConfig>(kind)
    if (!config?.listen) ctx.report(kind, health('paused', 'sourceOff'))
    else {
      const at = new Date().toISOString()
      try {
        const { read, unreadable } = await pollChat(kind, ctx, signal)
        failures = 0
        ctx.report(kind, { state: 'ready', detail: `${read} messages, ${unreadable} unreadable`, at })
      } catch (error) {
        failures += 1
        // One network failure is retried on the next round; three in a row need a person.
        if (!signal.aborted && failures >= FAILURES_BEFORE_ERROR) {
          ctx.report(kind, health('error', 'pollFailures', { count: String(failures), error: String(error) }))
        }
      }
    }
    await delay((config?.pollSeconds ?? 5) * 1000, undefined, { signal }).catch(() => {})
  }
}

export const name = 'dsh-superagent-chat'
export const inject = ['orchestrator']
export function apply(ctx: Context): void {
  for (const kind of [telegramKind, localKind]) {
    const id = kind.kind as ChatKindName
    ctx.effect(() => ctx.orchestrator.sources.register(kind), `${id}: source kind`)
    ctx.effect(() => {
      const source = sourceContext(ctx.orchestrator, kind)
      const controller = new AbortController()
      // DSH ends its process on an unhandled rejection, so a failure that escapes the loop becomes the Source's health.
      const running = watch(id, source, controller.signal)
        .catch((error: unknown) => { source.report(id, { state: 'error', detail: String(error) }) })
      return async () => { controller.abort(); await running }
    })
  }
}
