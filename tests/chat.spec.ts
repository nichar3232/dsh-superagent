import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { Engine, key, type Trigger } from '../src/core/index.ts'
import { Store } from '../src/app/store.ts'
import { sourceContextOf, type SourceContextDeps } from '../src/app/source-context.ts'
import { Commands, CliError } from '../src/infra/commands.ts'
import { HealthBoard } from '../src/infra/health.ts'
import { KV } from '../src/kv.ts'
import { SettingsSchema, settingsPolicy } from '../src/policy.ts'
import { chatTrigger, localKind, pollChat, sendChat, telegramKind, type ChatMessage } from '../src/sources/chat.ts'
import { SourceRegistry } from '../src/sources/registry.ts'
import { testDeployment } from './fixtures/deployment.ts'

const message = (id: string, fields: Partial<ChatMessage> = {}): ChatMessage => ({ id, chat: '7', root: id, sender: { id: 'nick', bot: false },
  direct: true, voice: false, at: '2026-11-04T00:00:00Z', text: `text ${id}`, ...fields })

function host(listen = true) {
  const store = new Store(join(mkdtempSync(join(tmpdir(), 'chat-')), 'db.sqlite'))
  store.saveSettings(SettingsSchema.parse({ sources: { local: { enabled: true, config: { listen } } },
    policy: { sources: { local: { allowed: [{ id: 'nick' }] } } } }))
  const board = new HealthBoard()
  const sources = new SourceRegistry(() => store.settings(), board)
  sources.register(localKind)
  const engine = new Engine({ repo: store, scope: sources, policy: () => settingsPolicy(store.settings(), sources), assessor: { request: vi.fn() },
    executor: { start: vi.fn(), deliver: vi.fn(), stop: vi.fn() }, outbox: { send: vi.fn(), status: () => undefined, withdraw: vi.fn() } })
  const commands = new Commands(() => testDeployment(), {}, new AbortController().signal)
  const receive = vi.fn((trigger: Trigger) => { const result = engine.receive(trigger); engine.evaluateDue(); return result })
  const deps: SourceContextDeps = { store, engine, outbox: { send: item => { store.receipts.add(item) } }, commands, sources, deployment: testDeployment(),
    ready: Promise.resolve(), configured: () => true, receive }
  return { store, engine, commands, receive, ctx: sourceContextOf(deps, localKind) }
}
function queue(h: ReturnType<typeof host>, answer: () => unknown) {
  vi.spyOn(h.commands, 'withSignal').mockReturnValue(h.commands)
  return vi.spyOn(h.commands, 'run').mockImplementation(async () => JSON.stringify(answer()))
}

describe('chat Sources', () => {
  it('receives nothing by default, so the bot keeps one reader until cutover', () => {
    const config = telegramKind.schema.parse({})
    expect(config.listen).toBe(false)
    expect(telegramKind.scope!({ meta: { direct: true } }, config)).toEqual({ listened: false, reason: 'not_listening' })
    expect(telegramKind.channel!.allowed!({ chat: '7' }, config)).toBe(false)
  })

  it('translates a direct message, and drops the agent\'s own', () => {
    const trigger = chatTrigger('telegram', message('5', { root: '3', reply_to: '4', voice: true }), 'bot')!
    expect(trigger).toMatchObject({ id: key('telegram', '7:5'), source: 'telegram', addressed: 'direct', human: true,
      threads: [{ source: 'telegram', id: '7:3' }], actor: { source: 'telegram', id: 'nick' } })
    expect(trigger.channel?.address).toEqual({ chat: '7', message: '5', voice: 'true' })
    expect(telegramKind.repliesTo!(trigger)).toBe(key('telegram', '7:4'))
    expect(chatTrigger('telegram', message('6', { sender: { id: 'bot', bot: true } }), 'bot')).toBeUndefined()
  })

  it('listens only to direct messages', () => {
    const config = telegramKind.schema.parse({ listen: true })
    expect(telegramKind.scope!({ meta: { direct: true } }, config)).toEqual({ listened: true, observed: false, row: 'direct' })
    expect(telegramKind.scope!({ meta: { direct: false } }, config)).toEqual({ listened: false, reason: 'not_direct' })
  })

  it('takes in what the queue holds once, and moves the cursor after it', async () => {
    const h = host()
    queue(h, () => ({ me: 'bot', items: [message('1'), { id: 'broken' }, message('2', { direct: false })], next: 3 }))
    expect(await pollChat('local', h.ctx, new AbortController().signal)).toEqual({ read: 3, unreadable: 1 })
    expect(h.receive.mock.results[0]!.value).toBe('stored')
    expect(h.receive.mock.results[1]!.value).toBe('filtered')
    expect(h.store.get(KV.chat.cursor('local'))).toBe(3)
    expect(h.engine.candidate(key('local', '7:1'))?.strength).toBe('explicit')
    await pollChat('local', h.ctx, new AbortController().signal)
    expect(h.receive.mock.results[2]!.value).toBe('duplicate')
    h.store.close()
  })

  it('keeps the cursor of a round cut short', async () => {
    const h = host()
    vi.spyOn(h.commands, 'withSignal').mockReturnValue(h.commands)
    vi.spyOn(h.commands, 'run').mockRejectedValue(new Error('down'))
    await expect(pollChat('local', h.ctx, new AbortController().signal)).rejects.toThrow('down')
    expect(h.store.get(KV.chat.cursor('local'))).toBeUndefined()
    h.store.close()
  })

  it('sends once per key, speaks only a reply to a voice note, and is gone only on a 410', async () => {
    const h = host()
    const run = vi.spyOn(h.commands, 'run').mockResolvedValue('{}')
    const out = (kind: 'reply' | 'notice', voice?: string) => ({ id: 'm1', text: 'hi', kind,
      channel: { source: 'local', thread: 'local:7:1', address: { chat: '7', message: '1', ...(voice ? { voice } : {}) } } })
    expect(await sendChat('local', h, out('reply', 'true'))).toBe('sent')
    expect(run.mock.calls[0]![1]).toEqual(['--mode', 'local', 'reply', '--chat', '7', '--to', '1', '--key', 'm1', '--text', 'hi', '--voice'])
    await sendChat('local', h, out('notice', 'true'))
    expect(run.mock.calls[1]![1]).not.toContain('--voice')
    run.mockRejectedValueOnce(new CliError('gone', 410))
    expect(await sendChat('local', h, out('reply'))).toBe('gone')
    run.mockRejectedValueOnce(new CliError('later', 503))
    await expect(sendChat('local', h, out('reply'))).rejects.toThrow('later')
    h.store.close()
  })

  it('offers a direct row where an unlisted sender stays ignorable', () => {
    expect(telegramKind.policy!.rows!({})).toEqual([expect.objectContaining({ id: 'direct',
      cells: { listedUnaddressed: false, unlistedAddressed: true, unlistedUnaddressed: false } })])
  })
})
