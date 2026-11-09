import assert from 'node:assert/strict'
import { mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { apply } from '../index.js'
import { createWorkflowService } from '../src/service.js'

function fixture(t, extra = {}) {
  const root = realpathSync.native(mkdtempSync(join(tmpdir(), 'workflow-workdir-')))
  const workDir = join(root, 'project')
  mkdirSync(workDir)
  const spawned = []
  const released = []
  const svc = createWorkflowService({
    persist: false,
    ...(extra.persist === true ? { dir: join(root, 'store') } : {}),
    spawn: async spec => { spawned.push(spec); return `worker-${spawned.length}` },
    release: async id => { released.push(id) },
    dispatch: async () => ({ ok: true }),
    ...extra,
  })
  const lane = svc.define({
    name: 'Project lane',
    nodes: [{ id: 'plan', name: 'Planner' }, { id: 'code', name: 'Coder' }],
    edges: [{ from: 'plan', to: 'code' }],
  }).workflows.find(item => item.name === 'Project lane')
  t.after(async () => { await svc.dispose(); rmSync(root, { recursive: true, force: true }) })
  const row = value => value.workflows.find(item => item.id === lane.id)
  return { svc, lane, row, root, workDir, spawned, released }
}

test('a named run canonicalizes one directory and passes it to every node Bot', async t => {
  const { svc, row, root, workDir, spawned } = fixture(t)
  const alias = join(root, 'alias')
  symlinkSync(workDir, alias, 'dir')
  const started = await svc.runNamed({ workflow: 'Project lane', task: 'named', brief: 'Implement the change', workDir: alias })
  assert.equal(started.ok, true)
  assert.equal(row(started).run.task.workDir, workDir)
  assert.equal(spawned.length, 2)
  assert.ok(spawned.every(spec => spec.workDir === workDir))
})

test('assign refuses invalid explicit directories without creating or queuing Bots', async t => {
  const { svc, lane, row, root, spawned } = fixture(t)
  const file = join(root, 'file')
  writeFileSync(file, 'plain file')
  for (const workDir of [null, '', 'relative', file, join(root, 'missing')]) {
    const answer = await svc.assign({ workflowId: lane.id, taskId: 'invalid', title: 'Invalid directory', workDir })
    assert.equal(answer.ok, false)
    assert.match(answer.error, /workDir/)
  }
  assert.equal(spawned.length, 0)
  assert.equal(row(svc.state()).queueDepth, 0)
  assert.equal(row(svc.state()).activeCount, 0)
})

test('a FIFO task keeps its directory through disk persistence and starts there later', async t => {
  const { svc, lane, row, root, workDir, spawned } = fixture(t, { persist: true })
  const store = join(root, 'store')
  await svc.setConcurrency({ workflowId: lane.id, maxConcurrent: 1 })
  const first = row(await svc.runNamed({ workflow: lane.id, task: 'first', brief: 'First' })).run
  const parked = row(await svc.assign({ workflowId: lane.id, taskId: 'parked', title: 'Second', workDir }))
  assert.equal(parked.queue[0].workDir, workDir)
  assert.ok(spawned.every(spec => !Object.hasOwn(spec, 'workDir')), 'omission keeps the old spawn contract')
  const disk = JSON.parse(readFileSync(join(store, 'workflows.json'), 'utf8'))
  assert.equal(disk.lanes[lane.id].queue[0].workDir, workDir)
  const reloaded = createWorkflowService({ dir: store, persist: false })
  t.after(() => reloaded.dispose())
  assert.equal(row(reloaded.state()).queue[0].workDir, workDir)
  const started = row(await svc.reset({ workflowId: lane.id, runId: first.id }))
  assert.equal(started.run.task.id, 'parked')
  assert.equal(started.run.task.workDir, workDir)
  assert.ok(spawned.slice(2).every(spec => spec.workDir === workDir))
})

test('a directory removed while queued fails before any new Bot is spawned', async t => {
  const { svc, lane, row, workDir, spawned } = fixture(t)
  await svc.setConcurrency({ workflowId: lane.id, maxConcurrent: 1 })
  const first = row(await svc.runNamed({ workflow: lane.id, task: 'first', brief: 'First' })).run
  await svc.runNamed({ workflow: lane.id, task: 'missing-later', brief: 'Queued', workDir })
  rmSync(workDir, { recursive: true })
  const answer = row(await svc.reset({ workflowId: lane.id, runId: first.id }))
  assert.equal(spawned.length, 2)
  assert.equal(answer.activeCount, 0)
  assert.equal(answer.queueDepth, 0)
  assert.ok(answer.messages.some(message => /workDir is not an existing directory/.test(message.text)))
})

test('losing the directory during spawn releases the new Bots without starting a run', async t => {
  const made = []
  let dir
  const { svc, row, workDir, released } = fixture(t, {
    spawn: async spec => { made.push(spec); if (made.length === 2) rmSync(dir, { recursive: true }); return `partial-${made.length}` },
  })
  dir = workDir
  const answer = await svc.runNamed({ workflow: 'Project lane', brief: 'Race', workDir })
  assert.equal(answer.ok, false)
  assert.deepEqual(released, ['partial-1', 'partial-2'])
  assert.equal(row(svc.state()).activeCount, 0)
})

test('the registered run_workflow tool passes workDir through to fb.bot.create', async t => {
  const root = realpathSync.native(mkdtempSync(join(tmpdir(), 'workflow-workdir-tool-')))
  t.after(() => rmSync(root, { recursive: true, force: true }))
  const created = []
  const tools = new Map()
  const fb = {
    bot: {
      list: () => [{ id: 'chief', isMain: true, remote: false }],
      create: async spec => { created.push(spec); return `run-${created.length}` },
      remove: async () => true,
      send: async () => ({ action: 'allow' }),
    },
    readOnly: () => false,
    api: { handle: () => () => {} },
    tools: { register: spec => { tools.set(spec.name, spec); return () => {} } },
    warn() {},
  }
  const started = apply({ flybot: { scope: () => fb }, effect: fn => fn() }, { persist: false })
  t.after(() => started.dispose())
  started.service.define({ name: 'Tool lane', nodes: [{ id: 'work', name: 'Worker' }], edges: [] })
  const run = tools.get('run_workflow')
  assert.equal(run.parameters.workDir.type, 'string')
  const answer = await run.execute({ workflow: 'Tool lane', brief: 'Work', workDir: root }, { botId: 'chief' })
  assert.equal(answer.ok, true)
  assert.equal(created.length, 1)
  assert.equal(created[0].workDir, root)
})


for (const live of [false, true]) {
  test(`a refused queued directory does not strand the next valid task (${live ? 'live' : 'sync'})`, async t => {
    const { svc, lane, row, workDir, spawned } = fixture(t, live ? {} : { spawn: undefined })
    await svc.setConcurrency({ workflowId: lane.id, maxConcurrent: 1 })
    const first = row(await svc.runNamed({ workflow: lane.id, task: 'first', brief: 'First' })).run
    await svc.runNamed({ workflow: lane.id, task: 'missing-later', brief: 'Missing later', workDir })
    await svc.runNamed({ workflow: lane.id, task: 'valid-later', brief: 'Valid later' })
    rmSync(workDir, { recursive: true })
    const answer = row(await svc.reset({ workflowId: lane.id, runId: first.id }))
    assert.equal(answer.activeCount, 1)
    assert.equal(answer.run.task.id, 'valid-later')
    assert.equal(answer.queueDepth, 0)
    assert.equal(spawned.length, live ? 4 : 0)
  })
}


for (const failure of ['directory-lost', 'spawn-failed']) {
  test(`a refused startup retains failed Bot releases for reap (${failure})`, async t => {
    let dir
    let count = 0
    let failRelease = true
    const attempts = []
    const { svc, row, root, workDir } = fixture(t, {
      persist: true,
      spawn: async () => {
        count += 1
        if (count === 2) {
          if (failure === 'spawn-failed') throw new Error('spawn refused')
          rmSync(dir, { recursive: true })
        }
        return `unstarted-${count}`
      },
      release: async id => { attempts.push(id); if (failRelease) throw new Error('remove unavailable') },
    })
    dir = workDir
    const answer = await svc.runNamed({ workflow: 'Project lane', brief: 'Start', workDir })
    assert.equal(answer.ok, false)
    assert.equal(row(svc.state()).activeCount, 0)
    const expected = failure === 'spawn-failed' ? ['unstarted-1'] : ['unstarted-1', 'unstarted-2']
    assert.deepEqual(attempts, expected)
    const file = join(root, 'store', 'workflows.json')
    assert.deepEqual(JSON.parse(readFileSync(file, 'utf8')).orphans, expected)
    failRelease = false
    await svc.reap()
    assert.deepEqual(attempts, [...expected, ...expected])
    assert.equal(JSON.parse(readFileSync(file, 'utf8')).orphans, undefined)
  })
}
