import express from 'express';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { config, limits } from './config.mjs';
import { pool, query, initializeDatabase } from './db.mjs';
import { hashPassword, verifyPassword, createToken, requireAuth, requireAdmin } from './auth.mjs';
import { seedDemo } from './initialize.mjs';
import { validateInput } from './validation.mjs';

const app = express();
const authAttempts = new Map();
const connections = new Set();
let server;
let heartbeats;
let previousCpu;
let previousTime;

app.disable('x-powered-by');
app.use(express.json({ limit: '256kb' }));
app.use(setResponseHeaders);

app.get(['/api/health', '/health'], route(health));

app.post('/api/auth/register', authRateLimit, route(registerUser));
app.post('/api/auth/login', authRateLimit, route(loginUser));
app.get('/api/auth/me', requireAuth, getCurrentUser);

app.post('/api/jobs', requireAuth, route(createJob));
app.get('/api/jobs', requireAuth, route(listJobs));
app.get('/api/jobs/:id', requireAuth, route(getJob));
app.post('/api/jobs/:id/cancel', requireAuth, route(cancelJob));
app.get('/api/jobs/:id/events', requireAuth, route(streamJobProgress));
app.get('/api/jobs/:id/download', requireAuth, route(downloadJob));

app.get('/api/admin/metrics', requireAuth, requireAdmin, route(getAdminMetrics));

const frontendDirectory = fileURLToPath(new URL('../frontend', import.meta.url));
app.use(express.static(frontendDirectory, { index: 'index.html' }));
app.use('/api', apiNotFound);
app.get(['/', '/history', '/admin'], serveFrontend);
app.use(handleError);

await startServer();

function setResponseHeaders(req, res, next) {
  res.setHeader('X-Served-By', config.serverId);
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'same-origin');

  if (req.path.startsWith('/api/')) {
    res.setHeader('Cache-Control', 'no-store');
  }

  next();
}

function route(handler) {
  return function handleRequest(req, res, next) {
    return Promise.resolve(handler(req, res, next)).catch(next);
  };
}

function invalid(message, status = 400) {
  const error = new Error(message);
  error.status = status;
  return error;
}

function terminal(status) {
  return ['completed', 'failed', 'cancelled'].includes(status);
}

function publicUser(user) {
  return {
    id: user.id,
    name: user.name,
    email: user.email,
    role: user.role
  };
}

function authRateLimit(req, res, next) {
  const now = Date.now();

  for (const [key, entry] of authAttempts) {
    if (now - entry.start > 60000) {
      authAttempts.delete(key);
    }
  }

  const key = req.ip;
  const entry = authAttempts.get(key) || { start: now, count: 0 };
  entry.count += 1;

  if (entry.count > 40) {
    return res.status(429).json({
      error: 'Забагато спроб. Спробуйте за хвилину.'
    });
  }

  authAttempts.set(key, entry);
  next();
}

async function health(req, res) {
  await query('SELECT 1');

  res.json({
    status: 'ok',
    server: config.serverId,
    database: 'connected',
    timestamp: new Date().toISOString(),
    limits
  });
}

async function registerUser(req, res) {
  const name = typeof req.body.name === 'string' ? req.body.name.trim() : '';
  const email = typeof req.body.email === 'string' ? req.body.email.trim().toLowerCase() : '';
  const password = req.body.password;

  if (name.length < 2 || name.length > 80) {
    throw invalid('Ім’я має містити від 2 до 80 символів.');
  }
  if (email.length > 200 || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    throw invalid('Введіть коректну електронну адресу.');
  }
  if (typeof password !== 'string' || password.length < 8 || password.length > 128) {
    throw invalid('Пароль має містити від 8 до 128 символів.');
  }

  const passwordHash = await hashPassword(password);

  try {
    const result = await query(`
      INSERT INTO users(id, name, email, password_hash)
      VALUES($1, $2, $3, $4)
      RETURNING id, name, email, role
    `, [randomUUID(), name, email, passwordHash]);

    const user = result.rows[0];
    res.status(201).json({ user, token: createToken(user) });
  } catch (error) {
    if (error.code === '23505') {
      throw invalid('Обліковий запис із цією адресою вже існує.', 409);
    }
    throw error;
  }
}

async function loginUser(req, res) {
  const email = typeof req.body.email === 'string' ? req.body.email.trim().toLowerCase() : '';
  const password = req.body.password;

  if (typeof password !== 'string' || password.length > 128 || email.length > 200) {
    throw invalid('Некоректні дані входу.');
  }

  const result = await query('SELECT * FROM users WHERE email=$1', [email]);
  const account = result.rows[0];

  if (!account || !await verifyPassword(password, account.password_hash)) {
    throw invalid('Неправильна адреса або пароль.', 401);
  }

  const user = publicUser(account);
  res.json({ user, token: createToken(user) });
}

function getCurrentUser(req, res) {
  res.json({ user: req.user });
}

async function createJob(req, res) {
  let input;

  try {
    input = validateInput(req.body);
  } catch (error) {
    throw invalid(error.message);
  }

  if (input.tasks.length > config.maxTasks ||
      input.resources.length > config.maxResources ||
      input.iterations > config.maxIterations) {
    throw invalid('Перевищено налаштовані ліміти розрахунку.');
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    await client.query('SELECT id FROM users WHERE id=$1 FOR UPDATE', [req.user.id]);

    const activeJobs = await client.query(`
      SELECT count(*)::int AS count
      FROM jobs
      WHERE user_id=$1 AND status IN ('queued', 'running')
    `, [req.user.id]);

    if (activeJobs.rows[0].count >= config.maxActiveJobs) {
      throw invalid(
        `Максимум ${config.maxActiveJobs} активних розрахунків. Зачекайте або скасуйте один.`,
        429
      );
    }

    const created = await client.query(`
      INSERT INTO jobs(id, user_id, name, input, handled_by)
      VALUES($1, $2, $3, $4, $5)
      RETURNING *
    `, [randomUUID(), req.user.id, input.name, JSON.stringify(input), config.serverId]);

    const job = created.rows[0];
    await client.query("SELECT pg_notify('tempo_jobs',$1)", [job.id]);
    await client.query('COMMIT');

    const jobsWithEstimates = await estimates(created.rows);
    res.status(201).json({ job: jobsWithEstimates[0] });
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

async function listJobs(req, res) {
  const result = await query(`
    SELECT * FROM jobs
    WHERE user_id=$1
    ORDER BY created_at DESC
    LIMIT 50
  `, [req.user.id]);

  const jobs = await estimates(result.rows);
  res.json({ jobs });
}

async function getJob(req, res) {
  const job = await findJob(req);
  const jobsWithEstimates = await estimates([job]);
  res.json({ job: jobsWithEstimates[0] });
}

async function findJob(req) {
  const jobId = req.params.id;
  const validId = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

  if (!validId.test(jobId)) {
    throw invalid('Розрахунок не знайдено.', 404);
  }

  const result = await query('SELECT * FROM jobs WHERE id=$1', [jobId]);
  const job = result.rows[0];

  if (!job) {
    throw invalid('Розрахунок не знайдено.', 404);
  }
  if (job.user_id !== req.user.id && req.user.role !== 'admin') {
    throw invalid('Немає доступу до цього розрахунку.', 403);
  }

  return job;
}

async function cancelJob(req, res) {
  const job = await findJob(req);

  if (terminal(job.status)) {
    throw invalid('Розрахунок уже завершено.', 409);
  }

  const result = await query(`
    UPDATE jobs
    SET status='cancelled',
        cancel_requested=true,
        stage='Скасовано користувачем',
        completed_at=now(),
        execution_ms=CASE
          WHEN started_at IS NULL THEN 0
          ELSE (extract(epoch FROM(now()-started_at))*1000)::int
        END
    WHERE id=$1 AND status IN ('queued', 'running')
    RETURNING *
  `, [job.id]);

  const cancelledJob = result.rows[0];
  if (!cancelledJob) {
    throw invalid('Розрахунок завершився до скасування.', 409);
  }

  res.json({ job: cancelledJob });
}

async function streamJobProgress(req, res) {
  let job = await findJob(req);

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  let closed = false;
  let busy = false;
  let previousSnapshot = '';

  async function sendUpdate() {
    if (closed || busy) return;
    busy = true;

    try {
      const result = await query('SELECT * FROM jobs WHERE id=$1', [job.id]);
      if (!result.rows[0]) return res.end();

      const jobsWithEstimates = await estimates(result.rows);
      job = jobsWithEstimates[0];
      const snapshot = JSON.stringify(job);

      if (snapshot !== previousSnapshot && !closed) {
        res.write(`data: ${snapshot}\n\n`);
        previousSnapshot = snapshot;
      }
      if (terminal(job.status)) {
        res.end();
      }
    } catch {
      if (!closed) res.end();
    } finally {
      busy = false;
    }
  }

  const updates = setInterval(() => { void sendUpdate(); }, 750);
  const keepalive = setInterval(() => {
    if (!closed) res.write(': keepalive\n\n');
  }, 15000);

  res.once('close', () => {
    closed = true;
    clearInterval(updates);
    clearInterval(keepalive);
  });

  await sendUpdate();
}

async function downloadJob(req, res) {
  const job = await findJob(req);

  if (job.status !== 'completed' || !job.result) {
    throw invalid('Результат ще не готовий.', 409);
  }

  const report = {
    name: job.name,
    input: job.input,
    result: job.result,
    worker: job.worker_id,
    executionMs: job.execution_ms
  };

  res.setHeader('Content-Disposition', `attachment; filename="tempo-${job.id}.json"`);
  res.type('json').send(JSON.stringify(report, null, 2));
}

function estimateDuration(input) {
  const iterations = input?.iterations || 100000;
  const taskCount = input?.tasks?.length || 12;
  const estimatedMilliseconds = 1000 + iterations * taskCount ** 2 * 0.000007;
  return Math.min(config.timeoutSeconds * 1000, estimatedMilliseconds);
}

function remainingTime(job) {
  const elapsed = Math.max(0, Date.now() - new Date(job.started_at).getTime());
  const progress = Math.max(0, Math.min(99, job.progress));

  if (progress > 5) {
    return Math.max(1000, elapsed * (100 - progress) / progress);
  }

  return Math.max(1000, estimateDuration(job.input) - elapsed);
}

async function estimates(jobs) {
  if (!jobs.some(job => job.status === 'queued')) return jobs;

  const [queuedResult, workersResult, runningResult] = await Promise.all([
    query("SELECT id,input FROM jobs WHERE status='queued' ORDER BY created_at,id"),
    query("SELECT count(*)::int AS count FROM nodes WHERE kind='worker' AND last_seen>now()-interval '12 seconds'"),
    query("SELECT input,progress,started_at FROM jobs WHERE status='running'")
  ]);

  const capacity = Math.max(1, workersResult.rows[0].count);
  const availability = runningResult.rows.map(remainingTime);

  while (availability.length < capacity) {
    availability.push(0);
  }

  const positions = new Map();

  for (const [index, job] of queuedResult.rows.entries()) {
    const waitMilliseconds = Math.min(...availability);
    const slot = availability.indexOf(waitMilliseconds);
    availability[slot] += estimateDuration(job.input);

    positions.set(job.id, {
      queue_position: index + 1,
      estimated_wait_seconds: Math.ceil(waitMilliseconds / 1000)
    });
  }

  return jobs.map(job => {
    if (job.status !== 'queued') return job;

    const estimate = positions.get(job.id) || {
      queue_position: null,
      estimated_wait_seconds: null
    };

    return { ...job, ...estimate };
  });
}

async function getAdminMetrics(req, res) {
  const [countsResult, nodesResult, averageResult, desiredResult] = await Promise.all([
    query('SELECT status,count(*)::int AS count FROM jobs GROUP BY status'),
    query("SELECT * FROM nodes WHERE last_seen>now()-interval '15 seconds' ORDER BY kind,id"),
    query(`
      SELECT avg(execution_ms)::float AS duration
      FROM (
        SELECT execution_ms FROM jobs
        WHERE status='completed'
        ORDER BY completed_at DESC
        LIMIT 20
      ) recent
    `),
    query("SELECT value FROM settings WHERE key='desired_workers'")
  ]);

  const queue = { queued: 0, running: 0, completed: 0, failed: 0, cancelled: 0 };
  for (const row of countsResult.rows) {
    queue[row.status] = row.count;
  }

  const averageSeconds = Math.round((averageResult.rows[0].duration || 0) / 100) / 10;
  const desiredWorkers = Number(desiredResult.rows[0]?.value || 2);

  res.json({ queue, nodes: nodesResult.rows, averageSeconds, desiredWorkers, limits });
}

function serveFrontend(req, res) {
  const indexFile = fileURLToPath(new URL('../frontend/index.html', import.meta.url));
  res.sendFile(indexFile);
}

function apiNotFound(req, res) {
  res.status(404).json({ error: 'Маршрут не знайдено.' });
}

function handleError(error, req, res, next) {
  if (res.headersSent) return next(error);

  if (error.type === 'entity.too.large') {
    return res.status(413).json({ error: 'Надто великий запит.' });
  }
  if (error instanceof SyntaxError && error.status === 400) {
    return res.status(400).json({ error: 'Некоректний JSON.' });
  }

  const status = error.status || 500;
  if (status >= 500) console.error(error);

  const message = status >= 500
    ? 'Сервер тимчасово недоступний. Спробуйте ще раз.'
    : error.message;

  res.status(status).json({ error: message });
}

async function heartbeat() {
  const now = performance.now();
  const cpu = process.cpuUsage();
  const usedMicroseconds = (cpu.user - previousCpu.user) + (cpu.system - previousCpu.system);
  const elapsedMilliseconds = now - previousTime;
  const percent = usedMicroseconds / (elapsedMilliseconds * 10);

  previousCpu = cpu;
  previousTime = now;

  const cpuPercent = Math.min(100, Math.max(0, percent));
  const memoryMb = process.memoryUsage().rss / 1024 / 1024;

  await query(`
    INSERT INTO nodes(id, kind, last_seen, cpu_percent, memory_mb)
    VALUES($1, 'api', now(), $2, $3)
    ON CONFLICT(id) DO UPDATE
    SET last_seen=now(), cpu_percent=$2, memory_mb=$3
  `, [config.serverId, cpuPercent, memoryMb]);
}

async function startServer() {
  await initializeDatabase();
  await seedDemo();

  previousCpu = process.cpuUsage();
  previousTime = performance.now();
  await heartbeat();

  heartbeats = setInterval(() => {
    heartbeat().catch(error => console.error(error.message));
  }, 2000);

  server = app.listen(config.port, config.host, () => {
    console.log(`${config.serverId} listening on http://${config.host}:${config.port}`);
  });

  server.on('connection', socket => {
    connections.add(socket);
    socket.once('close', () => connections.delete(socket));
  });

  process.once('SIGINT', shutdown);
  process.once('SIGTERM', shutdown);
}

async function shutdown() {
  clearInterval(heartbeats);
  server.close();

  for (const socket of connections) {
    socket.destroy();
  }

  await query('DELETE FROM nodes WHERE id=$1', [config.serverId]).catch(() => {});
  await pool.end();
  process.exit(0);
}
