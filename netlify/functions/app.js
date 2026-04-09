const { createClient } = require('@libsql/client');
const nodemailer = require('nodemailer');
const { v4: uuid } = require('uuid');

const db = createClient({
  url: process.env.TURSO_URL,
  authToken: process.env.TURSO_TOKEN,
});

// --- Connection pool: reuse SMTP transporters across batches ---
const transporterPool = {};
const getTransporter = (s) => {
  if (!transporterPool[s.id]) {
    transporterPool[s.id] = nodemailer.createTransport({
      host: s.host,
      port: parseInt(s.port),
      secure: parseInt(s.port) === 465,
      auth: { user: s.user, pass: s.pass },
      tls: { rejectUnauthorized: false },
      pool: true,
      maxConnections: 3,
      maxMessages: 100,
    });
  }
  return transporterPool[s.id];
};

let tablesReady = false;
async function ensureTables() {
  if (tablesReady) return;
  await db.executeMultiple(`
    CREATE TABLE IF NOT EXISTS smtps (id TEXT PRIMARY KEY, name TEXT, host TEXT, port TEXT, user TEXT, pass TEXT, "from" TEXT, dailyLimit TEXT, sentToday INTEGER DEFAULT 0, lastReset TEXT, createdAt TEXT);
    CREATE TABLE IF NOT EXISTS lists (id TEXT PRIMARY KEY, name TEXT, createdAt TEXT);
    CREATE TABLE IF NOT EXISTS contacts (id TEXT PRIMARY KEY, listId TEXT, email TEXT, name TEXT, company TEXT, title TEXT, industry TEXT, pain_point TEXT, createdAt TEXT);
    CREATE TABLE IF NOT EXISTS campaigns (id TEXT PRIMARY KEY, name TEXT, subject TEXT, fromName TEXT, html TEXT, listId TEXT, delay INTEGER DEFAULT 0, scheduledAt TEXT, status TEXT DEFAULT 'queued', sent INTEGER DEFAULT 0, failed INTEGER DEFAULT 0, skipped INTEGER DEFAULT 0, total INTEGER DEFAULT 0, offset INTEGER DEFAULT 0, smtpIndex INTEGER DEFAULT 0, abTest INTEGER DEFAULT 0, randomizeDelay INTEGER DEFAULT 0, baseUrl TEXT, abStats TEXT, templates TEXT, createdAt TEXT);
    CREATE TABLE IF NOT EXISTS logs (id TEXT PRIMARY KEY, email TEXT, smtp TEXT, smtpId TEXT, status TEXT, opened INTEGER DEFAULT 0, clicked INTEGER DEFAULT 0, openedAt TEXT, clickedAt TEXT, error TEXT, bounce INTEGER DEFAULT 0, bounceType TEXT, ts TEXT, campaignId TEXT, campaign TEXT, subject TEXT, abVariant INTEGER DEFAULT -1, createdAt TEXT);
    CREATE TABLE IF NOT EXISTS unsubscribed (id TEXT PRIMARY KEY, email TEXT, createdAt TEXT);
    CREATE TABLE IF NOT EXISTS bounced (id TEXT PRIMARY KEY, email TEXT, bounceType TEXT, createdAt TEXT);
    CREATE TABLE IF NOT EXISTS gmail_accounts (id TEXT PRIMARY KEY, name TEXT, clientId TEXT, clientSecret TEXT, refreshToken TEXT, fromEmail TEXT, dailyLimit TEXT, sentToday INTEGER DEFAULT 0, lastReset TEXT, createdAt TEXT);
    CREATE INDEX IF NOT EXISTS idx_contacts_listId ON contacts(listId);
    CREATE INDEX IF NOT EXISTS idx_logs_campaignId ON logs(campaignId);
    CREATE INDEX IF NOT EXISTS idx_logs_status ON logs(status);
    CREATE INDEX IF NOT EXISTS idx_logs_opened ON logs(opened);
  `);
  try { await db.execute(`ALTER TABLE campaigns ADD COLUMN templates TEXT`); } catch {}
  tablesReady = true;
}

const row2obj = (cols, row) => {
  const obj = {};
  cols.forEach((c, i) => { obj[c] = row[i]; });
  if (obj.abStats && typeof obj.abStats === 'string') { try { obj.abStats = JSON.parse(obj.abStats); } catch {} }
  if (obj.templates && typeof obj.templates === 'string') { try { obj.templates = JSON.parse(obj.templates); } catch {} }
  if (obj.abTest !== undefined) obj.abTest = !!obj.abTest;
  if (obj.randomizeDelay !== undefined) obj.randomizeDelay = !!obj.randomizeDelay;
  if (obj.opened !== undefined) obj.opened = !!obj.opened;
  if (obj.clicked !== undefined) obj.clicked = !!obj.clicked;
  if (obj.bounce !== undefined) obj.bounce = !!obj.bounce;
  return obj;
};

const queryRows = async (sql, args = []) => {
  const r = await db.execute({ sql, args });
  return r.rows.map(row => row2obj(r.columns, row));
};
const getAll = async (table, lim = 500) => queryRows(`SELECT * FROM ${table} ORDER BY createdAt DESC LIMIT ?`, [lim]);
const getAllCampaigns = async (lim = 500) => queryRows(`SELECT id, name, subject, fromName, listId, delay, scheduledAt, status, sent, failed, skipped, total, "offset", smtpIndex, abTest, randomizeDelay, baseUrl, abStats, createdAt FROM campaigns ORDER BY createdAt DESC LIMIT ?`, [lim]);
const getWhere = async (table, field, val, lim = 10000) => queryRows(`SELECT * FROM ${table} WHERE ${field} = ? LIMIT ?`, [val, lim]);
const getById = async (table, id) => { const rows = await queryRows(`SELECT * FROM ${table} WHERE id = ?`, [id]); return rows[0] || null; };

const addItem = async (table, data) => {
  const id = data.id || uuid();
  const createdAt = new Date().toISOString();
  const allData = { id, createdAt, ...data };
  if (allData.abStats && typeof allData.abStats === 'object') allData.abStats = JSON.stringify(allData.abStats);
  if (allData.templates && typeof allData.templates === 'object') allData.templates = JSON.stringify(allData.templates);
  if (allData.abTest !== undefined) allData.abTest = allData.abTest ? 1 : 0;
  if (allData.randomizeDelay !== undefined) allData.randomizeDelay = allData.randomizeDelay ? 1 : 0;
  const cols = Object.keys(allData);
  const vals = Object.values(allData);
  await db.execute({ sql: `INSERT OR REPLACE INTO ${table} (${cols.map(c => `"${c}"`).join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`, args: vals });
  return id;
};

const updateItem = async (table, id, data) => {
  const d = { ...data };
  if (d.abStats && typeof d.abStats === 'object') d.abStats = JSON.stringify(d.abStats);
  if (d.templates && typeof d.templates === 'object') d.templates = JSON.stringify(d.templates);
  if (d.abTest !== undefined) d.abTest = d.abTest ? 1 : 0;
  if (d.randomizeDelay !== undefined) d.randomizeDelay = d.randomizeDelay ? 1 : 0;
  const sets = Object.keys(d).map(k => `"${k}" = ?`).join(', ');
  await db.execute({ sql: `UPDATE ${table} SET ${sets} WHERE id = ?`, args: [...Object.values(d), id] });
};

const deleteItem = async (table, id) => db.execute({ sql: `DELETE FROM ${table} WHERE id = ?`, args: [id] });
const countAll = async (table) => { const r = await db.execute(`SELECT COUNT(*) as cnt FROM ${table}`); return Number(r.rows[0][0]); };
const countWhere = async (table, field, val) => { const r = await db.execute({ sql: `SELECT COUNT(*) as cnt FROM ${table} WHERE "${field}" = ?`, args: [val] }); return Number(r.rows[0][0]); };

const batchAdd = async (table, rows) => {
  const SIZE = 50;
  for (let i = 0; i < rows.length; i += SIZE) {
    const stmts = rows.slice(i, i + SIZE).map(data => {
      const d = { id: data.id || uuid(), createdAt: new Date().toISOString(), ...data };
      if (d.abStats && typeof d.abStats === 'object') d.abStats = JSON.stringify(d.abStats);
      if (d.templates && typeof d.templates === 'object') d.templates = JSON.stringify(d.templates);
      if (d.abTest !== undefined) d.abTest = d.abTest ? 1 : 0;
      if (d.randomizeDelay !== undefined) d.randomizeDelay = d.randomizeDelay ? 1 : 0;
      if (d.opened !== undefined) d.opened = d.opened ? 1 : 0;
      if (d.clicked !== undefined) d.clicked = d.clicked ? 1 : 0;
      if (d.bounce !== undefined) d.bounce = d.bounce ? 1 : 0;
      const cols = Object.keys(d);
      return { sql: `INSERT OR IGNORE INTO ${table} (${cols.map(c => `"${c}"`).join(', ')}) VALUES (${cols.map(() => '?').join(', ')})`, args: Object.values(d) };
    });
    await db.batch(stmts);
  }
};

const isValidEmail = (e) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test((e || '').trim());

const splitCsvLine = (line) => {
  const cols = [];
  let cur = '', inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') { inQ = !inQ; }
    else if (ch === ',' && !inQ) { cols.push(cur.trim()); cur = ''; }
    else { cur += ch; }
  }
  cols.push(cur.trim());
  return cols;
};

const parseCsv = (text) => {
  const lines = text.trim().split('\n');
  const headers = splitCsvLine(lines[0]).map(h => h.toLowerCase().replace(/"/g, ''));
  const ei = headers.findIndex(h => h.includes('email'));
  const ni = headers.findIndex(h => h.includes('name'));
  const ci = headers.findIndex(h => h.includes('company'));
  const ti = headers.findIndex(h => h.includes('title'));
  const ii = headers.findIndex(h => h.includes('industry'));
  const pi = headers.findIndex(h => h.includes('pain'));
  return lines.slice(1).filter(l => l.trim()).map(line => {
    const cols = splitCsvLine(line).map(c => c.replace(/^"|"$/g, ''));
    return {
      email: (cols[ei >= 0 ? ei : 0] || '').trim().toLowerCase(),
      name: ni >= 0 ? cols[ni] : '',
      company: ci >= 0 ? cols[ci] : '',
      title: ti >= 0 ? cols[ti] : '',
      industry: ii >= 0 ? cols[ii] : '',
      pain_point: pi >= 0 ? cols[pi] : '',
    };
  }).filter(r => r.email && isValidEmail(r.email));
};

const personalize = (str, r) => (str || '')
  .replace(/\{\{name\}\}/gi, r.name || '')
  .replace(/\{\{firstname\}\}/gi, (r.name || '').split(' ')[0] || '')
  .replace(/\{\{email\}\}/gi, r.email || '')
  .replace(/\{\{company\}\}/gi, r.company || '')
  .replace(/\{\{title\}\}/gi, r.title || '')
  .replace(/\{\{industry\}\}/gi, r.industry || '')
  .replace(/\{\{pain_point\}\}/gi, r.pain_point || '');

// SV[0] = plain subject (no mutation) — used for sensitive domains
const SV = [s=>s, s=>s+' 🔥', s=>s+' ✨', s=>'👋 '+s, s=>s+' — Limited Time'];
const FS = ['', ' Team', ' HQ', ' Pro', ' Labs'];

const parseBody = async (event) => {
  try {
    if (event.body) return JSON.parse(event.body);
  } catch {}
  return {};
};

const res = (statusCode, body, headers = {}) => ({
  statusCode,
  headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', ...headers },
  body: typeof body === 'string' ? body : JSON.stringify(body),
});

const getGmailAccessToken = async (clientId, clientSecret, refreshToken) => {
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ client_id: clientId, client_secret: clientSecret, refresh_token: refreshToken, grant_type: 'refresh_token' }),
  });
  const d = await r.json();
  if (!d.access_token) throw new Error('Failed to get access token: ' + (d.error_description || d.error || 'unknown'));
  return d.access_token;
};

const sendViaGmailApi = async (accessToken, fromEmail, toEmail, subject, htmlBody, fromName) => {
  const boundary = 'boundary_' + uuid().replace(/-/g, '');
  const raw = [
    'MIME-Version: 1.0',
    'Content-Type: multipart/alternative; boundary="' + boundary + '"',
    'From: "' + fromName + '" <' + fromEmail + '>',
    'To: ' + toEmail,
    'Subject: ' + subject,
    '',
    '--' + boundary,
    'Content-Type: text/html; charset=UTF-8',
    '',
    htmlBody,
    '--' + boundary + '--',
  ].join('\r\n');
  const encoded = Buffer.from(raw).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  const r = await fetch('https://gmail.googleapis.com/gmail/v1/users/me/messages/send', {
    method: 'POST',
    headers: { 'Authorization': 'Bearer ' + accessToken, 'Content-Type': 'application/json' },
    body: JSON.stringify({ raw: encoded }),
  });
  const d = await r.json();
  if (d.error) throw new Error(d.error.message || 'Gmail API error');
  return d;
};

const resetSmtpIfNewDay = async (smtpList) => {
  const today = new Date().toDateString();
  const toReset = smtpList.filter(s => s.lastReset !== today);
  for (const s of toReset) {
    await db.execute({ sql: `UPDATE smtps SET sentToday = 0, lastReset = ? WHERE id = ?`, args: [today, s.id] });
    s.sentToday = 0;
    s.lastReset = today;
  }
};

exports.handler = async (event) => {
  await ensureTables();

  const params = event.queryStringParameters || {};
  const action = params.action;
  const body = await parseBody(event);
  const proto = event.headers['x-forwarded-proto'] || 'https';
  const host = event.headers.host;
  const baseUrl = proto + '://' + host;

  if (action === 'track') {
    const { t: type, lid, u: url } = params;
    if (type === 'open' && lid) {
      try { await updateItem('logs', lid, { opened: 1, openedAt: new Date().toISOString() }); } catch {}
      const pixel = Buffer.from('R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7', 'base64');
      return { statusCode: 200, headers: { 'Content-Type': 'image/gif', 'Cache-Control': 'no-cache' }, body: pixel.toString('base64'), isBase64Encoded: true };
    }
    if (type === 'click' && lid && url) {
      try { await updateItem('logs', lid, { clicked: 1, clickedAt: new Date().toISOString() }); } catch {}
      return { statusCode: 302, headers: { Location: decodeURIComponent(url) }, body: '' };
    }
    return res(400, { error: 'bad request' });
  }

  if (action === 'unsub') {
    const email = params.email || body.email;
    if (email) { try { await addItem('unsubscribed', { email: email.toLowerCase() }); } catch {} }
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'text/html' },
      body: '<!DOCTYPE html><html><head><title>Unsubscribed</title><style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:Inter,Arial,sans-serif;background:#f8fafc;display:flex;align-items:center;justify-content:center;min-height:100vh}.box{background:#fff;border-radius:12px;padding:60px 50px;text-align:center;box-shadow:0 4px 24px rgba(0,0,0,.06);max-width:440px}h2{font-size:22px;margin-bottom:10px;color:#0f172a;font-weight:700}p{color:#64748b;font-size:14px;line-height:1.7}.check{width:64px;height:64px;background:#ecfdf5;border-radius:50%;display:flex;align-items:center;justify-content:center;margin:0 auto 24px;font-size:28px}</style></head><body><div class="box"><div class="check">✓</div><h2>Successfully Unsubscribed</h2><p>You have been removed from this mailing list.</p></div></body></html>'
    };
  }

  if (action === 'getSmtps') return res(200, await getAll('smtps'));
  if (action === 'addSmtp') {
    const { name, host, port, user, pass, from, dailyLimit } = body;
    const id = await addItem('smtps', { name, host, port, user, pass, from, dailyLimit: dailyLimit || '', sentToday: 0, lastReset: new Date().toDateString() });
    return res(200, { id });
  }
  if (action === 'deleteSmtp') {
    // Remove from pool so it gets recreated fresh if re-added
    if (body.id && transporterPool[body.id]) {
      try { transporterPool[body.id].close(); } catch {}
      delete transporterPool[body.id];
    }
    await deleteItem('smtps', body.id);
    return res(200, { ok: true });
  }

  if (action === 'getGmails') return res(200, await getAll('gmail_accounts'));

  if (action === 'addGmail') {
    const { name, clientId, clientSecret, refreshToken, fromEmail, dailyLimit } = body;
    if (!clientId || !clientSecret || !refreshToken || !fromEmail) return res(400, { error: 'Missing required fields' });
    try {
      await getGmailAccessToken(clientId, clientSecret, refreshToken);
    } catch(e) {
      return res(400, { error: 'Invalid credentials: ' + e.message });
    }
    const id = await addItem('gmail_accounts', { name: name || fromEmail, clientId, clientSecret, refreshToken, fromEmail, dailyLimit: dailyLimit || '', sentToday: 0, lastReset: new Date().toDateString() });
    return res(200, { id });
  }

  if (action === 'deleteGmail') { await deleteItem('gmail_accounts', body.id); return res(200, { ok: true }); }

  if (action === 'updateGmail') {
    const { id, name, fromEmail, dailyLimit } = body;
    if (!id) return res(400, { error: 'Missing id' });
    const upd = { name: name || '', dailyLimit: dailyLimit || '' };
    if (fromEmail) upd.fromEmail = fromEmail;
    await updateItem('gmail_accounts', id, upd);
    return res(200, { ok: true });
  }

  if (action === 'testGmail') {
    const list = await getAll('gmail_accounts');
    const g = list.find(x => x.id === body.id);
    if (!g) return res(404, { ok: false });
    try {
      const accessToken = await getGmailAccessToken(g.clientId, g.clientSecret, g.refreshToken);
      await sendViaGmailApi(accessToken, g.fromEmail, g.fromEmail, 'CentMailer Test', '<p>Gmail API test email - working correctly!</p>', 'CentMailer Test');
      return res(200, { ok: true, message: 'Test email sent to ' + g.fromEmail });
    } catch(e) {
      return res(200, { ok: false, error: e.message });
    }
  }

  if (action === 'updateSmtp') {
    const { id, name, host, port, user, pass, from, dailyLimit } = body;
    if (!id) return res(400, { error: 'Missing id' });
    const update = { name, host, port, user, from, dailyLimit: dailyLimit || '' };
    if (pass && pass !== '••••••••') update.pass = pass;
    // Invalidate pool entry so it reconnects with new credentials
    if (transporterPool[id]) {
      try { transporterPool[id].close(); } catch {}
      delete transporterPool[id];
    }
    await updateItem('smtps', id, update);
    return res(200, { ok: true });
  }

  if (action === 'testSmtp') {
    const list = await getAll('smtps');
    const s = list.find(x => x.id === body.id);
    if (!s) return res(404, { ok: false });
    const t = nodemailer.createTransport({ host: s.host, port: parseInt(s.port), secure: parseInt(s.port) === 465, auth: { user: s.user, pass: s.pass }, tls: { rejectUnauthorized: false } });
    try { await t.verify(); return res(200, { ok: true }); }
    catch (e) { return res(200, { ok: false, error: e.message }); }
  }

  if (action === 'getSmtpHealth') {
    const logs = await queryRows(`SELECT smtpId, smtp, status, error, ts FROM logs WHERE smtpId IS NOT NULL AND smtpId != '' ORDER BY createdAt DESC LIMIT 5000`);
    const health = {};
    logs.forEach(l => {
      if (!l.smtpId) return;
      if (!health[l.smtpId]) health[l.smtpId] = { sent: 0, failed: 0, lastSentAt: null, lastFailedAt: null, lastError: null };
      if (l.status === 'sent') {
        health[l.smtpId].sent++;
        if (!health[l.smtpId].lastSentAt) health[l.smtpId].lastSentAt = l.ts || null;
      }
      if (l.status === 'failed') {
        health[l.smtpId].failed++;
        if (!health[l.smtpId].lastFailedAt) health[l.smtpId].lastFailedAt = l.ts || null;
        if (!health[l.smtpId].lastError && l.error) health[l.smtpId].lastError = l.error;
      }
    });
    return res(200, health);
  }

  if (action === 'getLists') {
    const lists = await getAll('lists');
    const out = await Promise.all(lists.map(async l => ({ ...l, count: await countWhere('contacts', 'listId', l.id) })));
    return res(200, out);
  }

  if (action === 'deleteList') {
    await deleteItem('lists', body.id);
    await db.execute({ sql: 'DELETE FROM contacts WHERE listId = ?', args: [body.id] });
    return res(200, { ok: true });
  }

  if (action === 'uploadCsv') {
    const { csv, listName } = body;
    if (!csv || !listName) return res(400, { error: 'Missing csv or listName' });
    const parsed = parseCsv(csv);
    if (!parsed.length) return res(400, { error: 'No valid emails found in file.' });
    const seen = new Set(); const unique = parsed.filter(r => { if (seen.has(r.email)) return false; seen.add(r.email); return true; });
    const listId = await addItem('lists', { name: listName });
    await batchAdd('contacts', unique.map(r => ({ ...r, listId })));
    return res(200, { count: unique.length, dupes: parsed.length - unique.length, listId });
  }

  if (action === 'addContact') {
    const { listId, email, name, company, title, industry, pain_point } = body;
    if (!email || !isValidEmail(email)) return res(400, { error: 'Invalid email address' });
    const existing = await queryRows('SELECT id FROM contacts WHERE listId = ? AND email = ? LIMIT 1', [listId, email.toLowerCase()]);
    if (existing.length) return res(400, { error: 'Email already exists in this list' });
    const id = await addItem('contacts', { listId, email: email.toLowerCase(), name: name || '', company: company || '', title: title || '', industry: industry || '', pain_point: pain_point || '' });
    return res(200, { id });
  }

  if (action === 'getContacts') return res(200, await getWhere('contacts', 'listId', params.listId));
  if (action === 'getUnsubs') return res(200, await getAll('unsubscribed', 5000));
  if (action === 'getBounced') return res(200, await getAll('bounced', 5000));
  if (action === 'removeUnsub' || action === 'deleteUnsub') { await deleteItem('unsubscribed', body.id); return res(200, { ok: true }); }

  if (action === 'previewHtml') {
    const { html, subject, listId } = body;
    let contact = { name: 'John Doe', email: 'preview@example.com', company: 'Acme Corp', title: 'CEO', industry: 'SaaS', pain_point: 'scaling' };
    if (listId) { const c = await getWhere('contacts', 'listId', listId, 1); if (c.length) contact = c[0]; }
    return res(200, { html: personalize(html, contact), subject: personalize(subject || '', contact) });
  }

  if (action === 'generateAiCopy') {
    const { prompt, tone, industry, pain_point } = body;
    try {
      const groqRes = await fetch('https://api.groq.com/openai/v1/chat/completions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + process.env.GROQ_API_KEY },
        body: JSON.stringify({ model: 'llama3-8b-8192', messages: [
          { role: 'system', content: 'You are an expert B2B email copywriter. Return only valid HTML for the email body.' },
          { role: 'user', content: 'Write a ' + (tone||'professional') + ' cold email for: ' + prompt + '. Industry: ' + (industry||'general') + '. Pain point: ' + (pain_point||'not specified') + '. Under 150 words. Include a CTA.' }
        ], max_tokens: 600 }),
      });
      const groqData = await groqRes.json();
      return res(200, { html: groqData.choices?.[0]?.message?.content || '' });
    } catch (e) { return res(500, { error: 'AI generation failed: ' + e.message }); }
  }

  if (action === 'getCampaigns') {
    const campaigns = await getAllCampaigns(50);
    for (const c of campaigns) {
      if (c.status === 'running') {
        await updateItem('campaigns', c.id, { status: 'queued' });
        c.status = 'queued';
      }
    }
    const stats = {};
    for (const c of campaigns) {
      const rows = await queryRows('SELECT SUM(opened) as opens, SUM(clicked) as clicks FROM logs WHERE campaignId = ?', [c.id]);
      stats[c.id] = { opens: Number(rows[0]?.opens || 0), clicks: Number(rows[0]?.clicks || 0) };
    }
    return res(200, campaigns.map(c => ({ ...c, opens: stats[c.id]?.opens || 0, clicks: stats[c.id]?.clicks || 0 })));
  }

  if (action === 'duplicateCampaign') {
    const orig = await getById('campaigns', body.id);
    if (!orig) return res(404, { error: 'Not found' });
    const { id: _id, createdAt: _ca, ...rest } = orig;
    const newId = await addItem('campaigns', { ...rest, name: rest.name + ' (Copy)', status: 'queued', sent: 0, failed: 0, offset: 0, smtpIndex: 0 });
    return res(200, { id: newId });
  }

  if (action === 'getCampaignStatus') {
    const c = await getById('campaigns', params.id);
    if (!c) return res(404, { error: 'Not found' });
    return res(200, c);
  }

  if (action === 'pauseCampaign') { await updateItem('campaigns', body.id, { status: 'paused' }); return res(200, { ok: true }); }
  if (action === 'resumeCampaign') { await updateItem('campaigns', body.id, { status: 'queued' }); return res(200, { ok: true }); }

  if (action === 'startSend') {
    const { name, subject, fromName, html, listId, delay, abTest, randomizeDelay, templates } = body;
    const smtpList = await getAll('smtps');
    const gmailListCheck = await getAll('gmail_accounts');
    if (!smtpList.length && !gmailListCheck.length) return res(400, { error: 'No SMTP servers or Gmail accounts added' });
    const recipients = await getWhere('contacts', 'listId', listId);
    if (!recipients.length) return res(400, { error: 'No contacts in this list' });
    const unsubList = await getAll('unsubscribed', 50000);
    const bouncedList = await getAll('bounced', 50000);
    const unsubEmails = new Set(unsubList.map(u => u.email && u.email.toLowerCase()).filter(Boolean));
    const bouncedEmails = new Set(bouncedList.map(b => b.email && b.email.toLowerCase()).filter(Boolean));
    const activeRecipients = recipients.filter(r => r.email && isValidEmail(r.email) && !unsubEmails.has(r.email.toLowerCase()) && !bouncedEmails.has(r.email.toLowerCase()));
    const tplList = Array.isArray(templates) && templates.length > 1 ? templates : [{ subject, html }];
    const campaignId = await addItem('campaigns', {
      name, subject, fromName, html, listId,
      delay: parseInt(delay) || 0,
      status: 'queued', sent: 0, failed: 0,
      skipped: recipients.length - activeRecipients.length,
      total: recipients.length,
      offset: 0, smtpIndex: 0,
      abTest: abTest ? 1 : 0,
      randomizeDelay: randomizeDelay ? 1 : 0,
      baseUrl,
      abStats: { 0: { sent: 0, opens: 0 }, 1: { sent: 0, opens: 0 } },
      templates: tplList,
    });
    return res(200, { campaignId, total: recipients.length, skipped: recipients.length - activeRecipients.length });
  }

  if (action === 'scheduleSend') {
    const { name, subject, fromName, html, listId, delay, scheduledAt, abTest, randomizeDelay, templates } = body;
    const smtpList = await getAll('smtps');
    const gmailListCheck2 = await getAll('gmail_accounts');
    if (!smtpList.length && !gmailListCheck2.length) return res(400, { error: 'No SMTP servers or Gmail accounts added' });
    const recipients = await getWhere('contacts', 'listId', listId);
    if (!recipients.length) return res(400, { error: 'No contacts in this list' });
    const unsubList = await getAll('unsubscribed', 50000);
    const bouncedList = await getAll('bounced', 50000);
    const unsubEmails = new Set(unsubList.map(u => u.email && u.email.toLowerCase()).filter(Boolean));
    const bouncedEmails = new Set(bouncedList.map(b => b.email && b.email.toLowerCase()).filter(Boolean));
    const activeRecipients = recipients.filter(r => r.email && isValidEmail(r.email) && !unsubEmails.has(r.email.toLowerCase()) && !bouncedEmails.has(r.email.toLowerCase()));
    const tplList = Array.isArray(templates) && templates.length > 1 ? templates : [{ subject, html }];
    const id = await addItem('campaigns', {
      name, subject, fromName, html, listId,
      delay: parseInt(delay) || 0, scheduledAt,
      status: 'scheduled', sent: 0, failed: 0,
      skipped: recipients.length - activeRecipients.length,
      total: recipients.length,
      offset: 0, smtpIndex: 0,
      abTest: abTest ? 1 : 0,
      randomizeDelay: randomizeDelay ? 1 : 0,
      baseUrl,
      abStats: { 0: { sent: 0, opens: 0 }, 1: { sent: 0, opens: 0 } },
      templates: tplList,
    });
    return res(200, { id, total: recipients.length, skipped: recipients.length - activeRecipients.length });
  }

  if (action === 'getLogs') return res(200, await getAll('logs', 1000));

  if (action === 'getStats') {
    const [smtps, gmails, contacts, campaigns, sent, failed, unsub, bounced, opens, clicks] = await Promise.all([
      countAll('smtps'), countAll('gmail_accounts'), countAll('contacts'), countAll('campaigns'),
      countWhere('logs', 'status', 'sent'), countWhere('logs', 'status', 'failed'),
      countAll('unsubscribed'), countAll('bounced'),
      countWhere('logs', 'opened', 1), countWhere('logs', 'clicked', 1),
    ]);
    return res(200, { smtps, gmails, totalSenders: smtps + gmails, contacts, campaigns, sent, failed, unsub, bounced, opens, clicks });
  }

  if (action === 'getCampaignAnalytics') {
    const { campaignId } = params;
    const logs = campaignId ? await getWhere('logs', 'campaignId', campaignId) : await getAll('logs', 2000);
    const smtpStats = {}, hourStats = {}, dailyStats = {};
    logs.forEach(l => {
      if (l.smtp) {
        if (!smtpStats[l.smtp]) smtpStats[l.smtp] = { sent: 0, failed: 0, opens: 0, clicks: 0 };
        if (l.status === 'sent') smtpStats[l.smtp].sent++;
        if (l.status === 'failed') smtpStats[l.smtp].failed++;
        if (l.opened) smtpStats[l.smtp].opens++;
        if (l.clicked) smtpStats[l.smtp].clicks++;
      }
      if (l.status === 'sent' && l.ts) {
        const hour = new Date(l.ts).getHours();
        if (!hourStats[hour]) hourStats[hour] = { sent: 0, opens: 0 };
        hourStats[hour].sent++;
        if (l.opened) hourStats[hour].opens++;
        const day = l.ts.split('T')[0];
        if (!dailyStats[day]) dailyStats[day] = { sent: 0, opens: 0, clicks: 0, failed: 0 };
        dailyStats[day].sent++;
        if (l.opened) dailyStats[day].opens++;
        if (l.clicked) dailyStats[day].clicks++;
      }
      if (l.status === 'failed' && l.ts) {
        const day = l.ts.split('T')[0];
        if (dailyStats[day]) dailyStats[day].failed++;
      }
    });
    return res(200, { smtpStats, hourStats, dailyStats });
  }

  if (action === 'getAbStats') {
    const campaign = await getById('campaigns', params.campaignId);
    if (!campaign) return res(404, { error: 'Not found' });
    const logs = await getWhere('logs', 'campaignId', params.campaignId);
    const ab = { 0: { sent: 0, opens: 0, clicks: 0 }, 1: { sent: 0, opens: 0, clicks: 0 } };
    logs.forEach(l => {
      const v = l.abVariant;
      if (v === 0 || v === 1) {
        if (l.status === 'sent') ab[v].sent++;
        if (l.opened) ab[v].opens++;
        if (l.clicked) ab[v].clicks++;
      }
    });
    return res(200, { abStats: ab, campaign });
  }

  if (action === 'warmup') {
    const { smtpIds, emailPairs } = body;
    const smtpList = await getAll('smtps');
    const targets = smtpList.filter(s => smtpIds.includes(s.id));
    const results = [];
    for (const smtp of targets) {
      const t = nodemailer.createTransport({ host: smtp.host, port: parseInt(smtp.port), secure: parseInt(smtp.port) === 465, auth: { user: smtp.user, pass: smtp.pass }, tls: { rejectUnauthorized: false } });
      let sent = 0;
      const warmupCount = 3 + Math.floor(Math.random() * 5);
      for (let i = 0; i < warmupCount; i++) {
        const pair = emailPairs[i % emailPairs.length];
        try { await t.sendMail({ from: '"Warmup" <' + smtp.from + '>', to: pair, subject: 'Re: checking in', html: '<p>Just following up. Hope all is well!</p>' }); sent++; } catch {}
      }
      results.push({ smtp: smtp.name, sent });
    }
    return res(200, { results });
  }

  if (action === 'runCampaign') { await updateItem('campaigns', body.id, { status: 'queued' }); return res(200, { ok: true }); }

  if (action === 'deleteCampaign') {
    await deleteItem('campaigns', body.id);
    await db.execute({ sql: 'DELETE FROM logs WHERE campaignId = ?', args: [body.id] });
    return res(200, { ok: true });
  }

  if (action === 'processBatch') {
    const { campaignId } = body;

    const queued = await getById('campaigns', campaignId);
    if (!queued) return res(404, { error: 'Campaign not found' });
    if (queued.status === 'paused') return res(200, { done: false, sent: queued.sent || 0, failed: queued.failed || 0, paused: true });
    if (queued.status === 'done') return res(200, { done: true, sent: queued.sent || 0, failed: queued.failed || 0 });
    if (queued.status === 'running') { await db.execute({ sql: `UPDATE campaigns SET status='queued' WHERE id=? AND status='running'`, args: [campaignId] }); return res(200, { done: false, sent: queued.sent || 0, failed: queued.failed || 0, busy: true }); }

    const currentOffset = queued.offset || 0;
    const total = queued.total || 0;

    if (total === 0 || currentOffset >= total) {
      await updateItem('campaigns', queued.id, { status: 'done' });
      return res(200, { done: true, sent: queued.sent || 0, failed: queued.failed || 0 });
    }

    const claimResult = await db.execute({
      sql: `UPDATE campaigns SET status='running' WHERE id=? AND status='queued' AND "offset"=?`,
      args: [campaignId, currentOffset],
    });

    if (claimResult.rowsAffected === 0) {
      return res(200, { done: false, sent: queued.sent || 0, failed: queued.failed || 0, busy: true });
    }

    const smtpList = await getAll('smtps');
    const gmailList = await getAll('gmail_accounts');

    if (!smtpList.length && !gmailList.length) {
      await updateItem('campaigns', campaignId, { status: 'queued' });
      return res(400, { error: 'No SMTP servers or Gmail accounts' });
    }

    const todayStr = new Date().toDateString();
    for (const g of gmailList) {
      if (g.lastReset !== todayStr) {
        await db.execute({ sql: `UPDATE gmail_accounts SET sentToday = 0, lastReset = ? WHERE id = ?`, args: [todayStr, g.id] });
        g.sentToday = 0; g.lastReset = todayStr;
      }
    }

    await resetSmtpIfNewDay(smtpList);

    // Increased from 5 — reduces connect/auth overhead per email
    const BATCH_SIZE = 10;
    const offset = currentOffset;

    const unsubList = await getAll('unsubscribed', 50000);
    const bouncedList = await getAll('bounced', 50000);
    const unsubEmails = new Set(unsubList.map(u => u.email && u.email.toLowerCase()).filter(Boolean));
    const bouncedEmails = new Set(bouncedList.map(b => b.email && b.email.toLowerCase()).filter(Boolean));

    const rawContacts = await queryRows(
      `SELECT * FROM contacts WHERE listId = ? ORDER BY createdAt ASC LIMIT ? OFFSET ?`,
      [queued.listId, BATCH_SIZE, offset]
    );
    const batch = rawContacts.filter(r => r.email && isValidEmail(r.email) && !unsubEmails.has(r.email.toLowerCase()) && !bouncedEmails.has(r.email.toLowerCase()));

    const tplList = Array.isArray(queued.templates) && queued.templates.length > 0 ? queued.templates : [{ subject: queued.subject, html: queued.html }];

    // Use pooled transporters instead of creating new ones each batch
    const transporters = smtpList.map(s => ({
      ...s, limit: parseInt(s.dailyLimit) || 999999, used: parseInt(s.sentToday) || 0, type: 'smtp',
      t: getTransporter(s),
      blockCounts: {},
    }));

    const gmailTransporters = gmailList.map(g => ({
      ...g, limit: parseInt(g.dailyLimit) || 500, used: parseInt(g.sentToday) || 0, type: 'gmail',
      blockCounts: {},
    }));
    const allTransporters = [...transporters, ...gmailTransporters];

    const BLOCK_LIMIT = 2;
    const isBlockError = (err) =>
      err.responseCode === 421 || err.responseCode === 450 ||
      err.responseCode === 550 || err.responseCode === 554 ||
      /block|spam|blacklist|reject|denylist|policy violation|too many|not accept/i.test(err.message || '');
    const isDomainSensitive = (domain) =>
      /gmail\.com|yahoo\.com|yahoo\.ca|hotmail\.|outlook\.|live\.|icloud\.com|me\.com|mac\.com|aol\.com|msn\.com/.test(domain);

    let tIdx = queued.smtpIndex || 0;
    const batchLogs = [], newBounced = [];
    let batchSent = 0, batchFailed = 0;
    const abVariant = queued.abTest ? (Math.floor(offset / BATCH_SIZE) % 2) : null;
    const smtpSentCounts = {}, gmailSentCounts = {}, gmailTokenCache = {};

    for (let i = 0; i < batch.length; i++) {
      const r = batch[i];
      const tpl = tplList[(offset + i) % tplList.length];
      const recipSubject = tpl.subject || queued.subject;
      const recipHtml = tpl.html || queued.html;

      const recipDomain = (r.email || '').split('@')[1]?.toLowerCase().trim() || '';
      const domainSensitive = isDomainSensitive(recipDomain);
      let current = null;
      for (let j = 0; j < allTransporters.length; j++) {
        const c = allTransporters[(tIdx + j) % allTransporters.length];
        const domainBlocked = domainSensitive && (c.blockCounts[recipDomain] || 0) >= BLOCK_LIMIT;
        if (c.used < c.limit && !domainBlocked) { current = c; tIdx = (tIdx + j + 1) % allTransporters.length; break; }
      }
      if (!current) {
        batchFailed++;
        batchLogs.push({ id: uuid(), email: r.email, status: 'failed', error: 'All senders at daily limit or blocked for ' + recipDomain, ts: new Date().toISOString(), campaignId: queued.id, campaign: queued.name, subject: queued.subject });
        continue;
      }

      const logId = uuid();
      // Skip emoji/urgency subject mutations for sensitive domains (hotmail, outlook, live, etc.)
      const varFn = (domainSensitive || abVariant !== null) ? SV[0] : SV[Math.floor(Math.random() * SV.length)];
      const finalSubject = varFn(personalize(recipSubject, r));
      const finalFromName = queued.fromName || '';
      const bUrl = queued.baseUrl || baseUrl;

      let emailBody = personalize(recipHtml, r);
      const unsub = bUrl + '/api/app?action=unsub&email=' + encodeURIComponent(r.email);
      const trackClick = bUrl + '/api/app?action=track&t=click&lid=' + logId + '&u=';
      const trackOpen = bUrl + '/api/app?action=track&t=open&lid=' + logId;
      emailBody = emailBody.replace(/href="(https?:\/\/[^"]+)"/gi, function(match, u2) {
        if (u2.includes('/api/app')) return match;
        return 'href="' + trackClick + encodeURIComponent(u2) + '"';
      });
      emailBody += '<div style="text-align:center;font-size:11px;color:#999;padding:20px 0;margin-top:20px;border-top:1px solid #eee">You received this email because you subscribed. &nbsp;<a href="' + unsub + '" style="color:#999;text-decoration:underline">Unsubscribe</a></div>';
      emailBody += '<img src="' + trackOpen + '" width="1" height="1" style="display:none" alt="">';

      const delayMs = queued.randomizeDelay ? Math.floor(Math.random() * (parseInt(queued.delay) || 200) * 2) : (parseInt(queued.delay) || 0);

      try {
        if (delayMs > 0) await new Promise(resolve => setTimeout(resolve, delayMs));
        if (current.type === 'gmail') {
          if (!gmailTokenCache[current.id]) {
            gmailTokenCache[current.id] = await getGmailAccessToken(current.clientId, current.clientSecret, current.refreshToken);
          }
          await sendViaGmailApi(gmailTokenCache[current.id], current.fromEmail, r.email, finalSubject, emailBody, finalFromName);
          gmailSentCounts[current.id] = (gmailSentCounts[current.id] || 0) + 1;
        } else {
          await current.t.sendMail({
            from: '"' + finalFromName + '" <' + current.from + '>',
            to: r.email, subject: finalSubject, html: emailBody,
            headers: {
              'List-Unsubscribe': '<' + unsub + '>',
              'List-Unsubscribe-Post': 'List-Unsubscribe=One-Click',
              // Removed: 'X-Mailer' and 'Precedence: bulk' — both flag as bulk/spam
            },
          });
          smtpSentCounts[current.id] = (smtpSentCounts[current.id] || 0) + 1;
        }
        current.used++;
        batchSent++;
        batchLogs.push({ id: logId, email: r.email, smtp: current.name, smtpId: current.id, status: 'sent', opened: 0, clicked: 0, ts: new Date().toISOString(), campaignId: queued.id, campaign: queued.name, subject: queued.subject, abVariant: abVariant !== null ? abVariant : -1 });
      } catch (err) {
        batchFailed++;
        if (domainSensitive && isBlockError(err)) {
          current.blockCounts[recipDomain] = (current.blockCounts[recipDomain] || 0) + 1;
        }
        const hardBounce = (err.responseCode >= 550 && err.responseCode < 560) || /user.*(unknown|not.found|does.not.exist)/i.test(err.message || '');
        if (hardBounce) newBounced.push(r.email.toLowerCase());
        batchLogs.push({ id: uuid(), email: r.email, smtp: current.name, smtpId: current.id, status: 'failed', error: err.message, bounce: hardBounce ? 1 : 0, ts: new Date().toISOString(), campaignId: queued.id, campaign: queued.name, subject: queued.subject });
      }
    }

    for (const [smtpId, count] of Object.entries(smtpSentCounts)) {
      await db.execute({ sql: `UPDATE smtps SET sentToday = sentToday + ? WHERE id = ?`, args: [count, smtpId] });
    }
    for (const [gmailId, count] of Object.entries(gmailSentCounts)) {
      await db.execute({ sql: `UPDATE gmail_accounts SET sentToday = sentToday + ? WHERE id = ?`, args: [count, gmailId] });
    }

    const newOffset = offset + BATCH_SIZE;
    const done = newOffset >= total;
    const totalSent = (queued.sent || 0) + batchSent;
    const totalFailed = (queued.failed || 0) + batchFailed;
    const abStats = queued.abStats || { 0: { sent: 0, opens: 0 }, 1: { sent: 0, opens: 0 } };
    if (abVariant !== null) { abStats[abVariant] = abStats[abVariant] || { sent: 0, opens: 0 }; abStats[abVariant].sent += batchSent; }

    await updateItem('campaigns', queued.id, { offset: newOffset, smtpIndex: tIdx, sent: totalSent, failed: totalFailed, status: done ? 'done' : 'queued', abStats });
    if (newBounced.length) await batchAdd('bounced', newBounced.map(email => ({ email, bounceType: 'hard' })));
    if (batchLogs.length) await batchAdd('logs', batchLogs);

    return res(200, { done, sent: totalSent, failed: totalFailed });
  }

  return res(404, { error: 'Unknown action: ' + action });
};
