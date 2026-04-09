import { createClient } from '@libsql/client';
import nodemailer from 'nodemailer';
import { v4 as uuid } from 'uuid';

const db = createClient({
  url: process.env.TURSO_URL,
  authToken: process.env.TURSO_TOKEN,
});

const row2obj = (cols, row) => {
  const obj = {};
  cols.forEach((c, i) => { obj[c] = row[i]; });
  if (obj.abStats && typeof obj.abStats === 'string') { try { obj.abStats = JSON.parse(obj.abStats); } catch {} }
  if (obj.templates && typeof obj.templates === 'string') { try { obj.templates = JSON.parse(obj.templates); } catch {} }
  if (obj.abTest !== undefined) obj.abTest = !!obj.abTest;
  if (obj.randomizeDelay !== undefined) obj.randomizeDelay = !!obj.randomizeDelay;
  return obj;
};
const queryRows = async (sql, args = []) => { const r = await db.execute({ sql, args }); return r.rows.map(row => row2obj(r.columns, row)); };
const getAll = async (table, lim = 500) => queryRows(`SELECT * FROM ${table} ORDER BY createdAt DESC LIMIT ?`, [lim]);
const updateItem = async (table, id, data) => {
  const d = { ...data };
  if (d.abStats && typeof d.abStats === 'object') d.abStats = JSON.stringify(d.abStats);
  if (d.templates && typeof d.templates === 'object') d.templates = JSON.stringify(d.templates);
  const sets = Object.keys(d).map(k => `"${k}" = ?`).join(', ');
  await db.execute({ sql: `UPDATE ${table} SET ${sets} WHERE id = ?`, args: [...Object.values(d), id] });
};
const batchAdd = async (table, rows) => {
  const SIZE = 50;
  for (let i = 0; i < rows.length; i += SIZE) {
    const stmts = rows.slice(i, i + SIZE).map(data => {
      const d = { id: data.id || uuid(), createdAt: new Date().toISOString(), ...data };
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

const personalize = (str, r) => (str || '')
  .replace(/\{\{name\}\}/gi, r.name || '')
  .replace(/\{\{firstname\}\}/gi, (r.name || '').split(' ')[0] || '')
  .replace(/\{\{email\}\}/gi, r.email || '')
  .replace(/\{\{company\}\}/gi, r.company || '')
  .replace(/\{\{title\}\}/gi, r.title || '')
  .replace(/\{\{industry\}\}/gi, r.industry || '')
  .replace(/\{\{pain_point\}\}/gi, r.pain_point || '');

const SV = [s=>s, s=>s+' 🔥', s=>s+' ✨', s=>'👋 '+s, s=>s+' — Limited Time'];
const FS = ['', ' Team', ' HQ', ' Pro', ' Labs'];
const BATCH_SIZE = 50;

const getGmailAccessToken = async (clientId, clientSecret, refreshToken) => {
  const r = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ client_id: clientId, client_secret: clientSecret, refresh_token: refreshToken, grant_type: 'refresh_token' }),
  });
  const d = await r.json();
  if (!d.access_token) throw new Error('Gmail token error: ' + (d.error_description || d.error || 'unknown'));
  return d.access_token;
};

const sendViaGmailApi = async (accessToken, fromEmail, toEmail, subject, htmlBody, fromName) => {
  const boundary = 'b_' + uuid().replace(/-/g, '');
  const raw = [
    'MIME-Version: 1.0',
    `Content-Type: multipart/alternative; boundary="${boundary}"`,
    `From: "${fromName}" <${fromEmail}>`,
    `To: ${toEmail}`,
    `Subject: ${subject}`,
    '',
    `--${boundary}`,
    'Content-Type: text/html; charset=UTF-8',
    '',
    htmlBody,
    `--${boundary}--`,
  ].join('\r\n');
  const encoded = Buffer.from(raw).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
  const res = await fetch('https://gmail.googleapis.com/gmail/v1/users/me/messages/send', {
    method: 'POST',
    headers: { 'Authorization': 'Bearer ' + accessToken, 'Content-Type': 'application/json' },
    body: JSON.stringify({ raw: encoded }),
  });
  const d = await res.json();
  if (d.error) throw new Error(d.error.message || 'Gmail API error');
  return d;
};

export default async function handler() {
  try {
    const now = new Date().toISOString();
    const allCampaigns = await getAll('campaigns', 100);

    for (const c of allCampaigns) {
      if (c.status === 'scheduled' && c.scheduledAt && c.scheduledAt <= now) {
        await updateItem('campaigns', c.id, { status: 'queued' });
      }
    }

    const queued = allCampaigns.find(c => c.status === 'queued');
    if (!queued) return;

    const claim = await db.execute({
      sql: `UPDATE campaigns SET status='running' WHERE id=? AND status='queued' AND "offset"=?`,
      args: [queued.id, queued.offset || 0],
    });
    if (claim.rowsAffected === 0) return;

    const smtpList = await getAll('smtps');
    const gmailList = await getAll('gmail_accounts');

    if (!smtpList.length && !gmailList.length) {
      await updateItem('campaigns', queued.id, { status: 'queued' });
      return;
    }

    const todayStr = new Date().toDateString();
    for (const g of gmailList) {
      if (g.lastReset !== todayStr) {
        await db.execute({ sql: `UPDATE gmail_accounts SET sentToday = 0, lastReset = ? WHERE id = ?`, args: [todayStr, g.id] });
        g.sentToday = 0; g.lastReset = todayStr;
      }
    }
    for (const s of smtpList) {
      if (s.lastReset !== todayStr) {
        await db.execute({ sql: `UPDATE smtps SET sentToday = 0, lastReset = ? WHERE id = ?`, args: [todayStr, s.id] });
        s.sentToday = 0; s.lastReset = todayStr;
      }
    }

    const offset = queued.offset || 0;
    const total = queued.total || 0;

    if (total === 0 || offset >= total) {
      await updateItem('campaigns', queued.id, { status: 'done' });
      return;
    }

    const unsubList = await getAll('unsubscribed', 50000);
    const bouncedList = await getAll('bounced', 50000);
    const unsubEmails = new Set(unsubList.map(u => u.email && u.email.toLowerCase()).filter(Boolean));
    const bouncedEmails = new Set(bouncedList.map(b => b.email && b.email.toLowerCase()).filter(Boolean));

    const rawContacts = await queryRows(
      `SELECT * FROM contacts WHERE listId = ? ORDER BY createdAt ASC LIMIT ? OFFSET ?`,
      [queued.listId, BATCH_SIZE, offset]
    );
    const batch = rawContacts.filter(r => r.email && isValidEmail(r.email) && !unsubEmails.has(r.email.toLowerCase()) && !bouncedEmails.has(r.email.toLowerCase()));

    if (!batch.length) {
      const newOffset = offset + BATCH_SIZE;
      const done = newOffset >= total;
      await updateItem('campaigns', queued.id, { offset: newOffset, status: done ? 'done' : 'queued' });
      return;
    }

    const tplList = Array.isArray(queued.templates) && queued.templates.length > 0 ? queued.templates : [{ subject: queued.subject, html: queued.html }];

    const transporters = smtpList.map(s => ({
      ...s, limit: parseInt(s.dailyLimit) || 999999, used: parseInt(s.sentToday) || 0, type: 'smtp',
      t: nodemailer.createTransport({ host: s.host, port: parseInt(s.port), secure: parseInt(s.port) === 465, auth: { user: s.user, pass: s.pass }, tls: { rejectUnauthorized: false } }),
    }));

    const gmailTransporters = gmailList.map(g => ({
      ...g, limit: parseInt(g.dailyLimit) || 500, used: parseInt(g.sentToday) || 0, type: 'gmail',
    }));

    const allTransporters = [...transporters, ...gmailTransporters];
    const gmailTokenCache = {};
    const smtpSentCounts = {}, gmailSentCounts = {};

    let tIdx = queued.smtpIndex || 0;
    const logs = [], newBounced = [];
    let batchSent = 0, batchFailed = 0;
    const baseUrl = queued.baseUrl || '';
    const abVariant = queued.abTest ? (Math.floor(offset / BATCH_SIZE) % 2) : null;

    for (let i = 0; i < batch.length; i++) {
      const r = batch[i];
      const tpl = tplList[(offset + i) % tplList.length];
      const recipSubject = tpl.subject || queued.subject;
      const recipHtml = tpl.html || queued.html;

      let current = null;
      for (let j = 0; j < allTransporters.length; j++) {
        const c = allTransporters[(tIdx + j) % allTransporters.length];
        if (c.used < c.limit) { current = c; tIdx = (tIdx + j + 1) % allTransporters.length; break; }
      }
      if (!current) {
        batchFailed++;
        logs.push({ id: uuid(), email: r.email, status: 'failed', error: 'All senders at daily limit', ts: new Date().toISOString(), campaignId: queued.id, campaign: queued.name, subject: queued.subject });
        continue;
      }

      const logId = uuid();
      const varFn = abVariant !== null ? SV[abVariant % SV.length] : SV[Math.floor(Math.random() * SV.length)];
      const finalSubject = varFn(personalize(recipSubject, r));
      const finalFromName = (queued.fromName || '') + FS[Math.floor(Math.random() * FS.length)];
      const delayMs = queued.randomizeDelay ? Math.floor(Math.random() * (parseInt(queued.delay) || 200) * 2) : (parseInt(queued.delay) || 0);

      let body = personalize(recipHtml, r);
      if (baseUrl) {
        body = body.replace(/href="(https?:\/\/[^"]+)"/gi, (_, url) =>
          url.includes('/api/app') ? `href="${url}"` :
          `href="${baseUrl}/api/app?action=track&t=click&lid=${logId}&u=${encodeURIComponent(url)}"`
        );
        body += `<div style="text-align:center;font-size:11px;color:#999;padding:20px 0;margin-top:20px;border-top:1px solid #eee">You received this email because you subscribed. &nbsp;<a href="${baseUrl}/api/app?action=unsub&email=${encodeURIComponent(r.email)}" style="color:#999;text-decoration:underline">Unsubscribe</a></div>`;
        body += `<img src="${baseUrl}/api/app?action=track&t=open&lid=${logId}" width="1" height="1" style="display:none" alt="">`;
      }

      try {
        if (delayMs > 0) await new Promise(resolve => setTimeout(resolve, delayMs));

        if (current.type === 'gmail') {
          if (!gmailTokenCache[current.id]) {
            gmailTokenCache[current.id] = await getGmailAccessToken(current.clientId, current.clientSecret, current.refreshToken);
          }
          await sendViaGmailApi(gmailTokenCache[current.id], current.fromEmail, r.email, finalSubject, body, finalFromName);
          gmailSentCounts[current.id] = (gmailSentCounts[current.id] || 0) + 1;
        } else {
          await current.t.sendMail({
            from: `"${finalFromName}" <${current.from}>`, to: r.email, subject: finalSubject, html: body,
            headers: { 'List-Unsubscribe': baseUrl ? `<${baseUrl}/api/app?action=unsub&email=${encodeURIComponent(r.email)}>` : '', 'List-Unsubscribe-Post': 'List-Unsubscribe=One-Click', 'X-Mailer': 'CentMailer-Pro', 'Precedence': 'bulk' },
          });
          smtpSentCounts[current.id] = (smtpSentCounts[current.id] || 0) + 1;
        }

        current.used++;
        batchSent++;
        logs.push({ id: logId, email: r.email, smtp: current.name, smtpId: current.id, status: 'sent', opened: 0, clicked: 0, ts: new Date().toISOString(), campaignId: queued.id, campaign: queued.name, subject: queued.subject, abVariant: abVariant !== null ? abVariant : -1 });
      } catch (err) {
        batchFailed++;
        const hardBounce = (err.responseCode >= 550 && err.responseCode < 560) || /user.*(unknown|not.found|does.not.exist)/i.test(err.message || '');
        if (hardBounce) newBounced.push(r.email.toLowerCase());
        logs.push({ id: uuid(), email: r.email, smtp: current.name, smtpId: current.id, status: 'failed', error: err.message, bounce: hardBounce ? 1 : 0, ts: new Date().toISOString(), campaignId: queued.id, campaign: queued.name, subject: queued.subject });
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
    const abStats = queued.abStats || { 0: { sent: 0, opens: 0 }, 1: { sent: 0, opens: 0 } };
    if (abVariant !== null) { abStats[abVariant] = abStats[abVariant] || { sent: 0, opens: 0 }; abStats[abVariant].sent += batchSent; }

    await updateItem('campaigns', queued.id, { offset: newOffset, smtpIndex: tIdx, sent: (queued.sent || 0) + batchSent, failed: (queued.failed || 0) + batchFailed, status: done ? 'done' : 'queued', abStats });
    if (newBounced.length) await batchAdd('bounced', newBounced.map(email => ({ email, bounceType: 'hard' })));
    if (logs.length) await batchAdd('logs', logs);

  } catch (err) { console.error('Cron error:', err.message); }
}

export const config = { schedule: "*/5 * * * *" };