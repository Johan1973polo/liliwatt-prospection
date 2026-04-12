const express = require('express');
const jwt = require('jsonwebtoken');
const { google } = require('googleapis');
const axios = require('axios');
const path = require('path');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3002;
const JWT_SECRET = process.env.JWT_SECRET || 'liliwatt-prospection-secret-2026';
const SHEETS_MDP_ID = process.env.SHEETS_MDP_ID || '11gVGMBtqMUhPh70yjMgjW-yLDht6fO0KqWJAF53ASXk';
const MASTER_SHEET_ID = process.env.MASTER_SHEET_ID || '';

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ===== GOOGLE AUTH =====
let DRIVE_CREDENTIALS;
try {
  if (process.env.GOOGLE_DRIVE_CREDS_BASE64) {
    DRIVE_CREDENTIALS = JSON.parse(Buffer.from(process.env.GOOGLE_DRIVE_CREDS_BASE64, 'base64').toString());
  }
} catch(e) { console.warn('Drive credentials non disponibles'); }

function getSheetsClient(scopes) {
  const auth = new google.auth.GoogleAuth({ credentials: DRIVE_CREDENTIALS, scopes: scopes || ['https://www.googleapis.com/auth/spreadsheets'] });
  return google.sheets({ version: 'v4', auth });
}

// ===== AUTH =====
const verifyToken = (req, res, next) => {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Non autorisé' });
  try { req.user = jwt.verify(token, JWT_SECRET); next(); }
  catch(e) { return res.status(401).json({ error: 'Token invalide' }); }
};

// Login — vérifie dans Sheets MDP ZOHO
app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const sheets = getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
    const r = await sheets.spreadsheets.values.get({ spreadsheetId: SHEETS_MDP_ID, range: 'A:K' });
    const rows = r.data.values || [];
    for (const row of rows) {
      if ((row[3] || '').toLowerCase() === email.toLowerCase()) {
        const statut = row[10] || 'actif';
        if (statut === 'bloqué' || statut === 'inactif') {
          return res.status(403).json({ error: 'Compte bloqué' });
        }
        if (row[2] === password) {
          const token = jwt.sign({ email, nom: (row[1] || '') + ' ' + (row[0] || ''), role: row[9] || 'vendeur' }, JWT_SECRET, { expiresIn: '24h' });
          return res.json({ success: true, token, user: { email, nom: (row[1] || '') + ' ' + (row[0] || ''), role: row[9] || 'vendeur' } });
        }
        return res.status(401).json({ error: 'Mot de passe incorrect' });
      }
    }
    return res.status(401).json({ error: 'Email non trouvé' });
  } catch(e) { console.error('Login error:', e.message); res.status(500).json({ error: e.message }); }
});

// ===== PROSPECTS =====
// GET /api/mes-prospects — optimisé pour gros sheets
app.get('/api/mes-prospects', verifyToken, async (req, res) => {
  try {
    if (!MASTER_SHEET_ID) return res.status(503).json({ error: 'MASTER_SHEET_ID non configuré' });
    const sheets = getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);

    const isAdmin = req.user.role === 'admin' ||
      req.user.email === 'johan.mallet@liliwatt.fr' ||
      req.user.email === 'kevin.moreau@liliwatt.fr';

    // 1. En-têtes
    const headersRes = await sheets.spreadsheets.values.get({ spreadsheetId: MASTER_SHEET_ID, range: 'A1:BZ1' });
    const headers = headersRes.data.values[0] || [];
    const vendeurCol = headers.findIndex(h => h.toLowerCase().includes('vendeur_attribue'));

    if (isAdmin) {
      // Admin : 200 premières lignes
      const r = await sheets.spreadsheets.values.get({ spreadsheetId: MASTER_SHEET_ID, range: 'A2:BZ201' });
      const rows = r.data.values || [];
      const prospects = rows.map((row, i) => {
        const obj = { _row: i + 2 };
        headers.forEach((h, j) => { obj[h] = row[j] || ''; });
        return obj;
      });
      return res.json({ success: true, prospects, total: prospects.length });
    }

    // Vendeur : lire uniquement la colonne vendeur_attribue
    if (vendeurCol < 0) return res.json({ success: true, prospects: [], total: 0 });

    const colName = colLetter(vendeurCol);
    const vendeurColRes = await sheets.spreadsheets.values.get({ spreadsheetId: MASTER_SHEET_ID, range: `${colName}2:${colName}40000` });
    const vendeurValues = vendeurColRes.data.values || [];

    const rowNumbers = [];
    vendeurValues.forEach((cell, i) => {
      if ((cell[0] || '').toLowerCase().trim() === req.user.email.toLowerCase().trim()) {
        rowNumbers.push(i + 2);
      }
    });

    if (rowNumbers.length === 0) return res.json({ success: true, prospects: [], total: 0 });

    // Lire seulement les lignes attribuées (max 50)
    const targetRows = rowNumbers.slice(0, 50);
    const ranges = targetRows.map(n => `A${n}:BZ${n}`);
    const batchRes = await sheets.spreadsheets.values.batchGet({ spreadsheetId: MASTER_SHEET_ID, ranges });

    const prospects = (batchRes.data.valueRanges || []).map((vr, i) => {
      const row = (vr.values || [[]])[0] || [];
      const obj = { _row: targetRows[i] };
      headers.forEach((h, j) => { obj[h] = row[j] || ''; });
      return obj;
    });

    res.json({ success: true, prospects, total: rowNumbers.length });
  } catch(e) { console.error('Prospects error:', e.message); res.status(500).json({ error: e.message }); }
});

// POST /api/prospect/:row/statut
app.post('/api/prospect/:row/statut', verifyToken, async (req, res) => {
  try {
    const { statut } = req.body;
    const sheets = getSheetsClient();
    // Écrire dans une colonne dédiée (ex: colonne AV = statut appel)
    const r = await sheets.spreadsheets.values.get({ spreadsheetId: MASTER_SHEET_ID, range: 'A1:AZ1' });
    const headers = r.data.values[0] || [];
    let col = headers.indexOf('statut_appel');
    if (col < 0) {
      col = headers.length;
      await sheets.spreadsheets.values.update({ spreadsheetId: MASTER_SHEET_ID, range: colLetter(col) + '1', valueInputOption: 'RAW', requestBody: { values: [['statut_appel']] } });
    }
    await sheets.spreadsheets.values.update({ spreadsheetId: MASTER_SHEET_ID, range: colLetter(col) + req.params.row, valueInputOption: 'RAW', requestBody: { values: [[statut]] } });
    console.log(`📞 Statut ${req.params.row}: ${statut} par ${req.user.email}`);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/prospect/:row/note
app.post('/api/prospect/:row/note', verifyToken, async (req, res) => {
  try {
    const { note } = req.body;
    const sheets = getSheetsClient();
    const r = await sheets.spreadsheets.values.get({ spreadsheetId: MASTER_SHEET_ID, range: 'A1:AZ1' });
    const headers = r.data.values[0] || [];
    let col = headers.indexOf('note_appel');
    if (col < 0) { col = headers.length; await sheets.spreadsheets.values.update({ spreadsheetId: MASTER_SHEET_ID, range: colLetter(col) + '1', valueInputOption: 'RAW', requestBody: { values: [['note_appel']] } }); }
    await sheets.spreadsheets.values.update({ spreadsheetId: MASTER_SHEET_ID, range: colLetter(col) + req.params.row, valueInputOption: 'RAW', requestBody: { values: [[note]] } });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/prospect/:row/rappel
app.post('/api/prospect/:row/rappel', verifyToken, async (req, res) => {
  try {
    const { date_rappel } = req.body;
    const sheets = getSheetsClient();
    const r = await sheets.spreadsheets.values.get({ spreadsheetId: MASTER_SHEET_ID, range: 'A1:AZ1' });
    const headers = r.data.values[0] || [];
    let col = headers.indexOf('date_rappel');
    if (col < 0) { col = headers.length; await sheets.spreadsheets.values.update({ spreadsheetId: MASTER_SHEET_ID, range: colLetter(col) + '1', valueInputOption: 'RAW', requestBody: { values: [['date_rappel']] } }); }
    await sheets.spreadsheets.values.update({ spreadsheetId: MASTER_SHEET_ID, range: colLetter(col) + req.params.row, valueInputOption: 'RAW', requestBody: { values: [[date_rappel]] } });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/prospect/:row/rgpd — envoie mail RGPD
app.post('/api/prospect/:row/rgpd', verifyToken, async (req, res) => {
  try {
    const { email_client, raison_sociale } = req.body;
    if (!email_client) return res.status(400).json({ error: 'Email client requis' });
    const tokenZoho = await getZohoToken();
    const accountId = process.env.ZOHO_ACCOUNT_ID;
    if (!tokenZoho || !accountId) return res.status(503).json({ error: 'Zoho non configuré' });

    const vendeurNom = req.user.nom || req.user.email;
    const html = `<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
<div style="background:linear-gradient(135deg,#1e1b4b,#7c3aed);padding:32px;border-radius:12px 12px 0 0;text-align:center;">
<h1 style="color:#fff;font-size:28px;font-weight:800;letter-spacing:3px;margin:0;">LILIWATT</h1>
<p style="color:rgba(255,255,255,.8);font-size:12px;margin:6px 0 0;">Courtage Énergie B2B & B2C</p>
</div>
<div style="background:#f5f3ff;padding:32px;border-radius:0 0 12px 12px;">
<p style="font-size:16px;color:#1e1b4b;">Bonjour,</p>
<p style="color:#374151;line-height:1.7;">Suite à notre échange, je vous invite à transmettre vos factures d'énergie pour bénéficier d'une étude gratuite et sans engagement.</p>
<p style="color:#374151;line-height:1.7;">En quelques clics, déposez vos documents et nous vous proposerons les meilleures offres du marché.</p>
<div style="text-align:center;margin:24px 0;">
<a href="https://liliwatt-courtier.onrender.com/rgpd/LIEN_VENDEUR" style="display:inline-block;background:linear-gradient(135deg,#7c3aed,#d946ef);color:#fff;padding:16px 40px;border-radius:50px;text-decoration:none;font-weight:700;font-size:15px;">Transmettre mes factures</a>
</div>
<p style="color:#6b7280;font-size:13px;">Votre conseiller : ${vendeurNom}</p>
<hr style="border:1px solid #e9d5ff;margin:24px 0;">
<p style="font-size:11px;color:#9ca3af;">LILIWATT — LILISTRAT STRATÉGIE SAS — 59 rue de Ponthieu, Bureau 326 — 75008 Paris</p>
</div></div>`;

    await axios.post(`https://mail.zoho.eu/api/accounts/${accountId}/messages`, {
      fromAddress: 'bo@liliwatt.fr', toAddress: email_client,
      subject: `${vendeurNom} — Votre étude énergie gratuite — ${raison_sociale || ''}`,
      content: html, mailFormat: 'html'
    }, { headers: { 'Authorization': `Zoho-oauthtoken ${tokenZoho}`, 'Content-Type': 'application/json' }, timeout: 15000 });

    console.log(`📧 RGPD envoyé à ${email_client} par ${req.user.email}`);
    res.json({ success: true });
  } catch(e) { console.error('RGPD error:', e.message); res.status(500).json({ error: e.message }); }
});

// ===== HELPERS =====
async function getZohoToken() {
  try {
    const r = await axios.post('https://accounts.zoho.eu/oauth/v2/token', null, {
      params: { refresh_token: process.env.ZOHO_REFRESH_TOKEN, client_id: process.env.ZOHO_CLIENT_ID, client_secret: process.env.ZOHO_CLIENT_SECRET, grant_type: 'refresh_token' },
      timeout: 15000
    });
    return r.data.access_token;
  } catch(e) { return null; }
}

function colLetter(idx) {
  let s = '';
  while (idx >= 0) { s = String.fromCharCode(65 + (idx % 26)) + s; idx = Math.floor(idx / 26) - 1; }
  return s;
}

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

app.listen(port, () => console.log(`🚀 LILIWATT Prospection sur http://localhost:${port}`));
