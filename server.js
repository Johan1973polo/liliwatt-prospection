const express = require('express');
const jwt = require('jsonwebtoken');
const { google } = require('googleapis');
const axios = require('axios');
const nodemailer = require('nodemailer');
const path = require('path');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3002;
const JWT_SECRET = process.env.JWT_SECRET || 'liliwatt-prospection-secret-2026';
const SHEETS_MDP_ID = process.env.SHEETS_MDP_ID || '11gVGMBtqMUhPh70yjMgjW-yLDht6fO0KqWJAF53ASXk';
const MASTER_SHEET_ID = process.env.MASTER_SHEET_ID || '1JFEAXFZbdvf40yDWZGVnuEgUN15XdOAx6WgqL69-AMA';

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ===== GOOGLE AUTH =====
let DRIVE_CREDENTIALS;
try {
  if (process.env.GOOGLE_DRIVE_CREDS_BASE64) {
    DRIVE_CREDENTIALS = JSON.parse(Buffer.from(process.env.GOOGLE_DRIVE_CREDS_BASE64, 'base64').toString());
  } else {
    DRIVE_CREDENTIALS = require('/Users/strategyglobal/Desktop/courtier-energie/liliwatt-drive-credentials.json');
  }
} catch(e) { console.warn('Drive credentials non disponibles'); }

function getSheetsClient(scopes) {
  const auth = new google.auth.GoogleAuth({ credentials: DRIVE_CREDENTIALS, scopes: scopes || ['https://www.googleapis.com/auth/spreadsheets'] });
  return google.sheets({ version: 'v4', auth });
}

function colLetter(idx) {
  let s = '';
  while (idx >= 0) { s = String.fromCharCode(65 + (idx % 26)) + s; idx = Math.floor(idx / 26) - 1; }
  return s;
}

// ===== AUTH =====
const verifyToken = (req, res, next) => {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Non autorisé' });
  try { req.user = jwt.verify(token, JWT_SECRET); next(); }
  catch(e) { return res.status(401).json({ error: 'Token invalide' }); }
};

// Login
app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const sheets = getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
    const r = await sheets.spreadsheets.values.get({ spreadsheetId: SHEETS_MDP_ID, range: 'A:K' });
    const rows = r.data.values || [];
    for (const row of rows) {
      if ((row[3] || '').toLowerCase() === email.toLowerCase()) {
        const statut = row[10] || 'actif';
        if (statut === 'bloqué' || statut === 'inactif') return res.status(403).json({ error: 'Compte bloqué' });
        if (row[2] === password) {
          const isAdm = (row[9]||'').toLowerCase().trim() === 'admin' || email === 'johan.mallet@liliwatt.fr' || email === 'kevin.moreau@liliwatt.fr';
          const role = isAdm ? 'admin' : (row[9] || 'vendeur');
          const token = jwt.sign({
            email, nom: (row[1] || '') + ' ' + (row[0] || ''), prenom: row[1] || '', nom_famille: row[0] || '',
            role, drive_folder_id: row[5] || '', token_rgpd: row[7] || '',
            mdp: row[2] || ''
          }, JWT_SECRET, { expiresIn: '24h' });
          return res.json({ success: true, token, user: {
            email, nom: (row[1] || '') + ' ' + (row[0] || ''), prenom: row[1] || '',
            role, token_rgpd: row[7] || ''
          }});
        }
        return res.status(401).json({ error: 'Mot de passe incorrect' });
      }
    }
    return res.status(401).json({ error: 'Email non trouvé' });
  } catch(e) { console.error('Login error:', e.message); res.status(500).json({ error: e.message }); }
});

// ===== HELPERS SHEETS =====
async function getSheetData(sheetName) {
  const sheets = getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
  const r = await sheets.spreadsheets.values.get({ spreadsheetId: MASTER_SHEET_ID, range: `'${sheetName}'!A:Z` });
  return r.data.values || [];
}

async function findColIndex(sheetName, colName) {
  const rows = await getSheetData(sheetName);
  if (!rows.length) return -1;
  return rows[0].findIndex(h => h.toLowerCase().trim() === colName.toLowerCase().trim());
}

async function updateCell(sheetName, row, col, value) {
  const sheets = getSheetsClient();
  await sheets.spreadsheets.values.update({
    spreadsheetId: MASTER_SHEET_ID,
    range: `'${sheetName}'!${colLetter(col)}${row}`,
    valueInputOption: 'RAW',
    requestBody: { values: [[value]] }
  });
}

async function ensureColumn(sheetName, colName) {
  const sheets = getSheetsClient();
  const r = await sheets.spreadsheets.values.get({ spreadsheetId: MASTER_SHEET_ID, range: `'${sheetName}'!1:1` });
  const headers = (r.data.values || [[]])[0];
  let idx = headers.findIndex(h => h.toLowerCase().trim() === colName.toLowerCase().trim());
  if (idx < 0) {
    idx = headers.length;
    await sheets.spreadsheets.values.update({
      spreadsheetId: MASTER_SHEET_ID, range: `'${sheetName}'!${colLetter(idx)}1`,
      valueInputOption: 'RAW', requestBody: { values: [[colName]] }
    });
  }
  return idx;
}

// ===== GET /api/prospects/brute =====
app.get('/api/prospects/brute', verifyToken, async (req, res) => {
  try {
    const rows = await getSheetData('BASE BRUTE');
    if (rows.length < 2) return res.json({ success: true, prospects: [] });
    const headers = rows[0];
    const vendeurCol = headers.findIndex(h => h.toLowerCase().includes('vendeur_attribue'));
    const statutCol = headers.findIndex(h => h.toLowerCase().includes('statut_appel'));
    const secteurCol = headers.findIndex(h => h.toLowerCase().includes('secteur'));
    const villeCol = headers.findIndex(h => h.toLowerCase().includes('ville'));
    const isAdmin = req.user.role === 'admin' || req.user.email === 'johan.mallet@liliwatt.fr' || req.user.email === 'kevin.moreau@liliwatt.fr';
    const fSecteur = (req.query.secteur || '').toLowerCase();
    const fVille = (req.query.ville || '').toLowerCase();
    const fStatut = (req.query.statut || '').toLowerCase();
    const fSearch = (req.query.search || '').toLowerCase();

    const prospects = [];
    for (let i = 1; i < rows.length && prospects.length < 100; i++) {
      const row = rows[i];
      const attr = vendeurCol >= 0 ? (row[vendeurCol] || '').trim() : '';
      // Exclure HORS_POOL (sauf admin avec filtre spécial)
      if (attr === 'HORS_POOL' && !req.query.hors_pool) continue;
      // Vendeur : ses fiches + fiches non attribuées
      if (!isAdmin && attr && attr.toLowerCase() !== req.user.email.toLowerCase()) continue;
      // Filtres
      if (fSecteur && secteurCol >= 0 && !(row[secteurCol] || '').toLowerCase().includes(fSecteur)) continue;
      if (fVille && villeCol >= 0 && !(row[villeCol] || '').toLowerCase().includes(fVille)) continue;
      if (fStatut) {
        const st = statutCol >= 0 ? (row[statutCol] || '') : '';
        if (fStatut === 'non_traite' && st) continue;
        else if (fStatut !== 'non_traite' && st.toLowerCase() !== fStatut) continue;
      }
      if (fSearch) {
        const hay = row.join(' ').toLowerCase();
        if (!hay.includes(fSearch)) continue;
      }
      const obj = { _row: i + 1 };
      headers.forEach((h, j) => { obj[h] = row[j] || ''; });
      obj._attribue = attr.toLowerCase() === req.user.email.toLowerCase() || (isAdmin && !!attr);
      prospects.push(obj);
    }
    // Secteurs et villes uniques pour les dropdowns
    const secteurs = [...new Set(rows.slice(1).map(r => secteurCol >= 0 ? r[secteurCol] || '' : '').filter(Boolean))];
    const villes = [...new Set(rows.slice(1).map(r => villeCol >= 0 ? r[villeCol] || '' : '').filter(Boolean))];
    res.json({ success: true, prospects, secteurs, villes });
  } catch(e) { console.error('Brute error:', e.message); res.status(500).json({ error: e.message }); }
});

// ===== GET /api/prospects/leads =====
app.get('/api/prospects/leads', verifyToken, async (req, res) => {
  try {
    let rows;
    try { rows = await getSheetData('LEADS OHM'); } catch(e) { return res.json({ success: true, prospects: [] }); }
    if (rows.length < 2) return res.json({ success: true, prospects: [] });
    const headers = rows[0];
    const vendeurCol = headers.findIndex(h => h.toLowerCase().includes('vendeur_attribue'));
    const prospects = [];
    for (let i = 1; i < rows.length; i++) {
      const attr = vendeurCol >= 0 ? (row = rows[i], (row[vendeurCol] || '').trim()) : '';
      if (attr.toLowerCase() !== req.user.email.toLowerCase()) continue;
      const row = rows[i];
      const obj = { _row: i + 1, _sheet: 'LEADS OHM', _attribue: true };
      headers.forEach((h, j) => { obj[h] = row[j] || ''; });
      prospects.push(obj);
    }
    res.json({ success: true, prospects });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== POST /api/prospects/prendre/:row =====
app.post('/api/prospects/prendre/:row', verifyToken, async (req, res) => {
  try {
    const sheet = req.body.sheet || 'BASE BRUTE';
    const vendeurCol = await ensureColumn(sheet, 'vendeur_attribue');
    const statutCol = await ensureColumn(sheet, 'statut_appel');
    await updateCell(sheet, req.params.row, vendeurCol, req.user.email);
    await updateCell(sheet, req.params.row, statutCol, 'À appeler');
    console.log(`📌 Fiche ${req.params.row} prise par ${req.user.email}`);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== POST /api/prospects/statut/:row =====
app.post('/api/prospects/statut/:row', verifyToken, async (req, res) => {
  try {
    const { statut, note, date_rappel } = req.body;
    const sheet = req.body.sheet || 'BASE BRUTE';
    const isAdminUser = req.user.role === 'admin' || req.user.email === 'johan.mallet@liliwatt.fr' || req.user.email === 'kevin.moreau@liliwatt.fr';
    const vendeurCol = await ensureColumn(sheet, 'vendeur_attribue');

    // Lire l'attribution actuelle
    const sheets = getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
    const cellRes = await sheets.spreadsheets.values.get({
      spreadsheetId: MASTER_SHEET_ID, range: `'${sheet}'!${colLetter(vendeurCol)}${req.params.row}`
    });
    const currentAttr = ((cellRes.data.values || [[]])[0][0] || '').trim();

    // Vérif : si attribué à un autre vendeur → refuser
    if (currentAttr && currentAttr !== 'HORS_POOL' && currentAttr.toLowerCase() !== req.user.email.toLowerCase() && !isAdminUser) {
      return res.status(403).json({ error: 'Fiche attribuée à un autre vendeur' });
    }

    // Auto-attribution si fiche libre
    if (!currentAttr || currentAttr === '') {
      await updateCell(sheet, req.params.row, vendeurCol, req.user.email);
    }

    // Règles métier par statut
    if (statut === 'Pas intéressé') {
      // Libérer la fiche
      await updateCell(sheet, req.params.row, vendeurCol, '');
    } else if (statut === 'Faux numéro') {
      // Sortir du pool
      await updateCell(sheet, req.params.row, vendeurCol, 'HORS_POOL');
    } else if (statut === 'Client signé') {
      // Reste définitivement au vendeur — ne repart jamais
      if (!currentAttr || currentAttr === '') {
        await updateCell(sheet, req.params.row, vendeurCol, req.user.email);
      }
    }

    if (statut) {
      const col = await ensureColumn(sheet, 'statut_appel');
      await updateCell(sheet, req.params.row, col, statut);
    }
    if (note !== undefined) {
      const col = await ensureColumn(sheet, 'note_appel');
      await updateCell(sheet, req.params.row, col, note);
    }
    if (date_rappel) {
      const col = await ensureColumn(sheet, 'date_rappel');
      await updateCell(sheet, req.params.row, col, date_rappel);
    }
    // Historique
    const histCol = await ensureColumn(sheet, 'date_dernier_appel');
    await updateCell(sheet, req.params.row, histCol, new Date().toLocaleString('fr-FR'));
    console.log(`📞 Fiche ${req.params.row}: ${statut} par ${req.user.email}`);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== POST /api/prospects/mail/:row =====
app.post('/api/prospects/mail/:row', verifyToken, async (req, res) => {
  try {
    const { email_destinataire, nom_gerant } = req.body;
    if (!email_destinataire) return res.status(400).json({ error: 'Email requis' });
    const rgpdLink = `https://liliwatt-courtier.onrender.com/rgpd/${req.user.token_rgpd}`;
    const vendeurNom = `${req.user.prenom} ${req.user.nom_famille}`;

    const html = `<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
<div style="background:linear-gradient(135deg,#1e1b4b,#7c3aed);padding:28px;border-radius:12px 12px 0 0;text-align:center;">
<h1 style="color:#fff;font-size:26px;font-weight:800;letter-spacing:3px;margin:0;">LILIWATT</h1>
<p style="color:rgba(255,255,255,.7);font-size:11px;margin:4px 0 0;text-transform:uppercase;letter-spacing:1px;">Courtage Énergie B2B & B2C</p>
</div>
<div style="background:#f5f3ff;padding:32px;border-radius:0 0 12px 12px;">
<p style="font-size:15px;color:#1e1b4b;">Bonjour${nom_gerant ? ' ' + nom_gerant : ''},</p>
<p style="color:#374151;line-height:1.7;">Suite à notre entretien téléphonique, je me permets de vous transmettre ce lien afin de réaliser votre étude énergétique <strong>gratuite et sans engagement</strong>.</p>
<p style="color:#374151;line-height:1.7;">Merci de bien vouloir nous faire parvenir :</p>
<ul style="color:#374151;line-height:2;">
<li>Une <strong>facture hiver</strong> et une <strong>facture été</strong> d'électricité</li>
<li>Si vous consommez du gaz, une <strong>facture de gaz</strong></li>
</ul>
<div style="text-align:center;margin:28px 0;">
<a href="${rgpdLink}" style="display:inline-block;background:linear-gradient(135deg,#7c3aed,#d946ef);color:#fff;padding:16px 40px;border-radius:50px;text-decoration:none;font-weight:700;font-size:15px;">Transmettre mes factures</a>
</div>
<p style="color:#374151;">Je reste à votre disposition pour tout renseignement.</p>
<p style="color:#374151;">Cordialement,</p>
<div style="margin-top:20px;padding-top:16px;border-top:2px solid #e9d5ff;">
<strong style="color:#1e1b4b;">${vendeurNom}</strong><br>
<span style="color:#7c3aed;font-size:12px;">LILIWATT — Courtage Énergie</span><br>
<span style="font-size:12px;color:#6b7280;">${req.user.email}</span>
</div>
</div></div>`;

    // Envoyer via Zoho SMTP
    console.log(`📧 SMTP user: ${req.user.email} | pass length: ${(req.user.mdp||'').length} | to: ${email_destinataire}`);
    const transporter = nodemailer.createTransport({
      host: 'smtp.zoho.eu', port: 465, secure: true,
      auth: { user: req.user.email, pass: req.user.mdp }
    });
    await transporter.sendMail({
      from: `"${vendeurNom} — LILIWATT" <${req.user.email}>`,
      to: email_destinataire,
      subject: `Suite à notre entretien — Étude énergétique LILIWATT`,
      html
    });

    // Marquer rgpd_envoye dans Sheets
    const sheet = req.body.sheet || 'BASE BRUTE';
    const col = await ensureColumn(sheet, 'rgpd_envoye');
    await updateCell(sheet, req.params.row, col, 'oui');
    const col2 = await ensureColumn(sheet, 'email_envoye_a');
    await updateCell(sheet, req.params.row, col2, email_destinataire);

    console.log(`📧 Mail RGPD envoyé à ${email_destinataire} par ${req.user.email}`);
    res.json({ success: true });
  } catch(e) {
    console.error('Mail error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ===== GET /api/kpis =====
app.get('/api/kpis', verifyToken, async (req, res) => {
  try {
    const rows = await getSheetData('BASE BRUTE');
    if (rows.length < 2) return res.json({ success: true, kpis: { total: 0, appels: 0, interesses: 0, rappels: 0, rgpd: 0 }, historique: [] });
    const headers = rows[0];
    const vendeurCol = headers.findIndex(h => h.toLowerCase().includes('vendeur_attribue'));
    const statutCol = headers.findIndex(h => h.toLowerCase().includes('statut_appel'));
    const rgpdCol = headers.findIndex(h => h.toLowerCase().includes('rgpd_envoye'));
    const dateCol = headers.findIndex(h => h.toLowerCase().includes('date_dernier_appel'));
    const nomCol = headers.findIndex(h => h.toLowerCase().includes('raison_sociale'));

    let total = 0, appels = 0, interesses = 0, rappels = 0, rgpd = 0;
    const historique = [];

    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      if (vendeurCol < 0 || (row[vendeurCol] || '').toLowerCase() !== req.user.email.toLowerCase()) continue;
      total++;
      const st = statutCol >= 0 ? (row[statutCol] || '') : '';
      if (st && st !== 'À appeler') appels++;
      if (st.toLowerCase().includes('intéressé') || st.toLowerCase().includes('interesse')) interesses++;
      if (st.toLowerCase().includes('rappeler')) rappels++;
      if (rgpdCol >= 0 && (row[rgpdCol] || '').toLowerCase() === 'oui') rgpd++;
      if (dateCol >= 0 && row[dateCol]) {
        historique.push({ date: row[dateCol], nom: nomCol >= 0 ? row[nomCol] || '' : '', statut: st, row: i + 1 });
      }
    }

    historique.sort((a, b) => b.date.localeCompare(a.date));
    res.json({ success: true, kpis: { total, appels, interesses, rappels, rgpd }, historique: historique.slice(0, 10) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== ADMIN STATS =====
app.get('/api/admin/stats', verifyToken, async (req, res) => {
  try {
    const rows = await getSheetData('BASE BRUTE');
    if (rows.length < 2) return res.json({ success: true, stats: { total: 0, libres: 0, traitees: 0, hors_pool: 0, par_vendeur: [] } });
    const headers = rows[0];
    const vendeurCol = headers.findIndex(h => h.toLowerCase().includes('vendeur_attribue'));
    const statutCol = headers.findIndex(h => h.toLowerCase().includes('statut_appel'));
    let total = 0, libres = 0, traitees = 0, hors_pool = 0, signes = 0;
    const vendeurs = {};
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      total++;
      const attr = vendeurCol >= 0 ? (row[vendeurCol] || '').trim() : '';
      const st = statutCol >= 0 ? (row[statutCol] || '').trim() : '';
      if (attr === 'HORS_POOL') { hors_pool++; continue; }
      if (!attr) { libres++; continue; }
      if (st && st !== 'À appeler') traitees++;
      if (st === 'Client signé') signes++;
      if (!vendeurs[attr]) vendeurs[attr] = { email: attr, nb_fiches: 0, nb_traitees: 0, nb_signes: 0 };
      vendeurs[attr].nb_fiches++;
      if (st && st !== 'À appeler') vendeurs[attr].nb_traitees++;
      if (st === 'Client signé') vendeurs[attr].nb_signes++;
    }
    // Enrichir noms vendeurs depuis Sheets MDP
    try {
      const mdpSheets = getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
      const mdpR = await mdpSheets.spreadsheets.values.get({ spreadsheetId: SHEETS_MDP_ID, range: 'A:D' });
      const mdpRows = mdpR.data.values || [];
      for (const v of Object.values(vendeurs)) {
        const mdp = mdpRows.find(r => (r[3]||'').toLowerCase() === v.email.toLowerCase());
        if (mdp) v.nom = (mdp[1]||'') + ' ' + (mdp[0]||'');
      }
    } catch(e) {}
    res.json({ success: true, stats: { total, libres, traitees, hors_pool, signes, par_vendeur: Object.values(vendeurs) } });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== ADMIN SCRAPER =====
const isAdminMW = (req, res, next) => {
  const a = req.user.role === 'admin' || req.user.email === 'johan.mallet@liliwatt.fr' || req.user.email === 'kevin.moreau@liliwatt.fr';
  if (!a) return res.status(403).json({ error: 'Admin only' });
  next();
};

app.post('/api/admin/scraper', verifyToken, isAdminMW, async (req, res) => {
  try {
    const { ville, secteur } = req.body;
    if (!ville || !secteur) return res.status(400).json({ error: 'ville et secteur requis' });

    // Vérifier log
    const fs = require('fs');
    const logFile = '/Users/strategyglobal/Desktop/scraping_log.json';
    let log = {};
    try { log = JSON.parse(fs.readFileSync(logFile, 'utf8')); } catch(e) {}
    const key = `${secteur}_${ville.toLowerCase().replace(/\s/g,'_')}`;
    if (log[key]) {
      return res.json({ success: false, deja_fait: true, date: log[key] });
    }

    const { execSync } = require('child_process');
    const script = path.join(__dirname, 'scraper.py');
    const output = execSync(`python3 "${script}" ${secteur} "${ville}"`, { timeout: 120000, env: { ...process.env, PATH: process.env.PATH } }).toString();

    let trouves = 0, nouveaux = 0;
    for (const line of output.split('\n')) {
      const m1 = line.match(/(\d+) trouvés/); if (m1) trouves = parseInt(m1[1]);
      const m2 = line.match(/(\d+) nouvelles/); if (m2) nouveaux = parseInt(m2[1]);
    }
    console.log(`⚡ Scraping ${secteur}/${ville}: ${trouves} trouvés, ${nouveaux} ajoutés`);
    res.json({ success: true, trouves, nouveaux });
  } catch(e) {
    console.error('Scraper error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// Vendeurs list for admin dropdown
app.get('/api/admin/vendeurs-list', verifyToken, isAdminMW, async (req, res) => {
  try {
    const sheets = getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
    const r = await sheets.spreadsheets.values.get({ spreadsheetId: SHEETS_MDP_ID, range: 'A:K' });
    const rows = r.data.values || [];
    const vendeurs = [];
    for (const row of rows) {
      if (row[3] && row[3].includes('@') && (row[10] || 'actif') !== 'inactif') {
        vendeurs.push({ nom: row[0] || '', prenom: row[1] || '', email: row[3], role: row[9] || 'vendeur' });
      }
    }
    res.json({ success: true, vendeurs });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Leads OHM
app.get('/api/admin/leads-ohm', verifyToken, isAdminMW, async (req, res) => {
  try {
    let rows;
    try { rows = await getSheetData('LEADS OHM'); } catch(e) { return res.json({ success: true, prospects: [] }); }
    if (rows.length < 2) return res.json({ success: true, prospects: [] });
    const headers = rows[0];
    const segF = (req.query.segment || '').toUpperCase();
    const anneeF = req.query.annee_fin || '';
    const nonAttr = req.query.non_attribues === 'true';
    const perPage = Math.min(parseInt(req.query.per_page || '20'), 50);

    const g = (row, name) => { const i = headers.indexOf(name); return i >= 0 && i < row.length ? row[i] : ''; };
    const gI = (row, names) => { for (const n of names) { const v = g(row, n); if (v) return v; } return ''; };

    const prospects = [];
    for (let i = 1; i < rows.length && prospects.length < perPage; i++) {
      const row = rows[i];
      if (segF) { const s = (gI(row, ['segments', 'typologie_contrat']) || '').toUpperCase(); if (!s.includes(segF)) continue; }
      if (anneeF && !(g(row, 'date_fin_livraison') || '').includes(anneeF)) continue;
      if (nonAttr && (g(row, 'vendeur_attribue') || '').trim()) continue;
      const obj = { _row: i + 1 };
      headers.forEach((h, j) => { obj[h] = row[j] || ''; });
      prospects.push(obj);
    }
    res.json({ success: true, prospects });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Attribuer leads
app.post('/api/admin/attribuer-leads', verifyToken, isAdminMW, async (req, res) => {
  try {
    const { rows: rowsList, vendeur_email } = req.body;
    if (!rowsList || !vendeur_email) return res.status(400).json({ error: 'rows et vendeur requis' });
    if (rowsList.length > 20) return res.status(400).json({ error: 'Max 20 leads' });

    let headers;
    try { const data = await getSheetData('LEADS OHM'); headers = data[0]; } catch(e) { return res.status(500).json({ error: 'Feuille LEADS OHM introuvable' }); }

    const col = await ensureColumn('LEADS OHM', 'vendeur_attribue');
    for (const row of rowsList) {
      await updateCell('LEADS OHM', row, col, vendeur_email);
    }
    console.log(`⭐ ${rowsList.length} leads attribués à ${vendeur_email}`);
    res.json({ success: true, attribues: rowsList.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.listen(port, () => console.log(`🚀 LILIWATT Prospection sur http://localhost:${port}`));
