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

// Login (Neon + bcrypt)
const { prisma } = require('./lib/db');
const bcrypt = require('bcryptjs');

app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password, mdp: mdpField } = req.body;
    const pwd = password || mdpField;
    if (!email || !pwd) return res.status(400).json({ error: 'Email et mot de passe requis' });

    const user = await prisma.user.findUnique({
      where: { email: email.trim().toLowerCase() },
      include: { credentials: { where: { serviceName: 'RGPD' }, select: { login: true } } }
    });

    if (!user || !user.isActive) return res.status(401).json({ error: 'Identifiants invalides' });
    if (!user.passwordHash) return res.status(401).json({ error: 'Compte non configure' });

    const ok = await bcrypt.compare(pwd, user.passwordHash);
    if (!ok) return res.status(401).json({ error: 'Identifiants invalides' });

    await prisma.user.update({ where: { id: user.id }, data: { lastSeen: new Date() } });

    const isAdm = user.role === 'ADMIN' || user.email === 'johan.mallet@liliwatt.fr' || user.email === 'kevin.moreau@liliwatt.fr';
    const role = isAdm ? 'admin' : (user.role === 'REFERENT' ? 'referent' : 'vendeur');
    const tokenRgpd = user.credentials[0]?.login || '';

    const token = jwt.sign({
      id: user.id, email: user.email,
      prenom: user.firstName || '', nom_famille: user.lastName || '',
      nom: `${user.firstName || ''} ${user.lastName || ''}`.trim(),
      role, token_rgpd: tokenRgpd, referentId: user.referentId
    }, JWT_SECRET, { expiresIn: '24h' });

    console.log('LOGIN:', user.email, '| role:', role);
    res.json({ success: true, token, user: {
      id: user.id, email: user.email,
      prenom: user.firstName || '', nom: `${user.firstName || ''} ${user.lastName || ''}`.trim(),
      role, token_rgpd: tokenRgpd
    }});
  } catch(e) { console.error('Login error:', e.message); res.status(500).json({ error: e.message }); }
});

// ===== HELPERS SHEETS =====
async function getSheetData(sheetName) {
  const sheets = getSheetsClient(['https://www.googleapis.com/auth/spreadsheets.readonly']);
  const r = await sheets.spreadsheets.values.get({ spreadsheetId: MASTER_SHEET_ID, range: `'${sheetName}'!A:ZZ` });
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

// ===== GET /api/prospects/brute (Neon) =====
app.get('/api/prospects/brute', verifyToken, async (req, res) => {
  try {
    const userId = req.user.id;
    const isAdmin = req.user.role === 'admin';
    const fSecteur = (req.query.secteur || '').toLowerCase();
    const fVille = (req.query.ville || '').toLowerCase();
    const fStatut = (req.query.statut || '').toLowerCase();
    const fSearch = (req.query.search || '').toLowerCase();

    // Build where clause
    const where = { source: { in: ['BRUTE', 'MANUELLE'] } };
    if (!isAdmin) {
      where.OR = [{ vendeurId: userId }, { vendeurId: null }];
    }
    if (fSecteur) where.secteur = { contains: fSecteur, mode: 'insensitive' };
    if (fVille) where.ville = { contains: fVille, mode: 'insensitive' };
    if (fStatut === 'non_traite') {
      where.statutAppel = null;
    } else if (fStatut) {
      const statutMap = { 'a appeler':'A_APPELER','appele':'APPELE','interesse':'INTERESSE','attente de documents':'ATTENTE_DOCUMENTS',
        'dossier recu':'DOSSIER_RECU','a rappeler':'A_RAPPELER','pas interesse':'PAS_INTERESSE','faux numero':'FAUX_NUMERO',
        'ne repond pas':'NE_REPOND_PAS','client signe':'CLIENT_SIGNE' };
      where.statutAppel = statutMap[fStatut] || undefined;
    }
    if (fSearch) {
      where.raisonSociale = { contains: fSearch, mode: 'insensitive' };
    }

    const prospects = await prisma.prospect.findMany({
      where,
      orderBy: [{ vendeurId: 'desc' }, { createdAt: 'desc' }],
      take: 100,
    });

    // Map to frontend-compatible format (keep _row as id for backwards compat)
    const mapped = prospects.map(p => ({
      _row: p.id, // frontend uses _row as identifier
      id: p.id,
      place_id: p.placeId,
      raison_sociale: p.raisonSociale,
      adresse: p.adresse || '',
      ville: p.ville || '',
      secteur: p.secteur || '',
      telephone: p.telephone || '',
      site_web: p.siteWeb || '',
      note_google: p.noteGoogle || '',
      nb_avis: p.nbAvis || '',
      vendeur_attribue: p.vendeurId ? req.user.email : '',
      statut_appel: p.statutAppel ? p.statutAppel.replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, c => c.toUpperCase()) : '',
      note_appel: p.noteAppel || '',
      date_rappel: p.dateRappel || '',
      rgpd_envoye: p.rgpdEnvoye ? 'oui' : '',
      email_envoye_a: p.emailEnvoyeA || '',
      date_dernier_appel: p.dateDernierAppel || '',
      _attribue: p.vendeurId === userId || (isAdmin && !!p.vendeurId),
    }));

    // Secteurs et villes uniques for dropdowns
    const allBrute = await prisma.prospect.findMany({
      where: { source: { in: ['BRUTE', 'MANUELLE'] } },
      select: { secteur: true, ville: true },
      distinct: ['secteur', 'ville'],
    });
    const secteurs = [...new Set(allBrute.map(p => p.secteur).filter(Boolean))];
    const villes = [...new Set(allBrute.map(p => p.ville).filter(Boolean))];

    res.json({ success: true, prospects: mapped, secteurs, villes });
  } catch(e) { console.error('Brute error:', e.message); res.status(500).json({ error: e.message }); }
});

// ===== GET /api/prospects/leads (Base Premium) =====
app.get('/api/prospects/leads', verifyToken, async (req, res) => {
  try {
    console.log('💎 LEADS OHM - Sheet ID:', MASTER_SHEET_ID, '| User:', req.user.email);
    let rows;
    try { rows = await getSheetData('LEADS OHM'); } catch(e) { console.error('💎 LEADS OHM read error:', e.message); return res.json({ success: true, prospects: [] }); }
    console.log('💎 LEADS OHM - Nb lignes:', rows.length);
    if (rows.length < 2) return res.json({ success: true, prospects: [] });
    const headers = rows[0];
    console.log('💎 TOUS les headers:', headers.map((h,i) => i+':'+h).join(' | '));
    const vendeurCol = headers.findIndex(h => h.toLowerCase().includes('vendeur_attribue'));
    console.log('💎 vendeur_attribue col index:', vendeurCol, '| Nb colonnes:', headers.length);
    const g = (row, name) => { const i = headers.findIndex(h => h.toLowerCase().includes(name)); return i >= 0 && i < row.length ? row[i] : ''; };
    const prospects = [];
    let totalForUser = 0;
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const attr = vendeurCol >= 0 ? (row[vendeurCol] || '').trim() : '';
      if (attr.toLowerCase() !== req.user.email.toLowerCase()) continue;
      totalForUser++;
      // Debug première fiche
      if (totalForUser === 1) {
        console.log('💎 DEBUG row length:', row.length, '| signataire col:', headers.findIndex(h => h.toLowerCase().includes('signataire')),
          '| score col:', headers.findIndex(h => h.toLowerCase().includes('score')),
          '| date_fin col:', headers.findIndex(h => h.toLowerCase().includes('date_fin')),
          '| segments col:', headers.findIndex(h => h.toLowerCase().includes('segments')));
        console.log('💎 DEBUG values: signataire=', g(row,'signataire'), '| score=', g(row,'score'), '| date_fin=', g(row,'date_fin_livraison'), '| segments=', g(row,'segments'));
      }
      // Vendeur : colonnes essentielles + score + dates + volume + segment
      prospects.push({
        _row: i + 1, _sheet: 'LEADS OHM', _attribue: true,
        raison_sociale: g(row, 'raison_sociale'),
        siren: g(row, 'siren'),
        signataire: g(row, 'signataire'),
        email_signataire: g(row, 'email_signataire'),
        tel_signataire: g(row, 'tel_signataire'),
        adresse: g(row, 'adresse'),
        score: g(row, 'score'),
        pay_rank: g(row, 'pay_rank'),
        observation_pay_rank: g(row, 'observation_pay_rank'),
        date_fin_livraison: g(row, 'date_fin_livraison'),
        volume_total: g(row, 'volume_total'),
        segments: g(row, 'segments') || g(row, 'typologie'),
        energie: g(row, 'energie'),
        statut_appel: g(row, 'statut_appel'),
        note_appel: g(row, 'note_appel'),
        vendeur_attribue: attr
      });
    }
    console.log('💎 Lignes trouvées pour', req.user.email, ':', totalForUser);
    res.json({ success: true, prospects });
  } catch(e) { console.error('💎 LEADS error:', e.message); res.status(500).json({ error: e.message }); }
});

// ===== POST /api/prospects/prendre/:id (Neon) =====
app.post('/api/prospects/prendre/:id', verifyToken, async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.id;

    const prospect = await prisma.prospect.findUnique({
      where: { id },
      select: { id: true, vendeurId: true, raisonSociale: true, isVerrouillee: true }
    });
    if (!prospect) return res.status(404).json({ error: 'Prospect introuvable' });
    if (prospect.isVerrouillee) return res.status(403).json({ error: 'Fiche verrouillee' });
    if (prospect.vendeurId && prospect.vendeurId !== userId) return res.status(409).json({ error: 'Fiche deja attribuee' });

    await prisma.prospect.update({
      where: { id },
      data: { vendeurId: userId, statutAppel: 'A_APPELER' }
    });

    await prisma.activityLog.create({
      data: { userId, prospectId: id, type: 'ATTRIBUTION', metadata: { raisonSociale: prospect.raisonSociale } }
    });

    console.log(`📌 Fiche ${prospect.raisonSociale} prise par ${req.user.email}`);
    res.json({ success: true });
  } catch(e) { console.error('Prendre error:', e); res.status(500).json({ error: e.message }); }
});

// ===== POST /api/prospects/statut/:id (Neon) =====
app.post('/api/prospects/statut/:id', verifyToken, async (req, res) => {
  try {
    const { id } = req.params;
    const { statut, note, date_rappel } = req.body;
    const userId = req.user.id;
    const isAdminUser = req.user.role === 'admin';

    const statutMap = {
      'À appeler':'A_APPELER','A appeler':'A_APPELER','Appelé':'APPELE','Appele':'APPELE',
      'Intéressé':'INTERESSE','Interesse':'INTERESSE','Attente de documents':'ATTENTE_DOCUMENTS',
      'Dossier reçu':'DOSSIER_RECU','Dossier recu':'DOSSIER_RECU','À rappeler':'A_RAPPELER',
      'A rappeler':'A_RAPPELER','Pas intéressé':'PAS_INTERESSE','Pas interesse':'PAS_INTERESSE',
      'Faux numéro':'FAUX_NUMERO','Faux numero':'FAUX_NUMERO',
      'Ne répond pas':'NE_REPOND_PAS','Ne repond pas':'NE_REPOND_PAS',
      'Client signé':'CLIENT_SIGNE','Client signe':'CLIENT_SIGNE'
    };
    const statutEnum = statutMap[statut] || statut;

    const prospect = await prisma.prospect.findUnique({
      where: { id },
      select: { id: true, vendeurId: true, raisonSociale: true, isVerrouillee: true }
    });
    if (!prospect) return res.status(404).json({ error: 'Prospect introuvable' });
    if (prospect.vendeurId && prospect.vendeurId !== userId && !isAdminUser) {
      return res.status(403).json({ error: 'Fiche attribuee a un autre vendeur' });
    }

    const data = {
      statutAppel: statutEnum,
      dateDernierAppel: new Date(),
    };
    if (note !== undefined) data.noteAppel = note;
    if (date_rappel) data.dateRappel = new Date(date_rappel);

    // Auto-attribution si fiche libre
    if (!prospect.vendeurId) data.vendeurId = userId;

    // Regles metier
    if (statutEnum === 'PAS_INTERESSE') {
      if (!prospect.isVerrouillee) data.vendeurId = null;
    } else if (statutEnum === 'FAUX_NUMERO') {
      data.vendeurId = null;
      data.telephone = null;
    }

    await prisma.prospect.update({ where: { id }, data });

    await prisma.activityLog.create({
      data: { userId, prospectId: id, type: 'STATUS_CHANGE', metadata: { newStatut: statutEnum, note: note ? note.substring(0, 100) : undefined } }
    });

    console.log(`📞 ${prospect.raisonSociale}: ${statutEnum} par ${req.user.email}`);
    res.json({ success: true });
  } catch(e) { console.error('Statut error:', e); res.status(500).json({ error: e.message }); }
});

// ===== POST /api/prospects/mail/:id (Neon — RGPD email via Zoho SMTP) =====
app.post('/api/prospects/mail/:id', verifyToken, async (req, res) => {
  try {
    const { email_destinataire, nom_gerant } = req.body;
    if (!email_destinataire) return res.status(400).json({ error: 'Email requis' });

    // Get vendor's Zoho SMTP credentials from Neon
    const vendeurUser = await prisma.user.findUnique({
      where: { id: req.user.id },
      include: { credentials: { where: { serviceName: 'ZOHO' }, select: { login: true, passwordEncrypted: true } } }
    });
    const zohoLogin = vendeurUser?.credentials[0]?.login || req.user.email;
    const zohoPass = vendeurUser?.credentials[0]?.passwordEncrypted || '';
    if (!zohoPass) return res.status(400).json({ error: 'Credentials Zoho non configurees' });

    const rgpdLink = `https://liliwatt-courtier.onrender.com/rgpd/${req.user.token_rgpd}`;
    const vendeurNom = `${req.user.prenom} ${req.user.nom_famille}`;

    const html = `<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
<div style="background:linear-gradient(135deg,#1e1b4b,#7c3aed);padding:28px;border-radius:12px 12px 0 0;text-align:center;">
<h1 style="color:#fff;font-size:26px;font-weight:800;letter-spacing:3px;margin:0;">LILIWATT</h1>
<p style="color:rgba(255,255,255,.7);font-size:11px;margin:4px 0 0;text-transform:uppercase;letter-spacing:1px;">Courtage Energie B2B & B2C</p>
</div>
<div style="background:#f5f3ff;padding:32px;border-radius:0 0 12px 12px;">
<p style="font-size:15px;color:#1e1b4b;">Bonjour${nom_gerant ? ' ' + nom_gerant : ''},</p>
<p style="color:#374151;line-height:1.7;">Suite a notre entretien telephonique, je me permets de vous transmettre ce lien afin de realiser votre etude energetique <strong>gratuite et sans engagement</strong>.</p>
<p style="color:#374151;line-height:1.7;">Merci de bien vouloir nous faire parvenir :</p>
<ul style="color:#374151;line-height:2;">
<li>Une <strong>facture hiver</strong> et une <strong>facture ete</strong> d'electricite</li>
<li>Si vous consommez du gaz, une <strong>facture de gaz</strong></li>
</ul>
<div style="text-align:center;margin:28px 0;">
<a href="${rgpdLink}" style="display:inline-block;background:linear-gradient(135deg,#7c3aed,#d946ef);color:#fff;padding:16px 40px;border-radius:50px;text-decoration:none;font-weight:700;font-size:15px;">Transmettre mes factures</a>
</div>
<p style="color:#374151;">Je reste a votre disposition pour tout renseignement.</p>
<p style="color:#374151;">Cordialement,</p>
<div style="margin-top:20px;padding-top:16px;border-top:2px solid #e9d5ff;">
<strong style="color:#1e1b4b;">${vendeurNom}</strong><br>
<span style="color:#7c3aed;font-size:12px;">LILIWATT — Courtage Energie</span><br>
<span style="font-size:12px;color:#6b7280;">${req.user.email}</span>
</div>
</div></div>`;

    const transporter = nodemailer.createTransport({
      host: 'smtp.zoho.eu', port: 465, secure: true,
      auth: { user: zohoLogin, pass: zohoPass }
    });
    await transporter.sendMail({
      from: `"${vendeurNom} — LILIWATT" <${zohoLogin}>`,
      to: email_destinataire,
      subject: `Suite a notre entretien — Etude energetique LILIWATT`,
      html
    });

    // Marquer rgpd_envoye dans Neon
    await prisma.prospect.update({
      where: { id: req.params.id },
      data: { rgpdEnvoye: true, emailEnvoyeA: email_destinataire }
    });

    await prisma.activityLog.create({
      data: { userId: req.user.id, prospectId: req.params.id, type: 'RGPD_SENT', metadata: { to: email_destinataire } }
    });

    console.log(`📧 Mail RGPD envoye a ${email_destinataire} par ${req.user.email}`);
    res.json({ success: true });
  } catch(e) {
    console.error('Mail error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ===== GET /api/kpis (Neon) =====
app.get('/api/kpis', verifyToken, async (req, res) => {
  try {
    const userId = req.user.id;
    const myProspects = await prisma.prospect.findMany({
      where: { vendeurId: userId, source: { in: ['BRUTE', 'MANUELLE'] } },
      select: { id: true, raisonSociale: true, statutAppel: true, rgpdEnvoye: true, dateDernierAppel: true },
      orderBy: { dateDernierAppel: 'desc' },
    });

    const total = myProspects.length;
    const appels = myProspects.filter(p => p.statutAppel && p.statutAppel !== 'A_APPELER').length;
    const interesses = myProspects.filter(p => p.statutAppel === 'INTERESSE').length;
    const rappels = myProspects.filter(p => p.statutAppel === 'A_RAPPELER').length;
    const rgpd = myProspects.filter(p => p.rgpdEnvoye).length;

    const historique = myProspects
      .filter(p => p.dateDernierAppel)
      .slice(0, 10)
      .map(p => ({
        date: p.dateDernierAppel ? new Date(p.dateDernierAppel).toLocaleString('fr-FR') : '',
        nom: p.raisonSociale,
        statut: p.statutAppel || '',
        row: p.id,
      }));

    res.json({ success: true, kpis: { total, appels, interesses, rappels, rgpd }, historique });
  } catch(e) { console.error('KPIs error:', e); res.status(500).json({ error: e.message }); }
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

// ===== ADMIN MIDDLEWARE =====
const isAdminMW = (req, res, next) => {
  const a = req.user.role === 'admin' || req.user.email === 'johan.mallet@liliwatt.fr' || req.user.email === 'kevin.moreau@liliwatt.fr';
  if (!a) return res.status(403).json({ error: 'Admin only' });
  next();
};

// ===== INIT LEADS OHM — ajouter colonnes manquantes =====
app.get('/api/admin/init-leads-ohm', verifyToken, isAdminMW, async (req, res) => {
  try {
    const sheets = getSheetsClient();
    const r = await sheets.spreadsheets.values.get({ spreadsheetId: MASTER_SHEET_ID, range: "'LEADS OHM'!1:1" });
    const headers = (r.data.values || [[]])[0];
    console.log('📋 LEADS OHM headers actuels:', headers.length, 'colonnes');

    const toAdd = ['vendeur_attribue', 'statut_appel', 'note_appel', 'date_rappel', 'rgpd_envoye', 'date_contact'];
    const added = [];

    for (const col of toAdd) {
      if (!headers.some(h => h.toLowerCase().trim() === col.toLowerCase())) {
        const nextCol = colLetter(headers.length + added.length);
        await sheets.spreadsheets.values.update({
          spreadsheetId: MASTER_SHEET_ID,
          range: `'LEADS OHM'!${nextCol}1`,
          valueInputOption: 'RAW',
          requestBody: { values: [[col]] }
        });
        added.push(col);
        console.log(`  ✅ Colonne ajoutée: ${col} → ${nextCol}`);
      } else {
        console.log(`  ⏭️ Colonne existante: ${col}`);
      }
    }

    console.log('📋 Init terminé:', added.length, 'colonnes ajoutées');
    res.json({ success: true, added, existing: headers.length, total: headers.length + added.length });
  } catch(e) {
    console.error('❌ Init LEADS OHM error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ===== ADMIN SCRAPER =====

app.post('/api/admin/scraper', verifyToken, isAdminMW, async (req, res) => {
  try {
    const { ville, secteur } = req.body;
    if (!ville || !secteur) return res.status(400).json({ error: 'ville et secteur requis' });

    // Vérifier log
    const fs = require('fs');
    const logFile = process.env.SCRAPING_LOG || '/tmp/scraping_log.json';
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

// Leads OHM — pagination + filtres étendus
app.get('/api/admin/leads-ohm', verifyToken, isAdminMW, async (req, res) => {
  try {
    let rows;
    try { rows = await getSheetData('LEADS OHM'); } catch(e) { return res.json({ success: true, prospects: [], total: 0, pages: 0 }); }
    if (rows.length < 2) return res.json({ success: true, prospects: [], total: 0, pages: 0 });
    const headers = rows[0];
    const segF = (req.query.segment || '').toUpperCase();
    const anneeF = req.query.annee_fin || '';
    const dateFinDebut = req.query.date_fin_debut || ''; // format YYYY-MM
    const dateFinFin = req.query.date_fin_fin || '';     // format YYYY-MM
    const statutF = req.query.statut_ohm || '';
    const scoreMin = parseInt(req.query.score_min || '0');
    const nonAttr = req.query.non_attribues === 'true';
    const hasSign = req.query.has_signataire === 'true';
    const hasEmail = req.query.has_email === 'true';
    const page = parseInt(req.query.page || '1');
    const perPage = Math.min(parseInt(req.query.per_page || '50'), 50);

    const g = (row, name) => { const i = headers.findIndex(h => h.toLowerCase().includes(name.toLowerCase())); return i >= 0 && i < row.length ? row[i] : ''; };

    function parseDateFin(str) {
      if (!str || !str.trim()) return null;
      str = str.trim();
      // DD/MM/YYYY
      if (str.match(/^\d{1,2}\/\d{1,2}\/\d{4}$/)) {
        const [d,m,y] = str.split('/');
        return new Date(parseInt(y), parseInt(m)-1, parseInt(d));
      }
      // YYYY-MM-DD
      if (str.match(/^\d{4}-\d{2}-\d{2}$/)) return new Date(str);
      // MM/YYYY
      if (str.match(/^\d{1,2}\/\d{4}$/)) {
        const [m,y] = str.split('/');
        return new Date(parseInt(y), parseInt(m)-1, 1);
      }
      // Nombre Excel
      if (str.match(/^\d+$/)) {
        const epoch = new Date(1899, 11, 30);
        return new Date(epoch.getTime() + parseInt(str) * 86400000);
      }
      return null;
    }
    const dDebut = dateFinDebut ? new Date(dateFinDebut + '-01') : null;
    const dFin = dateFinFin ? new Date(parseInt(dateFinFin.split('-')[0]), parseInt(dateFinFin.split('-')[1]), 0) : null; // dernier jour du mois

    const filtered = [];
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      if (segF) { const s = (g(row, 'segment') || g(row, 'typologie') || '').toUpperCase(); if (!s.includes(segF)) continue; }
      if (anneeF && !(g(row, 'date_fin') || '').includes(anneeF)) continue;
      if (dDebut || dFin) {
        const d = parseDateFin(g(row, 'date_debut_livraison'));
        if (!d) continue;
        if (dDebut && d < dDebut) continue;
        if (dFin && d > dFin) continue;
      }
      if (statutF && (g(row, 'statut') || '') !== statutF) continue;
      if (scoreMin > 0) { const sc = parseInt(g(row, 'score') || '0'); if (isNaN(sc) || sc < scoreMin) continue; }
      if (hasSign && !(g(row, 'signataire') || '').trim()) continue;
      if (hasEmail && !(g(row, 'email_signataire') || '').trim()) continue;
      if (nonAttr && (g(row, 'vendeur_attribue') || '').trim()) continue;
      filtered.push({ _row: i + 1, _data: row });
    }
    console.log('💎 BASE PREMIUM liste:', filtered.length, 'résultats');

    const total = filtered.length;
    const pages = Math.ceil(total / perPage);
    const start = (page - 1) * perPage;
    const slice = filtered.slice(start, start + perPage);

    const prospects = slice.map(f => {
      const obj = { _row: f._row };
      headers.forEach((h, j) => { obj[h] = f._data[j] || ''; });
      return obj;
    });

    res.json({ success: true, prospects, total, page, pages });
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

// Lister les fiches attribuées
app.get('/api/admin/leads-attribues', verifyToken, isAdminMW, async (req, res) => {
  try {
    let rows;
    try { rows = await getSheetData('LEADS OHM'); } catch(e) { return res.json({ success: true, prospects: [] }); }
    if (rows.length < 2) return res.json({ success: true, prospects: [] });
    const headers = rows[0];
    const g = (row, name) => { const i = headers.findIndex(h => h.toLowerCase().includes(name.toLowerCase())); return i >= 0 && i < row.length ? row[i] : ''; };
    const vendeurF = (req.query.vendeur || '').toLowerCase();

    const prospects = [];
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      const attr = g(row, 'vendeur_attribue').trim();
      if (!attr) continue;
      if (vendeurF && !attr.toLowerCase().includes(vendeurF)) continue;
      prospects.push({
        _row: i + 1,
        raison_sociale: g(row, 'raison_sociale'),
        siren: g(row, 'siren'),
        signataire: g(row, 'signataire'),
        tel_signataire: g(row, 'tel_signataire'),
        email_signataire: g(row, 'email_signataire'),
        score: g(row, 'score'),
        segments: g(row, 'segments') || g(row, 'typologie'),
        date_fin_livraison: g(row, 'date_fin_livraison'),
        volume_total: g(row, 'volume_total'),
        vendeur_attribue: attr,
        statut_appel: g(row, 'statut_appel'),
      });
    }
    console.log('📋 Fiches attribuées:', prospects.length);
    res.json({ success: true, prospects });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Retirer un lead attribué
app.post('/api/admin/retirer-lead', verifyToken, isAdminMW, async (req, res) => {
  try {
    const { row } = req.body;
    if (!row) return res.status(400).json({ error: 'row requis' });
    const col = await ensureColumn('LEADS OHM', 'vendeur_attribue');
    await updateCell('LEADS OHM', row, col, '');
    console.log(`✖ Lead ligne ${row} retiré (vendeur_attribue vidé)`);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
// Auto-init colonnes LEADS OHM au démarrage
async function initLeadsOhmColumns() {
  if (!DRIVE_CREDENTIALS || !MASTER_SHEET_ID) return;
  try {
    const sheets = getSheetsClient();
    const r = await sheets.spreadsheets.values.get({ spreadsheetId: MASTER_SHEET_ID, range: "'LEADS OHM'!1:1" });
    const headers = (r.data.values || [[]])[0];
    console.log('✅ LEADS OHM colonnes actuelles:', headers.length, '→', headers.slice(-10).join(' | '));
    const needed = ['vendeur_attribue', 'statut_appel', 'note_appel', 'date_rappel', 'rgpd_envoye', 'date_contact'];
    const missing = needed.filter(h => !headers.some(x => x.toLowerCase().trim() === h));
    if (missing.length > 0) {
      const startCol = colLetter(headers.length);
      console.log('📝 Ajout colonnes à partir de', startCol, ':', missing.join(', '));
      await sheets.spreadsheets.values.update({
        spreadsheetId: MASTER_SHEET_ID, range: `'LEADS OHM'!${startCol}1`,
        valueInputOption: 'RAW', requestBody: { values: [missing] }
      });
      console.log('✅ Colonnes LEADS OHM ajoutées au démarrage:', missing.join(', '));
    } else {
      console.log('✅ Colonnes LEADS OHM OK — vendeur_attribue trouvée');
    }
  } catch(e) { console.warn('⚠️ Init LEADS OHM:', e.message); }
}

app.listen(port, () => {
  console.log(`🚀 LILIWATT Prospection sur http://localhost:${port}`);
  initLeadsOhmColumns();
});
