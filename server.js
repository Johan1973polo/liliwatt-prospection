const express = require('express');
const jwt = require('jsonwebtoken');
const nodemailer = require('nodemailer');
const path = require('path');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3002;
const JWT_SECRET = process.env.JWT_SECRET || 'liliwatt-prospection-secret-2026';

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use('/data', express.static(path.join(__dirname, 'data')));

// ===== PRISMA =====
const { prisma } = require('./lib/db');
const bcrypt = require('bcryptjs');

// ===== AUTH MIDDLEWARE =====
const verifyToken = (req, res, next) => {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Non autorise' });
  try { req.user = jwt.verify(token, JWT_SECRET); next(); }
  catch(e) { return res.status(401).json({ error: 'Token invalide' }); }
};

const isAdminMW = (req, res, next) => {
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'Admin only' });
  next();
};

// ===== LOGIN (Neon + bcrypt) =====
app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password, mdp: mdpField } = req.body;
    const pwd = password || mdpField;
    if (!email || !pwd) return res.status(400).json({ error: 'Email et mot de passe requis' });

    const user = await prisma.user.findUnique({
      where: { email: email.trim().toLowerCase() },
    });

    if (!user || !user.isActive) return res.status(401).json({ error: 'Identifiants invalides' });
    if (!user.passwordHash) return res.status(401).json({ error: 'Compte non configure' });

    const ok = await bcrypt.compare(pwd, user.passwordHash);
    if (!ok) return res.status(401).json({ error: 'Identifiants invalides' });

    await prisma.user.update({ where: { id: user.id }, data: { lastSeen: new Date() } });

    // Charger le token RGPD separement
    const rgpdCred = await prisma.credential.findFirst({ where: { userId: user.id, serviceName: 'RGPD' }, select: { login: true } });

    const role = user.role === 'ADMIN' ? 'admin' : (user.role === 'REFERENT' ? 'referent' : 'vendeur');
    const tokenRgpd = rgpdCred?.login || '';

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

// ===== POST /api/heartbeat (+ WorkSession) =====
app.post('/api/heartbeat', verifyToken, async (req, res) => {
  try {
    const userId = req.user.id;
    const now = new Date();
    await prisma.user.update({ where: { id: userId }, data: { lastSeen: now } });

    const lastActivityMs = req.body?.lastActivity ? parseInt(req.body.lastActivity) : Date.now();
    const isInactive = (Date.now() - lastActivityMs) > 15 * 60 * 1000;
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());

    let session = await prisma.workSession.findFirst({ where: { userId, endedAt: null, date: today }, orderBy: { startedAt: 'desc' } });

    if (isInactive && session) {
      const dur = Math.max(1, Math.round((lastActivityMs - new Date(session.startedAt).getTime()) / 60000));
      await prisma.workSession.update({ where: { id: session.id }, data: { endedAt: new Date(lastActivityMs), durationMinutes: dur } });
      session = null;
    }
    if (!session && !isInactive) {
      session = await prisma.workSession.create({ data: { userId, startedAt: now, date: today } });
    }

    const sessions = await prisma.workSession.findMany({ where: { userId, date: today }, select: { startedAt: true, endedAt: true, durationMinutes: true } });
    let totalMin = 0;
    for (const s of sessions) {
      if (s.endedAt && s.durationMinutes) totalMin += s.durationMinutes;
      else if (!s.endedAt) totalMin += Math.max(0, Math.round((Date.now() - new Date(s.startedAt).getTime()) / 60000));
    }

    res.json({ ok: true, sessionId: session?.id || null, totalMinutesToday: totalMin });
  } catch(e) { console.error('heartbeat error:', e); res.status(500).json({ error: e.message }); }
});

// ===== GET /api/me/presence =====
app.get('/api/me/presence', verifyToken, async (req, res) => {
  try {
    const uid = req.user.id;
    const now = new Date();
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const weekStart = new Date(today); const wd = weekStart.getDay() || 7; weekStart.setDate(weekStart.getDate() - wd + 1);
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);

    async function sumMin(since) {
      const ss = await prisma.workSession.findMany({ where: { userId: uid, date: { gte: since } }, select: { startedAt: true, endedAt: true, durationMinutes: true } });
      let t = 0; for (const s of ss) { if (s.durationMinutes) t += s.durationMinutes; else if (!s.endedAt) t += Math.round((Date.now() - new Date(s.startedAt).getTime()) / 60000); } return t;
    }
    const [todayMin, weekMin, monthMin] = await Promise.all([sumMin(today), sumMin(weekStart), sumMin(monthStart)]);
    const sessionsToday = await prisma.workSession.findMany({ where: { userId: uid, date: today }, orderBy: { startedAt: 'asc' }, select: { startedAt: true, endedAt: true, durationMinutes: true } });

    res.json({ success: true, today: todayMin, week: weekMin, month: monthMin, sessionsToday: sessionsToday.map(s => ({ start: s.startedAt, end: s.endedAt, duration: s.durationMinutes, active: !s.endedAt })) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== GET /api/me/rappels-jour =====
app.get('/api/me/rappels-jour', verifyToken, async (req, res) => {
  try {
    const uid = req.user.id; const now = new Date();
    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const tomorrow = new Date(today); tomorrow.setDate(tomorrow.getDate() + 1);
    const rappels = await prisma.prospect.findMany({
      where: { vendeurId: uid, statutAppel: 'A_RAPPELER', dateRappel: { not: null, lt: tomorrow } },
      orderBy: { dateRappel: 'asc' },
      select: { id: true, raisonSociale: true, ville: true, telephone: true, dateRappel: true, signataire: true, score: true, source: true }
    });
    const enRetard = [], aujourdhui = [], aVenir = [];
    for (const r of rappels) {
      const d = new Date(r.dateRappel);
      if (d < today) enRetard.push(r);
      else if (d <= now) aujourdhui.push(r);
      else aVenir.push(r);
    }
    res.json({ success: true, total: rappels.length, enRetard, aujourdhui, aVenir });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== GET /api/prospects/:id =====
app.get('/api/prospects/detail/:id', verifyToken, async (req, res) => {
  try {
    const p = await prisma.prospect.findUnique({ where: { id: req.params.id }, include: { vendeur: { select: { id: true, firstName: true, lastName: true, email: true } } } });
    if (!p) return res.status(404).json({ error: 'Fiche introuvable' });
    const isVendeur = req.user.role === 'vendeur';
    if (isVendeur && p.vendeurId && p.vendeurId !== req.user.id) return res.status(403).json({ error: 'Pas autorise' });
    res.json(p);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== POST /api/prospects/:id/toggle-favori =====
app.post('/api/prospects/:id/toggle-favori', verifyToken, async (req, res) => {
  try {
    const p = await prisma.prospect.findUnique({ where: { id: req.params.id }, select: { id: true, vendeurId: true, isFavori: true } });
    if (!p) return res.status(404).json({ error: 'Fiche introuvable' });
    const role = (req.user.role || '').toLowerCase();
    if (role !== 'admin' && p.vendeurId !== req.user.id) return res.status(403).json({ error: 'Pas votre fiche' });
    const v = !p.isFavori;
    await prisma.prospect.update({ where: { id: req.params.id }, data: { isFavori: v } });
    res.json({ success: true, isFavori: v });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== PUT /api/prospects/:id =====
app.put('/api/prospects/:id', verifyToken, async (req, res) => {
  try {
    const existing = await prisma.prospect.findUnique({ where: { id: req.params.id } });
    if (!existing) return res.status(404).json({ error: 'Fiche introuvable' });
    const isVendeur = req.user.role === 'vendeur';
    if (isVendeur && existing.vendeurId !== req.user.id) return res.status(403).json({ error: 'Pas autorise' });

    const allowed = ['raisonSociale','signataire','telephone','email','adresse','ville','codePostal','siteWeb','siren','secteur','noteAppel','statutAppel','dateRappel'];
    const data = {};
    for (const f of allowed) { if (req.body[f] !== undefined) data[f] = req.body[f] || null; }
    if (data.dateRappel) data.dateRappel = new Date(data.dateRappel);
    data.dateDernierAppel = new Date();

    // Regles metier statut
    if (data.statutAppel && data.statutAppel !== existing.statutAppel) {
      if (data.statutAppel === 'PAS_INTERESSE' && !existing.isVerrouillee) data.vendeurId = null;
      if (data.statutAppel === 'FAUX_NUMERO') { data.vendeurId = null; data.telephone = null; }
      // ActivityLog
      const callStatuts = ['APPELE','INTERESSE','PAS_INTERESSE','NE_REPOND_PAS','A_RAPPELER','FAUX_NUMERO','ATTENTE_DOCUMENTS','DOSSIER_RECU','CLIENT_SIGNE'];
      if (callStatuts.includes(data.statutAppel)) {
        await prisma.activityLog.create({ data: { userId: req.user.id, prospectId: req.params.id, type: 'CALL', metadata: { resultat: data.statutAppel } } });
      }
      await prisma.activityLog.create({ data: { userId: req.user.id, prospectId: req.params.id, type: 'STATUS_CHANGE', metadata: { from: existing.statutAppel, to: data.statutAppel } } });
    }

    const updated = await prisma.prospect.update({ where: { id: req.params.id }, data });
    res.json(updated);
  } catch(e) { console.error('PUT prospect error:', e); res.status(500).json({ error: e.message }); }
});

// ===== GET /api/prospects/mes-fiches =====
app.get('/api/prospects/mes-fiches', verifyToken, async (req, res) => {
  try {
    const userId = req.user.id;
    const { statut, search, ville, secteur, page = 1, limit = 100 } = req.query;
    const where = { vendeurId: userId, source: { in: ['BRUTE', 'MANUELLE'] } };
    if (statut === 'favoris') { where.isFavori = true; }
    else if (statut && statut !== 'tous' && statut !== 'all') where.statutAppel = statut;
    if (ville && ville !== 'all') where.ville = ville;
    if (secteur && secteur !== 'all') where.secteur = secteur;
    if (search) {
      where.OR = [
        { raisonSociale: { contains: search, mode: 'insensitive' } },
        { ville: { contains: search, mode: 'insensitive' } },
        { telephone: { contains: search } },
        { email: { contains: search, mode: 'insensitive' } },
      ];
    }
    const skip = (parseInt(page) - 1) * parseInt(limit);
    const [prospects, total, statsByStatus, allForDropdowns, favorisCount] = await Promise.all([
      prisma.prospect.findMany({ where, orderBy: [{ dateRappel: 'asc' }, { updatedAt: 'desc' }], skip, take: parseInt(limit) }),
      prisma.prospect.count({ where }),
      prisma.prospect.groupBy({ by: ['statutAppel'], where: { vendeurId: userId, source: { in: ['BRUTE', 'MANUELLE'] } }, _count: true }),
      prisma.prospect.findMany({ where: { vendeurId: userId, source: { in: ['BRUTE', 'MANUELLE'] } }, select: { ville: true, secteur: true } }),
      prisma.prospect.count({ where: { vendeurId: userId, source: { in: ['BRUTE', 'MANUELLE'] }, isFavori: true } }),
    ]);
    const statusCounts = statsByStatus.reduce((a, s) => { a[s.statutAppel || 'NULL'] = s._count; return a; }, {});
    statusCounts.favoris = favorisCount;

    // Villes + secteurs uniques avec compteurs
    const villeMap = {}, secteurMap = {};
    allForDropdowns.forEach(f => {
      if (f.ville) villeMap[f.ville] = (villeMap[f.ville] || 0) + 1;
      if (f.secteur) secteurMap[f.secteur] = (secteurMap[f.secteur] || 0) + 1;
    });
    const villesDisponibles = Object.entries(villeMap).map(([v, c]) => ({ ville: v, count: c })).sort((a, b) => b.count - a.count);
    const secteursDisponibles = Object.entries(secteurMap).map(([s, c]) => ({ secteur: s, count: c })).sort((a, b) => b.count - a.count);

    res.json({ prospects, total, page: parseInt(page), limit: parseInt(limit), statusCounts, villesDisponibles, secteursDisponibles });
  } catch(e) { console.error('Mes-fiches error:', e); res.status(500).json({ error: e.message }); }
});

// ===== GET /api/prospects/brute (Neon) =====
app.get('/api/prospects/brute', verifyToken, async (req, res) => {
  try {
    const userId = req.user.id;
    const isAdmin = req.user.role === 'admin';
    const fSecteur = (req.query.secteur || '').toLowerCase();
    const fVille = (req.query.ville || '').toLowerCase();
    const fStatut = (req.query.statut || '').toLowerCase();
    const fSearch = (req.query.search || '').toLowerCase();

    const where = { source: { in: ['BRUTE', 'MANUELLE'] } };
    if (isAdmin) {
      // Admin voit tout
    } else if (req.user.role === 'referent') {
      // Referent voit ses vendeurs + vendeurs de ses sous-referents + libres
      const sousRefs = await prisma.user.findMany({ where: { referentId: userId, role: 'REFERENT', isActive: true }, select: { id: true } });
      const sousRefIds = sousRefs.map(r => r.id);
      const mesVendeurs = await prisma.user.findMany({
        where: { isActive: true, OR: [{ referentId: userId }, ...(sousRefIds.length ? [{ referentId: { in: sousRefIds } }] : [])] },
        select: { id: true }
      });
      const ids = mesVendeurs.map(v => v.id);
      ids.push(userId);
      sousRefIds.forEach(id => ids.push(id));
      where.OR = [{ vendeurId: { in: ids } }, { vendeurId: null }];
    } else {
      // Vendeur : ses fiches + libres
      where.OR = [{ vendeurId: userId }, { vendeurId: null }];
    }
    if (fSecteur) where.secteur = { contains: fSecteur, mode: 'insensitive' };
    if (fVille) where.ville = { contains: fVille, mode: 'insensitive' };
    if (fStatut === 'non_traite') {
      where.statutAppel = null;
    } else if (fStatut) {
      const sm = { 'a appeler':'A_APPELER','appele':'APPELE','interesse':'INTERESSE','attente de documents':'ATTENTE_DOCUMENTS',
        'dossier recu':'DOSSIER_RECU','a rappeler':'A_RAPPELER','pas interesse':'PAS_INTERESSE','faux numero':'FAUX_NUMERO',
        'ne repond pas':'NE_REPOND_PAS','client signe':'CLIENT_SIGNE' };
      where.statutAppel = sm[fStatut] || undefined;
    }
    // Recherche multi-champs
    if (fSearch) {
      const searchOR = [
        { raisonSociale: { contains: fSearch, mode: 'insensitive' } },
        { ville: { contains: fSearch, mode: 'insensitive' } },
        { codePostal: { contains: fSearch } },
        { telephone: { contains: fSearch } },
        { secteur: { contains: fSearch, mode: 'insensitive' } },
      ];
      if (where.OR) {
        // Combiner permissions OR + search OR avec AND
        const permOR = where.OR;
        delete where.OR;
        where.AND = [{ OR: permOR }, { OR: searchOR }];
      } else {
        where.OR = searchOR;
      }
    }

    const prospects = await prisma.prospect.findMany({
      where, orderBy: [{ vendeurId: 'desc' }, { createdAt: 'desc' }], take: 200,
      include: { vendeur: { select: { email: true, firstName: true, lastName: true } } }
    });

    const mapped = prospects.map(p => ({
      _row: p.id, id: p.id, place_id: p.placeId,
      raison_sociale: p.raisonSociale, adresse: p.adresse || '', ville: p.ville || '',
      secteur: p.secteur || '', telephone: p.telephone || '', site_web: p.siteWeb || '',
      note_google: p.noteGoogle || '', nb_avis: p.nbAvis || '',
      vendeur_attribue: p.vendeur?.email || '',
      statut_appel: p.statutAppel ? p.statutAppel.replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, c => c.toUpperCase()) : '',
      note_appel: p.noteAppel || '', date_rappel: p.dateRappel || '',
      rgpd_envoye: p.rgpdEnvoye ? 'oui' : '', email_envoye_a: p.emailEnvoyeA || '',
      date_dernier_appel: p.dateDernierAppel || '',
      _attribue: p.vendeurId === userId || (isAdmin && !!p.vendeurId),
      isManuelle: p.isManuelle, isVerrouillee: p.isVerrouillee,
      vendeur_nom: p.vendeur ? `${p.vendeur.firstName || ''} ${p.vendeur.lastName || ''}`.trim() : '',
    }));

    const allBrute = await prisma.prospect.findMany({
      where: { source: { in: ['BRUTE', 'MANUELLE'] } }, select: { secteur: true, ville: true }, distinct: ['secteur', 'ville'],
    });
    const secteurs = [...new Set(allBrute.map(p => p.secteur).filter(Boolean))];
    const villes = [...new Set(allBrute.map(p => p.ville).filter(Boolean))];

    res.json({ success: true, prospects: mapped, secteurs, villes });
  } catch(e) { console.error('Brute error:', e.message); res.status(500).json({ error: e.message }); }
});

// ===== GET /api/prospects/leads (Neon) =====
app.get('/api/prospects/leads', verifyToken, async (req, res) => {
  try {
    const userId = req.user.id;
    const role = req.user.role;

    let vendeurFilter;
    if (role === 'admin') {
      vendeurFilter = { not: null };
    } else if (role === 'referent') {
      const sousRefs = await prisma.user.findMany({ where: { referentId: userId, role: 'REFERENT', isActive: true }, select: { id: true } });
      const sousRefIds = sousRefs.map(r => r.id);
      const mesVendeurs = await prisma.user.findMany({
        where: { isActive: true, OR: [{ referentId: userId }, ...(sousRefIds.length ? [{ referentId: { in: sousRefIds } }] : [])] },
        select: { id: true }
      });
      const ids = mesVendeurs.map(v => v.id);
      ids.push(userId);
      sousRefIds.forEach(id => ids.push(id));
      vendeurFilter = { in: ids };
    } else {
      vendeurFilter = userId;
    }

    const { statut } = req.query;
    const baseWhere = { source: { in: ['PREMIUM', 'PREMIUM_SIGNED'] }, vendeurId: vendeurFilter };
    const where = { ...baseWhere };
    if (statut && statut !== 'all') {
      if (statut === 'favoris') where.isFavori = true;
      else {
        const map = { a_appeler:'A_APPELER', appele:'APPELE', a_rappeler:'A_RAPPELER', interesse:'INTERESSE', attente_doc:'ATTENTE_DOCUMENTS', dossier_recu:'DOSSIER_RECU', client_signe:'CLIENT_SIGNE', pas_interesse:'PAS_INTERESSE', nrp:'NE_REPOND_PAS', faux_numero:'FAUX_NUMERO' };
        if (map[statut]) where.statutAppel = map[statut];
      }
    }

    const [items, statsByStatus, favorisCount] = await Promise.all([
      prisma.prospect.findMany({ where, orderBy: [{ dateFinLivraison: 'asc' }], take: 200, include: { vendeur: { select: { email: true, firstName: true, lastName: true } } } }),
      prisma.prospect.groupBy({ by: ['statutAppel'], where: baseWhere, _count: true }),
      prisma.prospect.count({ where: { ...baseWhere, isFavori: true } }),
    ]);
    const statusCounts = statsByStatus.reduce((a, s) => { a[s.statutAppel || 'NULL'] = s._count; return a; }, {});
    statusCounts.favoris = favorisCount;

    const prospects = items.map(p => ({
      _row: p.id, id: p.id, _sheet: 'LEADS OHM', _attribue: true,
      raison_sociale: p.raisonSociale, siren: p.siren || '',
      signataire: p.signataire || '', email_signataire: p.email || '',
      tel_signataire: p.telephone || '', adresse: p.adresse || '',
      score: p.score || '', pay_rank: p.payRank || '', observation_pay_rank: '',
      date_fin_livraison: p.dateFinLivraison ? new Date(p.dateFinLivraison).toLocaleDateString('fr-FR') : '',
      volume_total: p.volumeTotal || '',
      segments: p.segment || '', energie: p.energie || '',
      statut_appel: p.statutAppel || '', note_appel: p.noteAppel || '',
      vendeur_attribue: p.vendeur?.email || req.user.email,
      vendeur_nom: p.vendeur ? `${p.vendeur.firstName || ''} ${p.vendeur.lastName || ''}`.trim() : '',
      isFavori: p.isFavori || false,
    }));

    console.log(`💎 LEADS pour ${req.user.email}: ${prospects.length} fiches`);
    res.json({ success: true, prospects, favorisCount, statusCounts });
  } catch(e) { console.error('Leads error:', e.message); res.status(500).json({ error: e.message }); }
});

// ===== POST /api/prospects/prendre/:id (Neon) =====
app.post('/api/prospects/prendre/:id', verifyToken, async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.id;
    const prospect = await prisma.prospect.findUnique({ where: { id }, select: { id: true, vendeurId: true, raisonSociale: true, isVerrouillee: true } });
    if (!prospect) return res.status(404).json({ error: 'Prospect introuvable' });
    if (prospect.isVerrouillee) return res.status(403).json({ error: 'Fiche verrouillee' });
    if (prospect.vendeurId && prospect.vendeurId !== userId) return res.status(409).json({ error: 'Fiche deja attribuee' });

    const updated = await prisma.prospect.update({ where: { id }, data: { vendeurId: userId, statutAppel: 'A_APPELER' } });
    await prisma.activityLog.create({ data: { userId, prospectId: id, type: 'ATTRIBUTION', metadata: { raisonSociale: prospect.raisonSociale } } });

    console.log(`📌 ${prospect.raisonSociale} prise par ${req.user.email}`);
    res.json({ success: true, prospect: { ...updated, _row: updated.id } });
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

    const prospect = await prisma.prospect.findUnique({ where: { id }, select: { id: true, vendeurId: true, raisonSociale: true, isVerrouillee: true } });
    if (!prospect) return res.status(404).json({ error: 'Prospect introuvable' });
    if (prospect.vendeurId && prospect.vendeurId !== userId && !isAdminUser) return res.status(403).json({ error: 'Fiche attribuee a un autre vendeur' });

    const data = { statutAppel: statutEnum, dateDernierAppel: new Date() };
    if (note !== undefined) data.noteAppel = note;
    if (date_rappel) data.dateRappel = new Date(date_rappel);
    if (!prospect.vendeurId) data.vendeurId = userId;
    if (statutEnum === 'PAS_INTERESSE' && !prospect.isVerrouillee) data.vendeurId = null;
    if (statutEnum === 'FAUX_NUMERO') { data.vendeurId = null; data.telephone = null; }

    await prisma.prospect.update({ where: { id }, data });
    await prisma.activityLog.create({ data: { userId, prospectId: id, type: 'STATUS_CHANGE', metadata: { newStatut: statutEnum } } });

    // Log CALL si statut post-appel
    if (['APPELE','INTERESSE','PAS_INTERESSE','NE_REPOND_PAS','A_RAPPELER','FAUX_NUMERO','ATTENTE_DOCUMENTS','DOSSIER_RECU','CLIENT_SIGNE'].includes(statutEnum)) {
      await prisma.activityLog.create({ data: { userId, prospectId: id, type: 'CALL', metadata: { resultat: statutEnum } } });
    }

    console.log(`📞 ${prospect.raisonSociale}: ${statutEnum} par ${req.user.email}`);
    res.json({ success: true });
  } catch(e) { console.error('Statut error:', e); res.status(500).json({ error: e.message }); }
});

// ===== POST /api/prospects/mail/:id (RGPD centralise via contact@liliwatt.fr) =====
app.post('/api/prospects/mail/:id', verifyToken, async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user.id;

    const prospect = await prisma.prospect.findUnique({ where: { id } });
    if (!prospect) return res.status(404).json({ error: 'Fiche introuvable' });

    // Champs modifiables depuis le modal RGPD
    const emailDest = req.body.email_destinataire || prospect.email;
    const signataire = req.body.nom_gerant || prospect.signataire;
    const telephone = req.body.telephone || prospect.telephone;
    if (!emailDest) return res.status(400).json({ error: 'Aucun email pour ce prospect. Renseigne-le dans la fiche.' });

    // Infos vendeur pour signature
    const vendeur = await prisma.user.findUnique({ where: { id: prospect.vendeurId || userId }, select: { email: true, firstName: true, lastName: true, phone: true } });
    if (!vendeur) return res.status(500).json({ error: 'Vendeur introuvable' });

    const smtpUser = process.env.SMTP_USER || 'contact@liliwatt.fr';
    const smtpPass = process.env.SMTP_PASS;
    if (!smtpPass) return res.status(500).json({ error: 'Configuration SMTP manquante. Contactez l\'admin.' });

    const prenom = vendeur.firstName || '';
    const nom = vendeur.lastName || '';
    const nomComplet = `${prenom} ${nom}`.trim() || 'Votre conseiller';
    const phoneLine = vendeur.phone ? `<tr><td style="padding:4px 0;color:#7c3aed;font-weight:600;">📞</td><td style="padding:4px 0 4px 8px;">${vendeur.phone}</td></tr>` : '';
    const prospectNom = signataire || prospect.raisonSociale || 'Madame, Monsieur';
    // token_rgpd est soit une URL complete, soit juste le token
    const tokenRgpd = req.user.token_rgpd || '';
    const rgpdLink = tokenRgpd.startsWith('http') ? tokenRgpd : `https://liliwatt-courtier.onrender.com/rgpd/${tokenRgpd}`;

    const html = `<!DOCTYPE html><html lang="fr"><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f5f3ff;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
<table cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#f5f3ff;"><tr><td align="center" style="padding:40px 20px;">
<table cellpadding="0" cellspacing="0" border="0" width="600" style="max-width:600px;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(124,58,237,.08);">
<tr><td style="background:linear-gradient(135deg,#7c3aed,#d946ef);padding:36px 40px;text-align:center;">
  <h1 style="margin:0;color:#fff;font-size:32px;font-weight:800;letter-spacing:-.02em;">LILIWATT</h1>
  <p style="margin:8px 0 0;color:rgba(255,255,255,.9);font-size:13px;letter-spacing:.05em;text-transform:uppercase;font-weight:600;">Courtier en energie B2B</p>
</td></tr>
<tr><td style="padding:40px;">
  <p style="margin:0 0 20px;font-size:15px;color:#1c1917;line-height:1.6;">Bonjour <strong>${prospectNom}</strong>,</p>
  <p style="margin:0 0 20px;font-size:15px;color:#1c1917;line-height:1.7;">Suite a notre echange telephonique, je vous confirme votre interet pour notre <strong>etude comparative gratuite</strong> des fournisseurs d'energie.</p>
  <p style="margin:0 0 16px;font-size:15px;color:#1c1917;line-height:1.7;">Pour vous presenter les meilleures offres, j'ai besoin que vous signiez ce mandat de gestion (RGPD) qui me permet :</p>
  <ul style="margin:0 0 20px;padding:0 0 0 20px;color:#44403c;line-height:2;font-size:14px;">
    <li>D'acceder a <strong>vos donnees de consommation</strong></li>
    <li>De <strong>negocier en votre nom</strong> les meilleurs tarifs</li>
    <li>De vous presenter <strong>un comparatif detaille</strong> avec les economies realisables</li>
  </ul>
  <table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:24px 0;background:linear-gradient(135deg,#fef3c7,#fde68a);border-radius:10px;border-left:4px solid #f59e0b;">
    <tr><td style="padding:14px 18px;"><p style="margin:0;color:#78350f;font-size:14px;font-weight:600;">⚡ Sans engagement et 100% gratuit</p><p style="margin:4px 0 0;color:#92400e;font-size:13px;">Notre service est finance par les fournisseurs partenaires.</p></td></tr>
  </table>
  <table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:32px 0;"><tr><td align="center">
    <a href="${rgpdLink}" style="display:inline-block;padding:16px 36px;background:linear-gradient(135deg,#7c3aed,#d946ef);color:#fff;text-decoration:none;border-radius:10px;font-weight:700;font-size:15px;box-shadow:0 4px 14px rgba(124,58,237,.4);">✍️ Signer le mandat en ligne</a>
  </td></tr><tr><td align="center" style="padding-top:8px;"><p style="margin:0;color:#78716c;font-size:12px;">Signature electronique · 2 minutes · Securise</p></td></tr></table>
  <p style="margin:32px 0 8px;font-size:15px;color:#1c1917;">A tres vite,</p>
  <table cellpadding="0" cellspacing="0" border="0" style="margin-top:8px;border-top:2px solid #ede9fe;padding-top:20px;width:100%;"><tr>
    <td valign="top" style="padding-right:16px;"><table cellpadding="0" cellspacing="0" border="0"><tr><td style="width:56px;height:56px;background:linear-gradient(135deg,#7c3aed,#d946ef);border-radius:50%;text-align:center;vertical-align:middle;color:#fff;font-weight:700;font-size:20px;line-height:56px;">${(prenom[0]||'').toUpperCase()}${(nom[0]||'').toUpperCase()}</td></tr></table></td>
    <td valign="top">
      <p style="margin:0 0 2px;font-size:17px;font-weight:700;color:#1c1917;">${prenom} <span style="text-transform:uppercase;">${nom}</span></p>
      <p style="margin:0 0 12px;font-size:13px;color:#7c3aed;font-weight:600;">Conseiller LILIWATT</p>
      <table cellpadding="0" cellspacing="0" border="0" style="font-size:13px;color:#44403c;">
        <tr><td style="padding:4px 0;color:#7c3aed;font-weight:600;">📧</td><td style="padding:4px 0 4px 8px;"><a href="mailto:${vendeur.email}" style="color:#44403c;text-decoration:none;">${vendeur.email}</a></td></tr>
        ${phoneLine}
        <tr><td style="padding:4px 0;color:#7c3aed;font-weight:600;">🌐</td><td style="padding:4px 0 4px 8px;"><a href="https://liliwatt.fr" style="color:#44403c;text-decoration:none;">liliwatt.fr</a></td></tr>
      </table>
    </td>
  </tr></table>
</td></tr>
<tr><td style="background:#fafaf9;padding:20px 40px;border-top:1px solid #f5f5f4;">
  <p style="margin:0 0 4px;font-size:11px;color:#78716c;text-align:center;line-height:1.5;"><strong>LILISTRAT STRATEGIE SAS</strong> · Marque <strong>LILIWATT</strong></p>
  <p style="margin:0;font-size:10px;color:#a8a29e;text-align:center;line-height:1.5;">59 rue de Ponthieu, Bureau 326 · 75008 Paris<br>SIREN 103 572 947 · SAS au capital de 10 000€</p>
</td></tr>
</table></td></tr></table></body></html>`;

    const transporter = nodemailer.createTransport({ host: process.env.SMTP_HOST || 'smtp.zoho.eu', port: parseInt(process.env.SMTP_PORT || '465'), secure: true, auth: { user: smtpUser, pass: smtpPass } });
    await transporter.sendMail({ from: `"LILIWATT - ${nomComplet}" <${smtpUser}>`, replyTo: vendeur.email, to: emailDest, subject: 'Mandat de gestion energie - LILIWATT', html });

    await prisma.prospect.update({ where: { id }, data: { rgpdEnvoye: true, emailEnvoyeA: emailDest, statutAppel: 'ATTENTE_DOCUMENTS', ...(signataire && { signataire }), ...(emailDest && { email: emailDest }), ...(telephone && { telephone }) } });
    await prisma.activityLog.create({ data: { userId, prospectId: id, type: 'RGPD_SENT', metadata: { destinataire: emailDest, envoyeDe: smtpUser, replyTo: vendeur.email } } });

    console.log(`📧 RGPD envoye a ${emailDest} via ${smtpUser} (replyTo: ${vendeur.email})`);
    res.json({ success: true, message: `Mandat envoye a ${emailDest}`, from: smtpUser, replyTo: vendeur.email });
  } catch(e) { console.error('Mail error:', e.message); res.status(500).json({ error: e.message }); }
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

    const historique = myProspects.filter(p => p.dateDernierAppel).slice(0, 10).map(p => ({
      date: new Date(p.dateDernierAppel).toLocaleString('fr-FR'), nom: p.raisonSociale, statut: p.statutAppel || '', row: p.id,
    }));

    res.json({ success: true, kpis: { total, appels, interesses, rappels, rgpd }, historique });
  } catch(e) { console.error('KPIs error:', e); res.status(500).json({ error: e.message }); }
});

// ===== GET /api/kpis/me (KPIs vendeur avec filtres temporels) =====
const { getDateRange } = require('./lib/dateFilters');

app.get('/api/kpis/me', verifyToken, async (req, res) => {
  try {
    const userId = req.user.id;
    const period = req.query.period || 'all';
    const dateFilter = getDateRange(period);
    const actWhere = dateFilter ? { userId, timestamp: dateFilter } : { userId };

    const [appels, rgpd, fiches, ventes, factures, pipeline, temps] = await Promise.all([
      prisma.activityLog.count({ where: { ...actWhere, type: 'CALL' } }),
      prisma.activityLog.count({ where: { ...actWhere, type: 'RGPD_SENT' } }),
      prisma.activityLog.count({ where: { ...actWhere, type: 'ATTRIBUTION' } }),
      prisma.activityLog.count({ where: { ...actWhere, type: 'SALE_SIGNED' } }),
      prisma.activityLog.count({ where: { ...actWhere, type: 'INVOICE_RECEIVED' } }),
      prisma.prospect.groupBy({ by: ['statutAppel'], where: { vendeurId: userId }, _count: true }),
      prisma.workSession.aggregate({ where: { userId, startedAt: dateFilter || undefined }, _sum: { durationMinutes: true } }),
    ]);

    const adhesion = appels > 0 ? Math.round((rgpd / appels) * 1000) / 10 : 0;
    const retour = rgpd > 0 ? Math.round((factures / rgpd) * 1000) / 10 : 0;
    const closing = factures > 0 ? Math.round((ventes / factures) * 1000) / 10 : 0;
    function color(v, type) {
      const t = type === 'adhesion' ? [30,15,5] : [30,20,10];
      return v >= t[0] ? 'fire' : v >= t[1] ? 'green' : v >= t[2] ? 'amber' : 'red';
    }
    const ps = pipeline.reduce((a, p) => { a[p.statutAppel || 'NULL'] = p._count; return a; }, {});

    res.json({
      period,
      counters: { appels, rgpd, fichesPrises: fiches, factures, ventes, tempsActifMinutes: temps._sum.durationMinutes || 0 },
      ratios: {
        adhesion: { value: adhesion, color: color(adhesion, 'adhesion') },
        retourFacture: { value: retour, color: color(retour, 'retour') },
        closing: { value: closing, color: color(closing, 'closing') },
      },
      pipeline: {
        aAppeler: ps.A_APPELER || 0, appele: ps.APPELE || 0, interesse: ps.INTERESSE || 0,
        attenteDocs: ps.ATTENTE_DOCUMENTS || 0, dossierRecu: ps.DOSSIER_RECU || 0,
        aRappeler: ps.A_RAPPELER || 0, clientSigne: ps.CLIENT_SIGNE || 0,
      }
    });
  } catch(e) { console.error('KPIs/me error:', e); res.status(500).json({ error: e.message }); }
});

// ===== GET /api/kpis/me/activity =====
app.get('/api/kpis/me/activity', verifyToken, async (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit) || 20, 50);
    const activities = await prisma.activityLog.findMany({
      where: { userId: req.user.id },
      orderBy: { timestamp: 'desc' },
      take: limit,
      include: { prospect: { select: { id: true, raisonSociale: true, telephone: true } } }
    });
    res.json({
      items: activities.map(a => ({
        id: a.id, type: a.type, timestamp: a.timestamp,
        prospect: a.prospect ? { id: a.prospect.id, nom: a.prospect.raisonSociale, telephone: a.prospect.telephone } : null,
        metadata: a.metadata,
      })),
      total: activities.length,
    });
  } catch(e) { console.error('Activity error:', e); res.status(500).json({ error: e.message }); }
});

// ===== GET /api/kpis/me/funnel =====
app.get('/api/kpis/me/funnel', verifyToken, async (req, res) => {
  try {
    const userId = req.user.id;
    const period = req.query.period || 'month';
    const dateFilter = getDateRange(period);
    const w = dateFilter ? { userId, timestamp: dateFilter } : { userId };

    const [fiches, appels, rgpd, factures, ventes] = await Promise.all([
      prisma.activityLog.count({ where: { ...w, type: 'ATTRIBUTION' } }),
      prisma.activityLog.count({ where: { ...w, type: 'CALL' } }),
      prisma.activityLog.count({ where: { ...w, type: 'RGPD_SENT' } }),
      prisma.activityLog.count({ where: { ...w, type: 'INVOICE_RECEIVED' } }),
      prisma.activityLog.count({ where: { ...w, type: 'SALE_SIGNED' } }),
    ]);

    res.json({
      period,
      steps: [
        { icon: '📋', label: 'Fiches prises', value: fiches, subtext: null },
        { icon: '📞', label: 'Appels', value: appels, subtext: fiches > 0 ? `${(appels/fiches).toFixed(1)} par fiche` : null },
        { icon: '✉️', label: 'RGPD', value: rgpd, subtext: appels > 0 ? `${((rgpd/appels)*100).toFixed(1)}% adhesion` : null },
        { icon: '📄', label: 'Factures', value: factures, subtext: rgpd > 0 ? `${((factures/rgpd)*100).toFixed(1)}% retour` : null },
        { icon: '🏆', label: 'Signes', value: ventes, subtext: factures > 0 ? `${((ventes/factures)*100).toFixed(0)}% closing` : null },
      ]
    });
  } catch(e) { console.error('Funnel error:', e); res.status(500).json({ error: e.message }); }
});

// ===== GET /api/kpis/team (referent + admin) =====
app.get('/api/kpis/team', verifyToken, async (req, res) => {
  try {
    const { id: userId, role } = req.user;
    const period = req.query.period || 'today';
    const dateFilter = getDateRange(period);
    if (role === 'vendeur') return res.status(403).json({ error: 'Reserve aux referents et admins' });

    let vendeurIds = [], teamLabel = '';
    if (role === 'admin') {
      const all = await prisma.user.findMany({ where: { role: 'VENDEUR', isActive: true }, select: { id: true } });
      vendeurIds = all.map(v => v.id); teamLabel = 'LILIWATT global';
    } else {
      const sousRefs = await prisma.user.findMany({ where: { referentId: userId, role: 'REFERENT', isActive: true }, select: { id: true } });
      const sousRefIds = sousRefs.map(r => r.id);
      const mesV = await prisma.user.findMany({
        where: { role: 'VENDEUR', isActive: true, OR: [{ referentId: userId }, ...(sousRefIds.length ? [{ referentId: { in: sousRefIds } }] : [])] },
        select: { id: true }
      });
      vendeurIds = mesV.map(v => v.id); teamLabel = 'Mon equipe';
    }

    if (!vendeurIds.length) return res.json({ teamLabel, period, vendeurCount: 0, counters: { appels: 0, rgpd: 0, factures: 0, ventes: 0, fichesPrises: 0 }, ratios: { adhesion: { value: 0, color: 'red' }, retourFacture: { value: 0, color: 'red' }, closing: { value: 0, color: 'red' } }, leaderboard: [] });

    const actW = dateFilter ? { userId: { in: vendeurIds }, timestamp: dateFilter } : { userId: { in: vendeurIds } };
    const [appels, rgpd, factures, ventes, fiches] = await Promise.all([
      prisma.activityLog.count({ where: { ...actW, type: 'CALL' } }),
      prisma.activityLog.count({ where: { ...actW, type: 'RGPD_SENT' } }),
      prisma.activityLog.count({ where: { ...actW, type: 'INVOICE_RECEIVED' } }),
      prisma.activityLog.count({ where: { ...actW, type: 'SALE_SIGNED' } }),
      prisma.activityLog.count({ where: { ...actW, type: 'ATTRIBUTION' } }),
    ]);

    const adhesion = appels > 0 ? Math.round((rgpd / appels) * 1000) / 10 : 0;
    const retour = rgpd > 0 ? Math.round((factures / rgpd) * 1000) / 10 : 0;
    const closing = factures > 0 ? Math.round((ventes / factures) * 1000) / 10 : 0;
    function col(v, t) { const th = t === 'adhesion' ? [30,15,5] : [30,20,10]; return v >= th[0] ? 'fire' : v >= th[1] ? 'green' : v >= th[2] ? 'amber' : 'red'; }

    const leaderboard = await Promise.all(vendeurIds.map(async vid => {
      const [u, vA, vR, vV, vT] = await Promise.all([
        prisma.user.findUnique({ where: { id: vid }, select: { id: true, firstName: true, lastName: true, email: true, lastSeen: true } }),
        prisma.activityLog.count({ where: { userId: vid, type: 'CALL', ...(dateFilter && { timestamp: dateFilter }) } }),
        prisma.activityLog.count({ where: { userId: vid, type: 'RGPD_SENT', ...(dateFilter && { timestamp: dateFilter }) } }),
        prisma.activityLog.count({ where: { userId: vid, type: 'SALE_SIGNED', ...(dateFilter && { timestamp: dateFilter }) } }),
        prisma.workSession.aggregate({ where: { userId: vid, ...(dateFilter && { startedAt: dateFilter }) }, _sum: { durationMinutes: true } }),
      ]);
      const adh = vA > 0 ? Math.round((vR / vA) * 1000) / 10 : 0;
      const isOnline = u.lastSeen && (Date.now() - new Date(u.lastSeen).getTime()) < 5 * 60 * 1000;
      return {
        id: u.id, firstName: u.firstName, lastName: u.lastName, email: u.email,
        appels: vA, rgpd: vR, ventes: vV,
        adhesion: { value: adh, color: col(adh, 'adhesion') },
        tempsActifMinutes: vT._sum.durationMinutes || 0,
        status: isOnline ? 'online' : 'offline',
      };
    }));
    leaderboard.sort((a, b) => b.appels - a.appels);

    res.json({
      teamLabel, period, vendeurCount: vendeurIds.length,
      objectives: { appelsCible: 120 * vendeurIds.length, tempsCible: 300 },
      counters: { appels, rgpd, factures, ventes, fichesPrises: fiches },
      ratios: { adhesion: { value: adhesion, color: col(adhesion, 'adhesion') }, retourFacture: { value: retour, color: col(retour, 'retour') }, closing: { value: closing, color: col(closing, 'closing') } },
      leaderboard,
    });
  } catch(e) { console.error('Team KPIs error:', e); res.status(500).json({ error: e.message }); }
});

// ===== GET /api/referent/mon-equipe =====
app.get('/api/referent/mon-equipe', verifyToken, async (req, res) => {
  try {
    const role = (req.user.role || '').toLowerCase();
    if (role !== 'referent' && role !== 'admin') return res.status(403).json({ error: 'Acces refuse' });

    const userId = req.user.id;
    // Vendeurs directs + sous-referents
    const sousRefs = await prisma.user.findMany({ where: { referentId: userId, role: 'REFERENT', isActive: true }, select: { id: true } });
    const sousRefIds = sousRefs.map(r => r.id);
    const vendeurs = await prisma.user.findMany({
      where: { role: 'VENDEUR', isActive: true, OR: [{ referentId: userId }, ...(sousRefIds.length ? [{ referentId: { in: sousRefIds } }] : [])] },
      select: { id: true, firstName: true, lastName: true, email: true, phone: true, lastSeen: true }
    });

    const since = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
    const todayD = new Date(); todayD.setHours(0, 0, 0, 0);
    const weekD = new Date(todayD); const wdx = weekD.getDay() || 7; weekD.setDate(weekD.getDate() - wdx + 1);

    const kpisVendeurs = await Promise.all(vendeurs.map(async v => {
      const [appels, rgpd, fiches, sessToday, sessWeek] = await Promise.all([
        prisma.activityLog.count({ where: { userId: v.id, type: 'CALL', timestamp: { gte: since } } }),
        prisma.activityLog.count({ where: { userId: v.id, type: 'RGPD_SENT', timestamp: { gte: since } } }),
        prisma.prospect.count({ where: { vendeurId: v.id } }),
        prisma.workSession.findMany({ where: { userId: v.id, date: todayD }, select: { startedAt: true, endedAt: true, durationMinutes: true } }),
        prisma.workSession.findMany({ where: { userId: v.id, date: { gte: weekD } }, select: { startedAt: true, endedAt: true, durationMinutes: true } }),
      ]);
      let minToday = 0; for (const s of sessToday) { if (s.durationMinutes) minToday += s.durationMinutes; else if (!s.endedAt) minToday += Math.round((Date.now() - new Date(s.startedAt).getTime()) / 60000); }
      let minWeek = 0; for (const s of sessWeek) { if (s.durationMinutes) minWeek += s.durationMinutes; else if (!s.endedAt) minWeek += Math.round((Date.now() - new Date(s.startedAt).getTime()) / 60000); }
      const ageMs = v.lastSeen ? Date.now() - new Date(v.lastSeen).getTime() : Infinity;
      const connectionStatus = ageMs < 60000 ? 'active' : ageMs < 15 * 60 * 1000 ? 'idle' : 'offline';
      return { id: v.id, nom: `${v.firstName || ''} ${v.lastName || ''}`.trim(), email: v.email, phone: v.phone, appels, rgpd, fichesActives: fiches, conversion: appels > 0 ? ((rgpd / appels) * 100).toFixed(1) : '0.0', online: connectionStatus === 'active', connectionStatus, minutesToday: minToday, minutesWeek: minWeek };
    }));

    const totaux = { vendeurs: kpisVendeurs.length, appels: kpisVendeurs.reduce((s, v) => s + v.appels, 0), rgpd: kpisVendeurs.reduce((s, v) => s + v.rgpd, 0), fichesActives: kpisVendeurs.reduce((s, v) => s + v.fichesActives, 0) };
    totaux.conversion = totaux.appels > 0 ? ((totaux.rgpd / totaux.appels) * 100).toFixed(1) : '0.0';

    res.json({ success: true, totauxEquipe: totaux, vendeurs: kpisVendeurs.sort((a, b) => b.appels - a.appels) });
  } catch(e) { console.error('Mon-equipe error:', e); res.status(500).json({ error: e.message }); }
});

// ===== GET /api/referent/vendeur/:id/kpis =====
app.get('/api/referent/vendeur/:id/kpis', verifyToken, async (req, res) => {
  try {
    const role = (req.user.role || '').toLowerCase();
    if (role !== 'referent' && role !== 'admin') return res.status(403).json({ error: 'Acces refuse' });

    const vid = req.params.id;
    const vendeur = await prisma.user.findUnique({ where: { id: vid }, select: { id: true, firstName: true, lastName: true, email: true, phone: true, referentId: true } });
    if (!vendeur) return res.status(404).json({ error: 'Vendeur introuvable' });
    if (role === 'referent' && vendeur.referentId !== req.user.id) return res.status(403).json({ error: 'Pas dans votre equipe' });

    const since7 = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
    const since30 = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
    async function kpis(since) {
      const [appels, rgpd] = await Promise.all([
        prisma.activityLog.count({ where: { userId: vid, type: 'CALL', timestamp: { gte: since } } }),
        prisma.activityLog.count({ where: { userId: vid, type: 'RGPD_SENT', timestamp: { gte: since } } }),
      ]);
      return { appels, rgpd, conversion: appels > 0 ? ((rgpd / appels) * 100).toFixed(1) : '0.0' };
    }
    const [k7, k30, topVilles] = await Promise.all([
      kpis(since7), kpis(since30),
      prisma.prospect.groupBy({ by: ['ville'], where: { vendeurId: vid }, _count: true, orderBy: { _count: { ville: 'desc' } }, take: 5 }),
    ]);

    res.json({ success: true, vendeur: { id: vendeur.id, nom: `${vendeur.firstName || ''} ${vendeur.lastName || ''}`.trim(), email: vendeur.email, phone: vendeur.phone }, kpis7d: k7, kpis30d: k30, topVilles: topVilles.map(f => ({ ville: f.ville, count: f._count })) });
  } catch(e) { console.error('Vendeur kpis error:', e); res.status(500).json({ error: e.message }); }
});

// ===== POST /api/prospects/manuelle (Neon) =====
app.post('/api/prospects/manuelle', verifyToken, async (req, res) => {
  try {
    const userId = req.user.id;
    const { raisonSociale, telephone, source: sourceManuelle, nomContact, prenomContact, email, siren, adresse, notes, signataire: sigBody, codePostal: cpBody, ville, secteur, segment, energie, noteAppel } = req.body;
    if (!raisonSociale?.trim()) return res.status(400).json({ error: 'Raison sociale obligatoire' });

    const cp = cpBody?.trim() || (adresse ? (adresse.match(/\b(\d{5})\b/) || [])[1] : null) || null;
    const sig = sigBody?.trim() || [prenomContact, nomContact].filter(Boolean).join(' ').trim() || null;

    const prospect = await prisma.prospect.create({
      data: {
        source: 'MANUELLE', raisonSociale: raisonSociale.trim(), telephone: telephone?.trim() || null,
        email: email?.trim() || null, siren: siren?.trim() || null, adresse: adresse?.trim() || null,
        codePostal: cp, ville: ville?.trim() || null, signataire: sig, secteur: secteur?.trim() || null,
        segment: segment?.trim() || null, energie: energie?.trim() || null,
        sourceManuelle: sourceManuelle?.trim() || req.user.email, noteAppel: noteAppel?.trim() || notes?.trim() || null,
        vendeurId: userId, statutAppel: 'A_APPELER', isManuelle: true, isVerrouillee: true,
      }
    });

    await prisma.activityLog.create({
      data: { userId, prospectId: prospect.id, type: 'PROSPECT_CREATED_MANUAL', metadata: { raisonSociale, source: sourceManuelle } }
    });

    console.log(`🔒 Fiche manuelle creee: ${raisonSociale} par ${req.user.email}`);
    res.json({ success: true, prospect: { ...prospect, _row: prospect.id } });
  } catch(e) { console.error('Manuelle error:', e); res.status(500).json({ error: e.message }); }
});

// =============================================================
// ADMIN ROUTES (Neon)
// =============================================================

// ===== GET /api/admin/stats =====
app.get('/api/admin/stats', verifyToken, isAdminMW, async (req, res) => {
  try {
    const [total, traitees, libres, hors_pool, signes] = await Promise.all([
      prisma.prospect.count({ where: { source: { in: ['BRUTE', 'MANUELLE'] } } }),
      prisma.prospect.count({ where: { source: { in: ['BRUTE', 'MANUELLE'] }, statutAppel: { not: null, notIn: ['A_APPELER'] } } }),
      prisma.prospect.count({ where: { source: { in: ['BRUTE', 'MANUELLE'] }, vendeurId: null } }),
      prisma.prospect.count({ where: { source: { in: ['BRUTE', 'MANUELLE'] }, statutAppel: 'FAUX_NUMERO' } }),
      prisma.prospect.count({ where: { source: { in: ['BRUTE', 'MANUELLE'] }, statutAppel: 'CLIENT_SIGNE' } }),
    ]);

    const vendeurs = await prisma.user.findMany({
      where: { role: { in: ['VENDEUR', 'REFERENT'] }, isActive: true },
      select: { id: true, email: true, firstName: true, lastName: true, role: true }
    });
    const par_vendeur = await Promise.all(vendeurs.map(async v => {
      const [nb_fiches, nb_traitees, nb_signes] = await Promise.all([
        prisma.prospect.count({ where: { vendeurId: v.id, source: { in: ['BRUTE', 'MANUELLE'] } } }),
        prisma.prospect.count({ where: { vendeurId: v.id, source: { in: ['BRUTE', 'MANUELLE'] }, statutAppel: { not: null, notIn: ['A_APPELER'] } } }),
        prisma.prospect.count({ where: { vendeurId: v.id, source: { in: ['BRUTE', 'MANUELLE'] }, statutAppel: 'CLIENT_SIGNE' } }),
      ]);
      return { email: v.email, nom: `${v.firstName || ''} ${v.lastName || ''}`.trim(), nb_fiches, nb_traitees, nb_signes };
    }));

    res.json({ success: true, stats: { total, traitees, libres, hors_pool, signes, par_vendeur } });
  } catch(e) { console.error('Stats error:', e); res.status(500).json({ error: e.message }); }
});

// ===== GET /api/admin/vendeurs-list =====
app.get('/api/admin/vendeurs-list', verifyToken, isAdminMW, async (req, res) => {
  try {
    const vendeurs = await prisma.user.findMany({
      where: { role: { in: ['VENDEUR', 'REFERENT'] }, isActive: true },
      select: { id: true, email: true, firstName: true, lastName: true, role: true, _count: { select: { prospectsAttribues: true } } },
      orderBy: [{ role: 'asc' }, { firstName: 'asc' }]
    });
    res.json({ success: true, vendeurs: vendeurs.map(v => ({
      id: v.id, email: v.email, prenom: v.firstName || '', nom: v.lastName || '',
      firstName: v.firstName, lastName: v.lastName,
      role: v.role === 'REFERENT' ? 'referent' : 'vendeur', nbAttribues: v._count.prospectsAttribues
    }))});
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== GET /api/admin/leads-ohm =====
app.get('/api/admin/leads-ohm', verifyToken, isAdminMW, async (req, res) => {
  try {
    const segF = (req.query.segment || '').toUpperCase();
    const dateFinDebut = req.query.date_fin_debut || '';
    const dateFinFin = req.query.date_fin_fin || '';
    const scoreMin = parseInt(req.query.score_min || '0');
    const nonAttr = req.query.non_attribues === 'true';
    const hasSign = req.query.has_signataire === 'true';
    const hasEmail = req.query.has_email === 'true';
    const page = parseInt(req.query.page || '1');
    const perPage = Math.min(parseInt(req.query.per_page || '50'), 50);

    const where = { source: { in: ['PREMIUM', 'PREMIUM_SIGNED'] } };
    if (segF) where.segment = { contains: segF, mode: 'insensitive' };
    if (dateFinDebut) where.dateDebutLivraison = { ...(where.dateDebutLivraison || {}), gte: new Date(dateFinDebut + '-01') };
    if (dateFinFin) {
      const [y, m] = dateFinFin.split('-');
      where.dateDebutLivraison = { ...(where.dateDebutLivraison || {}), lte: new Date(parseInt(y), parseInt(m), 0) };
    }
    if (scoreMin > 0) where.score = { gte: scoreMin };
    if (nonAttr) where.vendeurId = null;
    if (hasSign) where.signataire = { not: null };
    if (hasEmail) where.email = { not: null };

    const [items, total] = await Promise.all([
      prisma.prospect.findMany({
        where, skip: (page - 1) * perPage, take: perPage,
        orderBy: { dateFinLivraison: 'asc' },
        include: { vendeur: { select: { email: true } } }
      }),
      prisma.prospect.count({ where })
    ]);

    const prospects = items.map(p => ({
      _row: p.id, raison_sociale: p.raisonSociale, siren: p.siren || '',
      signataire: p.signataire || '', email_signataire: p.email || '', tel_signataire: p.telephone || '',
      adresse: p.adresse || '', score: p.score || '', segments: p.segment || '',
      date_fin_livraison: p.dateFinLivraison ? new Date(p.dateFinLivraison).toLocaleDateString('fr-FR') : '',
      volume_total: p.volumeTotal || '', vendeur_attribue: p.vendeur?.email || '',
      statut_appel: p.statutAppel || ''
    }));

    res.json({ success: true, prospects, total, page, pages: Math.ceil(total / perPage) });
  } catch(e) { console.error('Leads-ohm error:', e); res.status(500).json({ error: e.message }); }
});

// ===== POST /api/admin/attribuer-leads =====
app.post('/api/admin/attribuer-leads', verifyToken, isAdminMW, async (req, res) => {
  try {
    const { rows: rowsList, vendeur_email } = req.body;
    if (!rowsList?.length || !vendeur_email) return res.status(400).json({ error: 'rows et vendeur requis' });
    if (rowsList.length > 100) return res.status(400).json({ error: 'Max 100 leads' });

    const vendeur = await prisma.user.findUnique({ where: { email: vendeur_email.toLowerCase() }, select: { id: true } });
    if (!vendeur) return res.status(404).json({ error: 'Vendeur introuvable' });

    // rowsList contient des IDs (cuid) maintenant
    const result = await prisma.prospect.updateMany({
      where: { id: { in: rowsList }, vendeurId: null },
      data: { vendeurId: vendeur.id, statutAppel: 'A_APPELER' }
    });

    console.log(`⭐ ${result.count} leads attribues a ${vendeur_email}`);
    res.json({ success: true, attribues: result.count });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== GET /api/admin/leads-attribues =====
app.get('/api/admin/leads-attribues', verifyToken, isAdminMW, async (req, res) => {
  try {
    const vendeurF = (req.query.vendeur || '').toLowerCase();
    const where = { source: { in: ['PREMIUM', 'PREMIUM_SIGNED'] }, vendeurId: { not: null } };
    if (vendeurF) {
      const u = await prisma.user.findUnique({ where: { email: vendeurF }, select: { id: true } });
      if (u) where.vendeurId = u.id;
    }

    const items = await prisma.prospect.findMany({
      where, orderBy: { updatedAt: 'desc' },
      include: { vendeur: { select: { email: true } } }
    });

    const prospects = items.map(p => ({
      _row: p.id, raison_sociale: p.raisonSociale, siren: p.siren || '',
      signataire: p.signataire || '', tel_signataire: p.telephone || '', email_signataire: p.email || '',
      score: p.score || '', segments: p.segment || '',
      date_fin_livraison: p.dateFinLivraison ? new Date(p.dateFinLivraison).toLocaleDateString('fr-FR') : '',
      volume_total: p.volumeTotal || '', vendeur_attribue: p.vendeur?.email || '',
      statut_appel: p.statutAppel || ''
    }));

    console.log(`📋 Fiches attribuees: ${prospects.length}`);
    res.json({ success: true, prospects });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== POST /api/admin/retirer-lead =====
app.post('/api/admin/retirer-lead', verifyToken, isAdminMW, async (req, res) => {
  try {
    const { row, id } = req.body;
    const prospectId = id || row; // compat: accept both id and row
    if (!prospectId) return res.status(400).json({ error: 'id requis' });

    await prisma.prospect.update({
      where: { id: prospectId },
      data: { vendeurId: null, statutAppel: null }
    });

    await prisma.activityLog.create({
      data: { userId: req.user.id, prospectId, type: 'REATTRIBUTION_ADMIN', metadata: { action: 'retrait' } }
    });

    console.log(`✖ Lead ${prospectId} retire`);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== POST /api/admin/scraper (Neon) =====
app.post('/api/admin/scraper', verifyToken, isAdminMW, async (req, res) => {
  try {
    const { secteur, ville } = req.body;
    if (!secteur || !ville) return res.status(400).json({ error: 'secteur et ville requis' });
    const allowed = ['restaurant','hotel','boulangerie','laverie','garage','supermarche','salle_sport','spa','bar','camping','pressing','piscine'];
    if (!allowed.includes(secteur)) return res.status(400).json({ error: 'Secteur non autorise' });
    const villeClean = ville.replace(/[^a-zA-Z0-9\u00C0-\u017F\s\-']/g, '').trim();
    if (!villeClean) return res.status(400).json({ error: 'Ville invalide' });

    const { execSync } = require('child_process');
    const t0 = Date.now();
    let output;
    try {
      output = execSync(`python3 scraper_neon.py "${secteur}" "${villeClean}"`, {
        timeout: 180000, env: { ...process.env, DATABASE_URL: process.env.DATABASE_URL, GOOGLE_PLACES_API_KEY: process.env.GOOGLE_PLACES_API_KEY }
      }).toString();
    } catch(execErr) {
      console.error('Scraper exec error:', execErr.message);
      return res.status(500).json({ error: 'Echec du scraping', detail: execErr.stderr?.toString().substring(0, 500) || execErr.message });
    }

    const duration = Math.round((Date.now() - t0) / 1000);
    const match = output.match(/INSERTED:(\d+)\|SKIPPED:(\d+)/);
    const inserted = match ? parseInt(match[1]) : 0;
    const skipped = match ? parseInt(match[2]) : 0;

    try { await prisma.activityLog.create({ data: { userId: req.user.id, type: 'PROSPECT_CREATED_MANUAL', metadata: { action: 'scrape', secteur, ville: villeClean, inserted, skipped, durationSec: duration } } }); } catch(e) {}

    console.log(`⚡ Scrape ${secteur}/${villeClean}: ${inserted} ajoutes, ${skipped} doublons, ${duration}s`);
    res.json({ success: true, secteur, ville: villeClean, inserted, skipped, duration,
      message: `${inserted} prospect${inserted > 1 ? 's' : ''} ajoute${inserted > 1 ? 's' : ''}, ${skipped} doublon${skipped > 1 ? 's' : ''} en ${duration}s` });
  } catch(e) { console.error('Scraper error:', e); res.status(500).json({ error: e.message }); }
});

// ===== POST /api/admin/scrape-attribuer-preview =====
app.post('/api/admin/scrape-attribuer-preview', verifyToken, isAdminMW, async (req, res) => {
  try {
    const { vendeurId, departements, secteurs } = req.body;
    if (!vendeurId || !departements || !secteurs) return res.status(400).json({ error: 'Params manquants' });
    const villesFR = require('./data/villes-france.json');
    const villes = [];
    for (const dep of departements) { const d = villesFR[dep]; if (d) villes.push(...d.villes.slice(0, 10)); }
    if (!villes.length) return res.json({ success: true, conflits: [], librePool: 0, dejaAuVendeur: 0 });

    const villeFilters = villes.map(v => ({ ville: { contains: v, mode: 'insensitive' } }));
    // Fiches d'autres vendeurs
    const fichesAutres = await prisma.prospect.findMany({
      where: { OR: villeFilters, vendeurId: { not: null, not: vendeurId } },
      select: { id: true, ville: true, vendeurId: true, raisonSociale: true }
    });
    const parVendeur = {};
    fichesAutres.forEach(f => {
      if (!parVendeur[f.vendeurId]) parVendeur[f.vendeurId] = { count: 0, exemples: [] };
      parVendeur[f.vendeurId].count++;
      if (parVendeur[f.vendeurId].exemples.length < 3) parVendeur[f.vendeurId].exemples.push(f.raisonSociale + ' (' + f.ville + ')');
    });
    const vids = Object.keys(parVendeur);
    const infos = vids.length ? await prisma.user.findMany({ where: { id: { in: vids } }, select: { id: true, firstName: true, lastName: true, email: true } }) : [];
    const conflits = infos.map(v => ({ vendeurId: v.id, nom: ((v.firstName || '') + ' ' + (v.lastName || '')).trim() || v.email, email: v.email, count: parVendeur[v.id].count, exemples: parVendeur[v.id].exemples })).sort((a, b) => b.count - a.count);

    const [librePool, dejaAuVendeur] = await Promise.all([
      prisma.prospect.count({ where: { OR: villeFilters, vendeurId: null } }),
      prisma.prospect.count({ where: { OR: villeFilters, vendeurId: vendeurId } }),
    ]);

    res.json({ success: true, conflits, totalConflits: conflits.reduce((s, c) => s + c.count, 0), librePool, dejaAuVendeur, villesScannees: villes.length });
  } catch(e) { console.error('Preview error:', e); res.status(500).json({ error: e.message }); }
});

// ===== POST /api/admin/scrape-attribuer =====
const villesFrance = require('./data/villes-france.json');

app.post('/api/admin/scrape-attribuer', verifyToken, isAdminMW, async (req, res) => {
  try {
    const { vendeurId, departements, secteurs, mode = 'skip' } = req.body;
    console.log('[scrape-attribuer] vendeurId:', vendeurId, 'mode:', mode, 'deps:', departements, 'secteurs:', secteurs);
    if (!vendeurId || vendeurId === 'undefined') return res.status(400).json({ error: 'vendeurId requis' });
    if (!departements?.length) return res.status(400).json({ error: 'Au moins un departement' });
    if (!secteurs?.length) return res.status(400).json({ error: 'Au moins un secteur' });

    const vendeur = await prisma.user.findUnique({ where: { id: vendeurId }, select: { id: true, firstName: true, lastName: true, email: true } });
    if (!vendeur) return res.status(404).json({ error: 'Vendeur introuvable', vendeurIdRecu: vendeurId });

    const allowed = ['restaurant','hotel','boulangerie','laverie','garage','supermarche','salle_sport','spa','bar','camping','pressing','piscine'];
    const secteursOk = secteurs.filter(s => allowed.includes(s));
    if (!secteursOk.length) return res.status(400).json({ error: 'Aucun secteur valide' });

    // Build task list
    const taches = [];
    for (const dep of departements) {
      const data = villesFrance[dep];
      if (!data) continue;
      for (const ville of data.villes.slice(0, 10)) {
        for (const secteur of secteursOk) {
          taches.push({ dep, ville, secteur });
        }
      }
    }
    if (!taches.length) return res.status(400).json({ error: 'Aucune tache generee' });

    console.log(`🚀 Scrape attribue pour ${vendeur.firstName} ${vendeur.lastName}: ${taches.length} taches`);

    const { execSync } = require('child_process');
    const t0 = Date.now();
    let totalInserted = 0, totalSkipped = 0, totalReassigned = 0, errors = [];

    for (let i = 0; i < taches.length; i++) {
      const { ville, secteur } = taches[i];
      try {
        const output = execSync(`python3 scraper_neon.py "${secteur}" "${ville.replace(/"/g, '')}"`, {
          timeout: 90000, env: { ...process.env, DATABASE_URL: process.env.DATABASE_URL, GOOGLE_PLACES_API_KEY: process.env.GOOGLE_PLACES_API_KEY }
        }).toString();
        const match = output.match(/INSERTED:(\d+)\|SKIPPED:(\d+)/);
        const inserted = match ? parseInt(match[1]) : 0;
        const skipped = match ? parseInt(match[2]) : 0;
        totalInserted += inserted;
        totalSkipped += skipped;

        // Attribuer les fiches libres de cette ville/secteur au vendeur
        const assigned = await prisma.prospect.updateMany({
          where: { source: 'BRUTE', vendeurId: null, ville: { contains: ville, mode: 'insensitive' } },
          data: { vendeurId: vendeurId }
        });
        totalReassigned += assigned.count;

        if ((i + 1) % 10 === 0) console.log(`  [${i+1}/${taches.length}] ${ville} ${secteur}: +${inserted}`);
      } catch(err) {
        errors.push({ ville, secteur, error: err.message.substring(0, 100) });
      }
    }

    // Mode 'voler' : reattribuer les fiches d'autres vendeurs
    let totalVolees = 0;
    if (mode === 'voler') {
      for (const dep of departements) {
        const depData = villesFrance[dep]; if (!depData) continue;
        for (const ville of depData.villes.slice(0, 10)) {
          const volees = await prisma.prospect.updateMany({
            where: { ville: { contains: ville, mode: 'insensitive' }, vendeurId: { not: null, not: vendeurId } },
            data: { vendeurId: vendeurId }
          });
          totalVolees += volees.count;
        }
      }
      totalReassigned += totalVolees;
      console.log(`🔄 Mode voler: ${totalVolees} fiches transferees`);
    }

    const duration = Math.round((Date.now() - t0) / 1000);
    try { await prisma.activityLog.create({ data: { userId: req.user.id, type: 'PROSPECT_CREATED_MANUAL', metadata: { action: 'scrape_attribuer', mode, vendeurId, vendeurNom: `${vendeur.firstName} ${vendeur.lastName}`, departements, secteurs: secteursOk, totalInserted, totalReassigned, totalVolees, durationSec: duration } } }); } catch(e) {}

    console.log(`✅ Scrape attribue termine: ${totalInserted} inseres, ${totalReassigned} attribues, ${duration}s`);
    res.json({
      success: true, vendeur: { id: vendeur.id, nom: `${vendeur.firstName} ${vendeur.lastName}` },
      totalInserted, totalSkipped, totalReassigned, duration,
      tachesEffectuees: taches.length - errors.length, tachesEnErreur: errors.length,
      message: `${totalInserted + totalReassigned} fiches attribuees a ${vendeur.firstName} en ${duration}s`
    });
  } catch(e) { console.error('Scrape-attribuer error:', e); res.status(500).json({ error: e.message }); }
});

// ===== STATS BASE (admin) =====
app.get('/api/admin/stats-base', verifyToken, isAdminMW, async (req, res) => {
  try {
    const [bT, bD, pT, pD, mT, vA] = await Promise.all([
      prisma.prospect.count({ where: { source: 'BRUTE' } }),
      prisma.prospect.count({ where: { source: 'BRUTE', vendeurId: null } }),
      prisma.prospect.count({ where: { source: { in: ['PREMIUM', 'PREMIUM_SIGNED'] } } }),
      prisma.prospect.count({ where: { source: { in: ['PREMIUM', 'PREMIUM_SIGNED'] }, vendeurId: null } }),
      prisma.prospect.count({ where: { source: 'MANUELLE' } }),
      prisma.user.count({ where: { isActive: true, role: { in: ['VENDEUR', 'REFERENT'] } } }),
    ]);
    res.json({ success: true,
      brute: { total: bT, disponible: bD, attribuee: bT - bD, pctAttribuee: bT > 0 ? Math.round(((bT - bD) / bT) * 100) : 0 },
      premium: { total: pT, disponible: pD, attribuee: pT - pD, pctAttribuee: pT > 0 ? Math.round(((pT - pD) / pT) * 100) : 0 },
      manuelle: { total: mT }, vendeursActifs: vA
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== GERER VENDEURS (admin) =====
app.get('/api/admin/gerer-vendeurs', verifyToken, isAdminMW, async (req, res) => {
  try {
    const users = await prisma.user.findMany({
      where: { role: { in: ['VENDEUR', 'REFERENT'] } },
      select: { id: true, firstName: true, lastName: true, email: true, phone: true, role: true, isActive: true, lastSeen: true, referentId: true, createdAt: true },
      orderBy: [{ isActive: 'desc' }, { firstName: 'asc' }]
    });
    const enriched = await Promise.all(users.map(async u => {
      const fichesCount = await prisma.prospect.count({ where: { vendeurId: u.id } });
      let referentNom = null;
      if (u.referentId) { const r = await prisma.user.findUnique({ where: { id: u.referentId }, select: { firstName: true, lastName: true } }); if (r) referentNom = `${r.firstName || ''} ${r.lastName || ''}`.trim(); }
      return { id: u.id, nom: `${u.firstName || ''} ${u.lastName || ''}`.trim() || u.email, email: u.email, phone: u.phone, role: u.role, isActive: u.isActive, lastSeen: u.lastSeen, fichesCount, referentNom, createdAt: u.createdAt };
    }));
    res.json({ success: true, users: enriched });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/desactiver-vendeur', verifyToken, isAdminMW, async (req, res) => {
  try {
    const { userId, action, nouveauVendeurId } = req.body;
    if (!userId) return res.status(400).json({ error: 'userId requis' });
    if (!['reattribuer', 'liberer'].includes(action)) return res.status(400).json({ error: 'action invalide' });
    if (action === 'reattribuer' && !nouveauVendeurId) return res.status(400).json({ error: 'nouveauVendeurId requis' });
    const user = await prisma.user.findUnique({ where: { id: userId }, select: { id: true, firstName: true, lastName: true, email: true, isActive: true } });
    if (!user) return res.status(404).json({ error: 'User introuvable' });
    if (!user.isActive) return res.status(400).json({ error: 'Deja desactive' });
    if (user.id === req.user.id) return res.status(400).json({ error: 'Impossible de se desactiver soi-meme' });

    const fichesCount = await prisma.prospect.count({ where: { vendeurId: userId } });
    let reassigned = 0, liberees = 0;

    if (action === 'reattribuer' && fichesCount > 0) {
      const nv = await prisma.user.findUnique({ where: { id: nouveauVendeurId }, select: { id: true, isActive: true } });
      if (!nv || !nv.isActive) return res.status(400).json({ error: 'Nouveau vendeur invalide' });
      const r = await prisma.prospect.updateMany({ where: { vendeurId: userId }, data: { vendeurId: nouveauVendeurId } });
      reassigned = r.count;
    } else if (action === 'liberer' && fichesCount > 0) {
      const r = await prisma.prospect.updateMany({ where: { vendeurId: userId }, data: { vendeurId: null } });
      liberees = r.count;
    }

    await prisma.user.update({ where: { id: userId }, data: { isActive: false } });
    try { await prisma.activityLog.create({ data: { userId: req.user.id, type: 'PROSPECT_REASSIGNED', metadata: { action: 'desactiver_vendeur', vendeurId: userId, vendeurNom: `${user.firstName} ${user.lastName}`, mode: action, nouveauVendeurId, fichesCount, reassigned, liberees } } }); } catch(e) {}

    res.json({ success: true, message: `${user.firstName} desactive. ${reassigned} reattribuees, ${liberees} liberees.`, reassigned, liberees, fichesCount });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/reactiver-vendeur', verifyToken, isAdminMW, async (req, res) => {
  try {
    const { userId } = req.body;
    if (!userId) return res.status(400).json({ error: 'userId requis' });
    const user = await prisma.user.findUnique({ where: { id: userId }, select: { id: true, firstName: true, isActive: true } });
    if (!user) return res.status(404).json({ error: 'User introuvable' });
    if (user.isActive) return res.status(400).json({ error: 'Deja actif' });
    await prisma.user.update({ where: { id: userId }, data: { isActive: true } });
    res.json({ success: true, message: `${user.firstName} reactive. Ses anciennes fiches ne sont pas restaurees.` });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== ACTIVITE GLOBALE (admin) =====
app.get('/api/admin/activite-globale', verifyToken, isAdminMW, async (req, res) => {
  try {
    const today = new Date(); today.setHours(0, 0, 0, 0);
    const weekStart = new Date(today); const wd2 = weekStart.getDay() || 7; weekStart.setDate(weekStart.getDate() - wd2 + 1);
    const users = await prisma.user.findMany({ where: { isActive: true }, select: { id: true, firstName: true, lastName: true, email: true, role: true, lastSeen: true } });

    const enriched = await Promise.all(users.map(async u => {
      const [sT, sW] = await Promise.all([
        prisma.workSession.findMany({ where: { userId: u.id, date: today }, select: { startedAt: true, endedAt: true, durationMinutes: true } }),
        prisma.workSession.findMany({ where: { userId: u.id, date: { gte: weekStart } }, select: { startedAt: true, endedAt: true, durationMinutes: true } }),
      ]);
      let mT = 0; for (const s of sT) { if (s.durationMinutes) mT += s.durationMinutes; else if (!s.endedAt) mT += Math.round((Date.now() - new Date(s.startedAt).getTime()) / 60000); }
      let mW = 0; for (const s of sW) { if (s.durationMinutes) mW += s.durationMinutes; else if (!s.endedAt) mW += Math.round((Date.now() - new Date(s.startedAt).getTime()) / 60000); }
      const age = u.lastSeen ? Date.now() - new Date(u.lastSeen).getTime() : Infinity;
      return { id: u.id, nom: `${u.firstName || ''} ${u.lastName || ''}`.trim() || u.email, email: u.email, role: u.role, minutesToday: mT, minutesWeek: mW, connectionStatus: age < 60000 ? 'active' : age < 15 * 60 * 1000 ? 'idle' : 'offline' };
    }));

    const connectedNow = enriched.filter(u => u.connectionStatus === 'active').length;
    const avgMin = enriched.length > 0 ? Math.round(enriched.reduce((s, u) => s + u.minutesToday, 0) / enriched.length) : 0;
    const top = [...enriched].sort((a, b) => b.minutesToday - a.minutesToday)[0];

    res.json({ success: true, stats: { totalUsers: enriched.length, connectedNow, avgMinutesToday: avgMin, topUser: top ? { nom: top.nom, minutes: top.minutesToday } : null }, users: enriched.sort((a, b) => b.minutesToday - a.minutesToday) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/admin/user/:id/activite', verifyToken, isAdminMW, async (req, res) => {
  try {
    const uid = req.params.id; const range = req.query.range || 'day';
    const u = await prisma.user.findUnique({ where: { id: uid }, select: { id: true, firstName: true, lastName: true, email: true, role: true, lastSeen: true } });
    if (!u) return res.status(404).json({ error: 'User introuvable' });
    let since = new Date();
    if (range === 'day') since.setHours(0, 0, 0, 0);
    else if (range === 'week') { since.setHours(0, 0, 0, 0); const d = since.getDay() || 7; since.setDate(since.getDate() - d + 1); }
    else since = new Date(since.getFullYear(), since.getMonth(), 1);
    const sessions = await prisma.workSession.findMany({ where: { userId: uid, date: { gte: since } }, orderBy: { startedAt: 'desc' } });
    let totalMin = 0; for (const s of sessions) { if (s.durationMinutes) totalMin += s.durationMinutes; else if (!s.endedAt) totalMin += Math.round((Date.now() - new Date(s.startedAt).getTime()) / 60000); }
    res.json({ success: true, user: { nom: `${u.firstName || ''} ${u.lastName || ''}`.trim(), email: u.email, role: u.role, lastSeen: u.lastSeen }, range, totalMinutes: totalMin, sessionsCount: sessions.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== PREMIUM ATTRIBUTION =====
app.get('/api/admin/premium/stats', verifyToken, isAdminMW, async (req, res) => {
  try {
    const [statuts, scoreS, dateS, libres, attribuees] = await Promise.all([
      prisma.prospect.groupBy({ by: ['statutOhm'], where: { source: { in: ['PREMIUM', 'PREMIUM_SIGNED'] } }, _count: true, orderBy: { _count: { statutOhm: 'desc' } } }),
      prisma.prospect.aggregate({ where: { source: { in: ['PREMIUM', 'PREMIUM_SIGNED'] }, score: { not: null } }, _min: { score: true }, _max: { score: true }, _avg: { score: true } }),
      prisma.prospect.aggregate({ where: { source: { in: ['PREMIUM', 'PREMIUM_SIGNED'] }, dateDebutLivraison: { not: null } }, _min: { dateDebutLivraison: true }, _max: { dateDebutLivraison: true } }),
      prisma.prospect.count({ where: { source: { in: ['PREMIUM', 'PREMIUM_SIGNED'] }, vendeurId: null } }),
      prisma.prospect.count({ where: { source: { in: ['PREMIUM', 'PREMIUM_SIGNED'] }, vendeurId: { not: null } } }),
    ]);
    res.json({ success: true, statuts: statuts.map(s => ({ statut: s.statutOhm, count: s._count })), scoreMin: scoreS._min.score, scoreMax: scoreS._max.score, scoreAvg: scoreS._avg.score ? parseFloat(scoreS._avg.score.toFixed(1)) : 0, dateMin: dateS._min.dateDebutLivraison, dateMax: dateS._max.dateDebutLivraison, libres, attribuees, total: libres + attribuees });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/premium/search', verifyToken, isAdminMW, async (req, res) => {
  try {
    const { statutsOhm, scoreMin, scoreMax, dateDebutMin, dateDebutMax, attribuesAussi, page = 1, pageSize = 50 } = req.body;
    const where = { source: { in: ['PREMIUM', 'PREMIUM_SIGNED'] } };
    if (statutsOhm?.length) where.statutOhm = { in: statutsOhm };
    if (scoreMin !== undefined && scoreMin !== null) { where.score = { ...(where.score || {}), gte: parseFloat(scoreMin) }; }
    if (scoreMax !== undefined && scoreMax !== null) { where.score = { ...(where.score || {}), lte: parseFloat(scoreMax) }; }
    if (dateDebutMin) { where.dateDebutLivraison = { ...(where.dateDebutLivraison || {}), gte: new Date(dateDebutMin) }; }
    if (dateDebutMax) { where.dateDebutLivraison = { ...(where.dateDebutLivraison || {}), lte: new Date(dateDebutMax + 'T23:59:59') }; }
    if (!attribuesAussi) where.vendeurId = null;

    const [total, fiches] = await Promise.all([
      prisma.prospect.count({ where }),
      prisma.prospect.findMany({ where, orderBy: { score: 'desc' }, skip: (page - 1) * pageSize, take: pageSize, select: { id: true, raisonSociale: true, ville: true, codePostal: true, score: true, statutOhm: true, dateDebutLivraison: true, dateFinLivraison: true, vendeurId: true, signataire: true, telephone: true, nbPdl: true } }),
    ]);
    // Enrichir vendeur noms
    const vids = [...new Set(fiches.map(f => f.vendeurId).filter(Boolean))];
    const vMap = {};
    if (vids.length) { const vs = await prisma.user.findMany({ where: { id: { in: vids } }, select: { id: true, firstName: true, lastName: true } }); vs.forEach(v => { vMap[v.id] = ((v.firstName || '') + ' ' + (v.lastName || '')).trim(); }); }

    res.json({ success: true, total, page, pageSize, totalPages: Math.ceil(total / pageSize), fiches: fiches.map(f => ({ ...f, vendeurNom: f.vendeurId ? (vMap[f.vendeurId] || '?') : null })) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/premium/attribuer', verifyToken, isAdminMW, async (req, res) => {
  try {
    const { vendeurId, prospectIds, mode = 'skip' } = req.body;
    if (!vendeurId) return res.status(400).json({ error: 'vendeurId requis' });
    if (!prospectIds?.length) return res.status(400).json({ error: 'prospectIds requis' });
    const vendeur = await prisma.user.findUnique({ where: { id: vendeurId }, select: { id: true, firstName: true, lastName: true } });
    if (!vendeur) return res.status(404).json({ error: 'Vendeur introuvable' });

    const where = { id: { in: prospectIds } };
    if (mode === 'skip') where.vendeurId = null;
    const result = await prisma.prospect.updateMany({ where, data: { vendeurId } });

    try { await prisma.activityLog.create({ data: { userId: req.user.id, type: 'ATTRIBUTION', metadata: { action: 'attribution_premium', vendeurId, vendeurNom: `${vendeur.firstName} ${vendeur.lastName}`, count: result.count, mode } } }); } catch(e) {}

    res.json({ success: true, attribuees: result.count, skipped: prospectIds.length - result.count, mode, vendeurNom: `${vendeur.firstName} ${vendeur.lastName}`, message: `${result.count} fiches attribuees a ${vendeur.firstName}` });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== STATIC =====
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

app.listen(port, () => {
  console.log(`🚀 LILIWATT Prospection sur http://localhost:${port}`);
});
