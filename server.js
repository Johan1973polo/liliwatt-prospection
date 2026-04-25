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
      // Referent voit ses vendeurs + libres
      const mesVendeurs = await prisma.user.findMany({ where: { referentId: userId, isActive: true }, select: { id: true } });
      const ids = mesVendeurs.map(v => v.id);
      ids.push(userId);
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
    if (fSearch) where.raisonSociale = { contains: fSearch, mode: 'insensitive' };

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
    const items = await prisma.prospect.findMany({
      where: { source: { in: ['PREMIUM', 'PREMIUM_SIGNED'] }, vendeurId: userId },
      orderBy: [{ dateFinLivraison: 'asc' }],
      take: 200
    });

    const prospects = items.map(p => ({
      _row: p.id, _sheet: 'LEADS OHM', _attribue: true,
      raison_sociale: p.raisonSociale, siren: p.siren || '',
      signataire: p.signataire || '', email_signataire: p.email || '',
      tel_signataire: p.telephone || '', adresse: p.adresse || '',
      score: p.score || '', pay_rank: p.payRank || '', observation_pay_rank: '',
      date_fin_livraison: p.dateFinLivraison ? new Date(p.dateFinLivraison).toLocaleDateString('fr-FR') : '',
      volume_total: p.volumeTotal || '',
      segments: p.segment || '', energie: p.energie || '',
      statut_appel: p.statutAppel || '', note_appel: p.noteAppel || '',
      vendeur_attribue: req.user.email
    }));

    console.log(`💎 LEADS pour ${req.user.email}: ${prospects.length} fiches`);
    res.json({ success: true, prospects });
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

    console.log(`📞 ${prospect.raisonSociale}: ${statutEnum} par ${req.user.email}`);
    res.json({ success: true });
  } catch(e) { console.error('Statut error:', e); res.status(500).json({ error: e.message }); }
});

// ===== POST /api/prospects/mail/:id (Neon — RGPD email) =====
app.post('/api/prospects/mail/:id', verifyToken, async (req, res) => {
  try {
    const { email_destinataire, nom_gerant } = req.body;
    if (!email_destinataire) return res.status(400).json({ error: 'Email requis' });

    const vendeurUser = await prisma.user.findUnique({
      where: { id: req.user.id },
      include: { credentials: { where: { serviceName: 'ZOHO' }, select: { login: true, passwordEncrypted: true } } }
    });
    const zohoLogin = vendeurUser?.credentials[0]?.login || req.user.email;
    const zohoPass = vendeurUser?.credentials[0]?.passwordEncrypted || '';
    if (!zohoPass) return res.status(400).json({ error: 'Credentials Zoho non configurees' });

    const rgpdLink = `https://liliwatt-courtier.onrender.com/rgpd/${req.user.token_rgpd}`;
    const vendeurNom = `${req.user.prenom} ${req.user.nom_famille}`;

    const html = `<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;"><div style="background:linear-gradient(135deg,#1e1b4b,#7c3aed);padding:28px;border-radius:12px 12px 0 0;text-align:center;"><h1 style="color:#fff;font-size:26px;font-weight:800;letter-spacing:3px;margin:0;">LILIWATT</h1><p style="color:rgba(255,255,255,.7);font-size:11px;margin:4px 0 0;text-transform:uppercase;letter-spacing:1px;">Courtage Energie B2B & B2C</p></div><div style="background:#f5f3ff;padding:32px;border-radius:0 0 12px 12px;"><p style="font-size:15px;color:#1e1b4b;">Bonjour${nom_gerant ? ' ' + nom_gerant : ''},</p><p style="color:#374151;line-height:1.7;">Suite a notre entretien telephonique, je me permets de vous transmettre ce lien afin de realiser votre etude energetique <strong>gratuite et sans engagement</strong>.</p><p style="color:#374151;line-height:1.7;">Merci de bien vouloir nous faire parvenir :</p><ul style="color:#374151;line-height:2;"><li>Une <strong>facture hiver</strong> et une <strong>facture ete</strong> d'electricite</li><li>Si vous consommez du gaz, une <strong>facture de gaz</strong></li></ul><div style="text-align:center;margin:28px 0;"><a href="${rgpdLink}" style="display:inline-block;background:linear-gradient(135deg,#7c3aed,#d946ef);color:#fff;padding:16px 40px;border-radius:50px;text-decoration:none;font-weight:700;font-size:15px;">Transmettre mes factures</a></div><p style="color:#374151;">Je reste a votre disposition.</p><p style="color:#374151;">Cordialement,</p><div style="margin-top:20px;padding-top:16px;border-top:2px solid #e9d5ff;"><strong style="color:#1e1b4b;">${vendeurNom}</strong><br><span style="color:#7c3aed;font-size:12px;">LILIWATT — Courtage Energie</span><br><span style="font-size:12px;color:#6b7280;">${req.user.email}</span></div></div></div>`;

    const transporter = nodemailer.createTransport({ host: 'smtp.zoho.eu', port: 465, secure: true, auth: { user: zohoLogin, pass: zohoPass } });
    await transporter.sendMail({ from: `"${vendeurNom} — LILIWATT" <${zohoLogin}>`, to: email_destinataire, subject: 'Suite a notre entretien — Etude energetique LILIWATT', html });

    await prisma.prospect.update({ where: { id: req.params.id }, data: { rgpdEnvoye: true, emailEnvoyeA: email_destinataire } });
    await prisma.activityLog.create({ data: { userId: req.user.id, prospectId: req.params.id, type: 'RGPD_SENT', metadata: { to: email_destinataire } } });

    console.log(`📧 RGPD envoye a ${email_destinataire} par ${req.user.email}`);
    res.json({ success: true });
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

// ===== POST /api/prospects/manuelle (Neon) =====
app.post('/api/prospects/manuelle', verifyToken, async (req, res) => {
  try {
    const userId = req.user.id;
    const { raisonSociale, telephone, source: sourceManuelle, nomContact, prenomContact, email, siren, adresse, notes } = req.body;
    if (!raisonSociale || !telephone || !sourceManuelle) return res.status(400).json({ error: 'raisonSociale, telephone et source obligatoires' });

    let codePostal = null;
    if (adresse) { const m = adresse.match(/\b(\d{5})\b/); if (m) codePostal = m[1]; }
    const signataire = [prenomContact, nomContact].filter(Boolean).join(' ').trim() || null;

    const prospect = await prisma.prospect.create({
      data: {
        source: 'MANUELLE', raisonSociale: raisonSociale.trim(), telephone: telephone.trim(),
        email: email?.trim() || null, siren: siren?.trim() || null, adresse: adresse?.trim() || null,
        codePostal, signataire, sourceManuelle: sourceManuelle.trim(), noteAppel: notes?.trim() || null,
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
      where: { role: 'VENDEUR', isActive: true },
      select: { id: true, email: true, firstName: true, lastName: true }
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
      where: { role: 'VENDEUR', isActive: true },
      select: { id: true, email: true, firstName: true, lastName: true, _count: { select: { prospectsAttribues: true } } },
      orderBy: { firstName: 'asc' }
    });
    res.json({ success: true, vendeurs: vendeurs.map(v => ({
      email: v.email, prenom: v.firstName || '', nom: v.lastName || '',
      role: 'vendeur', nbAttribues: v._count.prospectsAttribues
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

// ===== POST /api/admin/scraper (DESACTIVE — Phase 4) =====
app.post('/api/admin/scraper', verifyToken, isAdminMW, async (req, res) => {
  return res.status(503).json({
    error: 'Module scraper en maintenance',
    message: 'Le scraper Google Places sera reactive dans la prochaine version.'
  });
});

// ===== STATIC =====
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

app.listen(port, () => {
  console.log(`🚀 LILIWATT Prospection sur http://localhost:${port}`);
});
