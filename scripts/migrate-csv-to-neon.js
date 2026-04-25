/**
 * MIGRATION CSV → NEON
 *
 * Usage :
 *   node scripts/migrate-csv-to-neon.js --dry-run   (test sans ecrire)
 *   node scripts/migrate-csv-to-neon.js             (import reel)
 *   node scripts/migrate-csv-to-neon.js --reset     (vide la table avant import)
 *
 * Source : ~/Desktop/migration-prospection/{base-brute,leads-ohm}.csv
 * Cible : Neon PostgreSQL (table Prospect)
 */

require('dotenv').config()
const { PrismaClient } = require('@prisma/client')
const fs = require('fs')
const path = require('path')
const os = require('os')
const { parse } = require('csv-parse/sync')

const prisma = new PrismaClient()

const DRY_RUN = process.argv.includes('--dry-run')
const RESET = process.argv.includes('--reset')

const HOME = os.homedir()
const BASE_BRUTE_PATH = path.join(HOME, 'Desktop', 'migration-prospection', 'base-brute.csv')
const LEADS_OHM_PATH = path.join(HOME, 'Desktop', 'migration-prospection', 'leads-ohm.csv')

console.log('\n========================================')
console.log('MIGRATION CSV → NEON')
console.log('========================================')
console.log(`Mode : ${DRY_RUN ? '🧪 DRY-RUN (aucune ecriture)' : '💾 IMPORT REEL'}`)
if (RESET) console.log('⚠️  RESET active : table Prospect sera videe avant import')
console.log('')

// =======================
// HELPERS
// =======================

function extractCodePostal(adresse) {
  if (!adresse) return null
  const m = adresse.match(/\b(\d{5})\b/)
  return m ? m[1] : null
}

function parseDate(str) {
  if (!str || typeof str !== 'string') return null
  const s = str.trim()
  if (!s) return null

  // DD/MM/YYYY
  let m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/)
  if (m) {
    const [, d, mo, y] = m
    const date = new Date(`${y}-${mo.padStart(2, '0')}-${d.padStart(2, '0')}T00:00:00Z`)
    return isNaN(date.getTime()) ? null : date
  }

  // MM/YYYY
  m = s.match(/^(\d{1,2})\/(\d{4})$/)
  if (m) {
    const [, mo, y] = m
    const date = new Date(`${y}-${mo.padStart(2, '0')}-01T00:00:00Z`)
    return isNaN(date.getTime()) ? null : date
  }

  // YYYY-MM-DD
  m = s.match(/^(\d{4})-(\d{2})-(\d{2})/)
  if (m) {
    const date = new Date(`${m[0]}T00:00:00Z`)
    return isNaN(date.getTime()) ? null : date
  }

  // Excel serial number (entier > 25000)
  const num = parseInt(s, 10)
  if (!isNaN(num) && num > 25000 && num < 80000) {
    const date = new Date((num - 25569) * 86400 * 1000)
    return isNaN(date.getTime()) ? null : date
  }

  return null
}

function parseFloat2(v) {
  if (v === null || v === undefined || v === '') return null
  const n = parseFloat(String(v).replace(',', '.'))
  return isNaN(n) ? null : n
}

function parseInt2(v) {
  if (v === null || v === undefined || v === '') return null
  const n = parseInt(String(v), 10)
  return isNaN(n) ? null : n
}

function clean(v) {
  if (v === null || v === undefined) return null
  const s = String(v).trim()
  return s === '' || s === '-' ? null : s
}

function mapStatut(s) {
  if (!s) return null
  const t = String(s).trim().toLowerCase()
  if (t.includes('pas interess') || t.includes('pas intéress')) return 'PAS_INTERESSE'
  if (t.includes('a appeler') || t.includes('à appeler')) return 'A_APPELER'
  if (t === 'appele' || t === 'appelé') return 'APPELE'
  if (t.includes('intéress') || t.includes('interess')) return 'INTERESSE'
  if (t.includes('attente') && t.includes('document')) return 'ATTENTE_DOCUMENTS'
  if (t.includes('dossier') && t.includes('recu')) return 'DOSSIER_RECU'
  if (t.includes('dossier') && t.includes('reçu')) return 'DOSSIER_RECU'
  if (t.includes('rappeler')) return 'A_RAPPELER'
  if (t.includes('faux')) return 'FAUX_NUMERO'
  if (t.includes('repond pas') || t.includes('répond pas') || t === 'nrp') return 'NE_REPOND_PAS'
  if (t.includes('signe') || t.includes('signé')) return 'CLIENT_SIGNE'
  return null
}

// =======================
// CHARGEMENT DES USERS
// =======================

async function loadUserMap() {
  const users = await prisma.user.findMany({
    select: { id: true, email: true }
  })
  const map = new Map()
  for (const u of users) {
    map.set(u.email.toLowerCase(), u.id)
  }
  console.log(`👥 ${users.length} users charges depuis Neon`)
  return map
}

// =======================
// IMPORT BASE BRUTE
// =======================

async function importBaseBrute(userMap) {
  console.log('\n--- BASE BRUTE ---')
  if (!fs.existsSync(BASE_BRUTE_PATH)) {
    console.log(`❌ Fichier introuvable : ${BASE_BRUTE_PATH}`)
    return { imported: 0, skipped: 0, errors: 0 }
  }

  const csv = fs.readFileSync(BASE_BRUTE_PATH, 'utf8')
  const rows = parse(csv, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
    bom: true
  })
  console.log(`📥 ${rows.length} lignes lues depuis le CSV`)

  let imported = 0, skipped = 0, errors = 0
  const missingVendeurs = new Set()
  const batch = []

  for (const row of rows) {
    try {
      const placeId = clean(row.place_id)
      if (!placeId) { skipped++; continue }

      const raisonSociale = clean(row.raison_sociale)
      if (!raisonSociale) { skipped++; continue }

      // Mapping vendeur
      let vendeurId = null
      const emailRaw = clean(row.vendeur_attribue)
      if (emailRaw && emailRaw !== 'HORS_POOL' && emailRaw.includes('@')) {
        vendeurId = userMap.get(emailRaw.toLowerCase()) || null
        if (!vendeurId) missingVendeurs.add(emailRaw)
      }

      const adresse = clean(row.adresse)
      const codePostal = extractCodePostal(adresse)

      const data = {
        source: 'BRUTE',
        placeId,
        raisonSociale,
        adresse,
        ville: clean(row.ville),
        codePostal,
        secteur: clean(row.secteur),
        telephone: clean(row.telephone),
        email: clean(row.email_prospect),
        siteWeb: clean(row.site_web),
        noteGoogle: parseFloat2(row.note_google),
        nbAvis: parseInt2(row.nb_avis),
        vendeurId,
        statutAppel: mapStatut(row.statut_appel),
        noteAppel: clean(row.note_appel),
        dateRappel: parseDate(row.date_rappel),
        rgpdEnvoye: clean(row.rgpd_envoye)?.toLowerCase() === 'oui',
        emailEnvoyeA: clean(row.email_envoye_a),
        dateDernierAppel: parseDate(row.date_dernier_appel)
      }

      batch.push(data)
      imported++
    } catch (e) {
      errors++
      if (errors <= 5) console.log(`\n  ❌ Erreur ligne : ${e.message}`)
    }
  }

  // Batch insert BASE BRUTE (upsert by placeId via createMany + skipDuplicates)
  if (!DRY_RUN && batch.length > 0) {
    const BATCH_SIZE = 200
    for (let i = 0; i < batch.length; i += BATCH_SIZE) {
      const chunk = batch.slice(i, i + BATCH_SIZE)
      await prisma.prospect.createMany({ data: chunk, skipDuplicates: true })
      process.stdout.write(`\r  → ${Math.min(i + BATCH_SIZE, batch.length)}/${batch.length} inseres...`)
    }
  }

  console.log(`\n✅ BASE BRUTE : ${imported} importes, ${skipped} ignores, ${errors} erreurs`)
  if (missingVendeurs.size > 0) {
    console.log(`⚠️  Vendeurs introuvables (fiches gardees sans attribution) :`)
    for (const e of missingVendeurs) console.log(`     - ${e}`)
  }
  return { imported, skipped, errors }
}

// =======================
// IMPORT LEADS OHM
// =======================

async function importLeadsOhm(userMap) {
  console.log('\n--- LEADS OHM ---')
  if (!fs.existsSync(LEADS_OHM_PATH)) {
    console.log(`❌ Fichier introuvable : ${LEADS_OHM_PATH}`)
    return { imported: 0, skipped: 0, errors: 0, signed: 0 }
  }

  const csv = fs.readFileSync(LEADS_OHM_PATH, 'utf8')
  const rows = parse(csv, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
    bom: true
  })
  console.log(`📥 ${rows.length} lignes lues depuis le CSV`)

  let imported = 0, skipped = 0, errors = 0, signed = 0
  const missingVendeurs = new Set()
  const seenKeys = new Set()
  const batch = []

  for (const row of rows) {
    try {
      const raisonSociale = clean(row.raison_sociale)
      if (!raisonSociale) { skipped++; continue }

      const siren = clean(row.siren)
      const tel = clean(row.tel_signataire)

      // Cle de dedup : siren + tel + raison sociale
      const dedupKey = `${siren || 'nosiren'}::${tel || 'notel'}::${raisonSociale.toLowerCase()}`
      if (seenKeys.has(dedupKey)) { skipped++; continue }
      seenKeys.add(dedupKey)

      // Statut OHM → determine la source
      const statutOhm = clean(row.statut)
      const isSigned = statutOhm && statutOhm.toLowerCase().includes('finalis')
      const source = isSigned ? 'PREMIUM_SIGNED' : 'PREMIUM'

      // Mapping vendeur
      let vendeurId = null
      const emailRaw = clean(row.vendeur_attribue)
      if (emailRaw && emailRaw !== 'HORS_POOL' && emailRaw.includes('@')) {
        vendeurId = userMap.get(emailRaw.toLowerCase()) || null
        if (!vendeurId) missingVendeurs.add(emailRaw)
      }

      const adresse = clean(row.adresse)
      const codePostal = extractCodePostal(adresse)

      const data = {
        source,
        siren,
        raisonSociale,
        adresse,
        codePostal,
        signataire: clean(row.signataire),
        email: clean(row.email_signataire),
        telephone: tel,
        score: parseFloat2(row.score),
        payRank: parseInt2(row.pay_rank),
        volumeTotal: parseFloat2(row.volume_total),
        dateDebutLivraison: parseDate(row.date_debut_livraison),
        dateFinLivraison: parseDate(row.date_fin_livraison),
        energie: clean(row.energie),
        segment: clean(row.segments),
        statutOhm,
        pdls: clean(row.pdls),
        nbPdl: parseInt2(row.nb_pdl) || parseInt2(row.nbr_sites),
        margeCourtier: parseFloat2(row.marge_courtier),
        courtierFinal: clean(row.courtier_final),
        vendeurId,
        statutAppel: mapStatut(row.statut_appel),
        noteAppel: clean(row.note_appel),
        dateRappel: parseDate(row.date_rappel),
        rgpdEnvoye: clean(row.rgpd_envoye)?.toLowerCase() === 'oui'
      }

      batch.push(data)
      imported++
      if (isSigned) signed++
    } catch (e) {
      errors++
      if (errors <= 5) console.log(`\n  ❌ Erreur ligne : ${e.message}`)
    }
  }

  // Batch insert LEADS OHM
  if (!DRY_RUN && batch.length > 0) {
    const BATCH_SIZE = 500
    for (let i = 0; i < batch.length; i += BATCH_SIZE) {
      const chunk = batch.slice(i, i + BATCH_SIZE)
      await prisma.prospect.createMany({ data: chunk, skipDuplicates: true })
      process.stdout.write(`\r  → ${Math.min(i + BATCH_SIZE, batch.length)}/${batch.length} inseres...`)
    }
  }

  console.log(`\n✅ LEADS OHM : ${imported} importes (dont ${signed} PREMIUM_SIGNED), ${skipped} ignores, ${errors} erreurs`)
  if (missingVendeurs.size > 0) {
    console.log(`⚠️  Vendeurs introuvables :`)
    for (const e of missingVendeurs) console.log(`     - ${e}`)
  }
  return { imported, skipped, errors, signed }
}

// =======================
// MAIN
// =======================

async function main() {
  try {
    const userMap = await loadUserMap()

    if (RESET && !DRY_RUN) {
      console.log('\n🗑  Reset table Prospect...')
      const deleted = await prisma.prospect.deleteMany({})
      console.log(`   ${deleted.count} prospects supprimes`)
    }

    const t0 = Date.now()

    const r1 = await importBaseBrute(userMap)
    const r2 = await importLeadsOhm(userMap)

    const dt = Math.round((Date.now() - t0) / 1000)

    console.log('\n========================================')
    console.log('RAPPORT FINAL')
    console.log('========================================')
    console.log(`BASE BRUTE  : ${r1.imported} importes, ${r1.skipped} ignores, ${r1.errors} erreurs`)
    console.log(`LEADS OHM   : ${r2.imported} importes (${r2.signed} signes), ${r2.skipped} ignores, ${r2.errors} erreurs`)
    console.log(`TOTAL       : ${r1.imported + r2.imported} prospects`)
    console.log(`Duree       : ${dt}s`)
    console.log('')

    if (DRY_RUN) {
      console.log('🧪 DRY-RUN termine. Aucune ecriture en base.')
      console.log('   Pour lancer l\'import reel : node scripts/migrate-csv-to-neon.js')
    } else {
      console.log('💾 Import termine.')

      const finalCount = await prisma.prospect.count()
      const bySource = await prisma.prospect.groupBy({
        by: ['source'],
        _count: true
      })
      console.log(`\n📊 Etat final dans Neon :`)
      console.log(`   Total : ${finalCount}`)
      bySource.forEach(s => {
        console.log(`   ${s.source} : ${s._count}`)
      })
    }
  } catch (e) {
    console.error('\n❌ ERREUR FATALE :', e.message)
    console.error(e.stack)
    process.exit(1)
  } finally {
    await prisma.$disconnect()
  }
}

main()
