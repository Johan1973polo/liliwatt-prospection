/**
 * MIGRATION DES MOTS DE PASSE → BCRYPT
 * Lit la Google Sheet MDP, hash chaque mdp en bcrypt, MAJ User dans Neon.
 * Idempotent : skip les users deja hashes.
 */
require('dotenv').config()
const { PrismaClient } = require('@prisma/client')
const { google } = require('googleapis')
const bcrypt = require('bcryptjs')

const prisma = new PrismaClient()
const SHEETS_MDP_ID = '11gVGMBtqMUhPh70yjMgjW-yLDht6fO0KqWJAF53ASXk'
const SALT_ROUNDS = 10
const DRY_RUN = process.argv.includes('--dry-run')

async function getSheetsAuth() {
  const credsBase64 = process.env.GOOGLE_DRIVE_CREDS_BASE64
  let creds
  if (credsBase64) {
    creds = JSON.parse(Buffer.from(credsBase64, 'base64').toString('utf8'))
  } else {
    const fs = require('fs')
    const path = require('path')
    creds = JSON.parse(fs.readFileSync(path.join(__dirname, '..', '..', 'courtier-energie', 'liliwatt-drive-credentials.json'), 'utf8'))
  }
  return new google.auth.GoogleAuth({ credentials: creds, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] })
}

async function main() {
  console.log('\n========================================')
  console.log('MIGRATION MOTS DE PASSE → BCRYPT')
  console.log(`Mode : ${DRY_RUN ? 'DRY-RUN' : 'IMPORT REEL'}`)
  console.log('========================================\n')

  const auth = await getSheetsAuth()
  const sheets = google.sheets({ version: 'v4', auth })
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: SHEETS_MDP_ID, range: 'A:K' })
  const rows = res.data.values || []
  console.log(`Lignes lues : ${rows.length}`)

  // Colonnes connues : 0=nom_famille, 1=prenom, 2=mdp, 3=email, 9=role, 10=statut
  let updated = 0, skipped = 0, notFound = 0, alreadyHashed = 0

  for (let i = 1; i < rows.length; i++) {
    const row = rows[i]
    const email = (row[3] || '').trim().toLowerCase()
    const mdp = (row[2] || '').trim()
    if (!email || !mdp) { skipped++; continue }

    const user = await prisma.user.findUnique({ where: { email } })
    if (!user) {
      console.log(`  ⚠️  User introuvable : ${email}`)
      notFound++
      continue
    }

    if (user.passwordHash && user.passwordHash.startsWith('$2')) {
      alreadyHashed++
      continue
    }

    const hash = await bcrypt.hash(mdp, SALT_ROUNDS)
    if (!DRY_RUN) {
      await prisma.user.update({ where: { id: user.id }, data: { passwordHash: hash } })
    }
    console.log(`  ✅ ${email} → mdp hashe`)
    updated++
  }

  console.log('\n========================================')
  console.log('RAPPORT')
  console.log('========================================')
  console.log(`Mis a jour       : ${updated}`)
  console.log(`Deja hashes      : ${alreadyHashed}`)
  console.log(`Non trouves      : ${notFound}`)
  console.log(`Lignes vides     : ${skipped}`)
  await prisma.$disconnect()
}

main().catch(e => { console.error(e); process.exit(1) })
