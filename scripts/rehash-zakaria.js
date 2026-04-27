// Rehash password de Zakaria pour aligner DB Neon sur Sheet MDP ZOHO
// Usage: node scripts/rehash-zakaria.js
// Confirmation interactive requise avant update.

require('dotenv').config()
const { PrismaClient } = require('@prisma/client')
const bcrypt = require('bcryptjs')
const readline = require('readline')

const SALT_ROUNDS = 10
const TARGET_EMAIL = 'zakaria.jamaoui@liliwatt.fr'
// Password source de verite = Sheet MDP ZOHO (non affiche en clair)
const NEW_PASSWORD = 'ep45P#6nU$g'

const p = new PrismaClient()

function ask(question) {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout })
  return new Promise(resolve => rl.question(question, answer => { rl.close(); resolve(answer.trim()) }))
}

async function main() {
  console.log('\n=== REHASH PASSWORD — zakaria.jamaoui@liliwatt.fr ===\n')

  // 1. Chercher le user
  const user = await p.user.findUnique({
    where: { email: TARGET_EMAIL },
    select: { id: true, email: true, role: true, createdAt: true, passwordHash: true }
  })

  if (!user) {
    console.error('❌ User non trouve:', TARGET_EMAIL)
    await p.$disconnect()
    process.exit(1)
  }

  // 2. Afficher AVANT
  console.log('AVANT:')
  console.log('  id:', user.id)
  console.log('  email:', user.email)
  console.log('  role:', user.role)
  console.log('  createdAt:', user.createdAt?.toISOString()?.substring(0, 19))
  console.log('  hash prefix:', user.passwordHash?.substring(0, 15))
  console.log('  password: ***')

  // 3. Hasher le nouveau password
  const newHash = await bcrypt.hash(NEW_PASSWORD, SALT_ROUNDS)

  // 4. Confirmation interactive
  const answer = await ask('\nConfirmer le rehash pour ' + TARGET_EMAIL + ' ? (yes/no) ')
  if (answer !== 'yes') {
    console.log('❌ Annule.')
    await p.$disconnect()
    process.exit(0)
  }

  // 5. Update
  await p.user.update({
    where: { email: TARGET_EMAIL },
    data: { passwordHash: newHash }
  })

  // 6. Afficher APRES
  console.log('\nAPRES:')
  console.log('  hash prefix:', newHash.substring(0, 15))
  console.log('  ancien != nouveau:', user.passwordHash !== newHash ? '✅ OUI' : '❌ IDENTIQUE')

  // 7. Test bcrypt.compare
  const ok = await bcrypt.compare(NEW_PASSWORD, newHash)
  console.log('  bcrypt.compare test:', ok ? '✅ PASS' : '❌ FAIL — ROLLBACK NECESSAIRE')

  if (!ok) {
    console.error('\n🚨 ECHEC — le hash ne match pas. Contacter admin pour rollback.')
    await p.$disconnect()
    process.exit(1)
  }

  console.log('\n✅ Rehash termine avec succes.\n')
  await p.$disconnect()
}

main().catch(e => { console.error(e); process.exit(1) })
