// Backfill INVOICE_RECEIVED + SALE_SIGNED events pour prospects historiques
// Usage: node scripts/backfill-events.js          (dry-run)
//        node scripts/backfill-events.js --apply   (execute)

require('dotenv').config()
const { PrismaClient } = require('@prisma/client')
const p = new PrismaClient()

const DRY_RUN = !process.argv.includes('--apply')

async function main() {
  console.log(`\n${DRY_RUN ? '🔍 DRY RUN' : '🔧 APPLY'}\n`)

  // 1. Prospects cibles
  const prospects = await p.prospect.findMany({
    where: { statutAppel: { in: ['DOSSIER_RECU', 'CLIENT_SIGNE'] } },
    select: { id: true, statutAppel: true, vendeurId: true, dateDernierAppel: true, createdAt: true, raisonSociale: true }
  })
  console.log(`Prospects DOSSIER_RECU + CLIENT_SIGNE : ${prospects.length}`)

  // 2. Events existants pour ces prospects
  const ids = prospects.map(pr => pr.id)
  const existingEvents = await p.activityLog.findMany({
    where: { prospectId: { in: ids }, type: { in: ['INVOICE_RECEIVED', 'SALE_SIGNED'] } },
    select: { prospectId: true, type: true }
  })
  const existsSet = new Set(existingEvents.map(e => e.prospectId + ':' + e.type))

  // 3. Preparer les events a creer
  const toCreate = []
  const skipped = []

  for (const pr of prospects) {
    if (!pr.vendeurId) { skipped.push(pr); continue }
    const ts = pr.dateDernierAppel || pr.createdAt

    // INVOICE_RECEIVED pour DOSSIER_RECU et CLIENT_SIGNE
    if (!existsSet.has(pr.id + ':INVOICE_RECEIVED')) {
      toCreate.push({ userId: pr.vendeurId, prospectId: pr.id, type: 'INVOICE_RECEIVED', timestamp: ts, metadata: { backfilled: true } })
    }

    // SALE_SIGNED pour CLIENT_SIGNE uniquement
    if (pr.statutAppel === 'CLIENT_SIGNE' && !existsSet.has(pr.id + ':SALE_SIGNED')) {
      toCreate.push({ userId: pr.vendeurId, prospectId: pr.id, type: 'SALE_SIGNED', timestamp: ts, metadata: { backfilled: true } })
    }
  }

  const invoiceCount = toCreate.filter(e => e.type === 'INVOICE_RECEIVED').length
  const saleCount = toCreate.filter(e => e.type === 'SALE_SIGNED').length

  console.log(`\nEvents a creer :`)
  console.log(`  INVOICE_RECEIVED : ${invoiceCount}`)
  console.log(`  SALE_SIGNED      : ${saleCount}`)
  console.log(`  TOTAL            : ${toCreate.length}`)
  if (skipped.length) console.log(`  ⚠️  Skipped (pas de vendeurId) : ${skipped.length}`)

  console.log(`\nDetails (5 premiers) :`)
  toCreate.slice(0, 5).forEach(e => {
    const pr = prospects.find(p2 => p2.id === e.prospectId)
    console.log(`  ${e.type.padEnd(20)} ${(pr?.raisonSociale || '').substring(0, 30).padEnd(32)} vendeur:${e.userId.substring(0, 12)} ts:${e.timestamp.toISOString().substring(0, 10)}`)
  })

  if (DRY_RUN) {
    console.log(`\n→ Relancer avec --apply pour executer\n`)
    await p.$disconnect()
    return
  }

  // 4. Executer en transaction
  await p.$transaction(async (tx) => {
    for (const e of toCreate) {
      await tx.activityLog.create({ data: e })
    }
  })

  console.log(`\n✅ ${toCreate.length} events crees\n`)
  await p.$disconnect()
}

main().catch(e => { console.error(e); process.exit(1) })
