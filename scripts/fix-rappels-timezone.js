// Backfill timezone des rappels existants
// Bug: l'heure locale etait stockee comme UTC (decalage +1h CET / +2h CEST)
// Fix: soustraire l'offset pour retrouver la vraie heure UTC
//
// Usage: node scripts/fix-rappels-timezone.js          (dry-run)
//        node scripts/fix-rappels-timezone.js --apply   (execute)

require('dotenv').config()
const { PrismaClient } = require('@prisma/client')
const p = new PrismaClient()

const DRY_RUN = !process.argv.includes('--apply')
const DST_SWITCH = new Date('2026-03-29T01:00:00.000Z') // CET→CEST

async function main() {
  console.log(`\n${DRY_RUN ? '🔍 DRY RUN' : '🔧 APPLY'}\n`)

  const rappels = await p.prospect.findMany({
    where: { dateRappel: { not: null } },
    select: { id: true, dateRappel: true, raisonSociale: true },
    orderBy: { dateRappel: 'asc' }
  })

  console.log(`Rappels a corriger : ${rappels.length}`)

  const fixes = rappels.map(r => {
    const isCEST = r.dateRappel >= DST_SWITCH
    const offsetMs = isCEST ? 2 * 60 * 60 * 1000 : 1 * 60 * 60 * 1000
    const fixed = new Date(r.dateRappel.getTime() - offsetMs)
    return { id: r.id, nom: r.raisonSociale, before: r.dateRappel, after: fixed, offset: isCEST ? '-2h CEST' : '-1h CET' }
  })

  const cet = fixes.filter(f => f.offset === '-1h CET').length
  const cest = fixes.filter(f => f.offset === '-2h CEST').length
  console.log(`  CET (-1h)  : ${cet}`)
  console.log(`  CEST (-2h) : ${cest}`)

  console.log(`\nDetails :`)
  fixes.forEach(f => {
    console.log(`  ${f.nom.substring(0, 30).padEnd(32)} avant=${f.before.toISOString()} → apres=${f.after.toISOString()} (${f.offset})`)
  })

  if (DRY_RUN) {
    console.log(`\n→ Relancer avec --apply pour executer\n`)
    await p.$disconnect()
    return
  }

  await p.$transaction(async (tx) => {
    for (const f of fixes) {
      await tx.prospect.update({ where: { id: f.id }, data: { dateRappel: f.after } })
    }
  })

  console.log(`\n✅ ${fixes.length} rappels corriges\n`)
  await p.$disconnect()
}

main().catch(e => { console.error(e); process.exit(1) })
