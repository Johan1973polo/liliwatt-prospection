require('dotenv').config()
const { PrismaClient } = require('@prisma/client')
const prisma = new PrismaClient()

async function main() {
  console.log('\n=== VERIFICATION POST-MIGRATION ===\n')

  const total = await prisma.prospect.count()
  console.log(`Total prospects : ${total}`)

  const bySource = await prisma.prospect.groupBy({
    by: ['source'],
    _count: true
  })
  console.log('\nPar source :')
  bySource.forEach(s => console.log(`  ${s.source} : ${s._count}`))

  const byStatut = await prisma.prospect.groupBy({
    by: ['statutAppel'],
    _count: true
  })
  console.log('\nPar statut :')
  byStatut.forEach(s => console.log(`  ${s.statutAppel || 'NULL'} : ${s._count}`))

  const attribues = await prisma.prospect.count({
    where: { vendeurId: { not: null } }
  })
  console.log(`\nProspects attribues : ${attribues}`)

  const rgpd = await prisma.prospect.count({
    where: { rgpdEnvoye: true }
  })
  console.log(`RGPD envoyes : ${rgpd}`)

  // Verifier FK vendeur
  const sample = await prisma.prospect.findMany({
    where: { vendeurId: { not: null } },
    take: 5,
    include: { vendeur: { select: { email: true, role: true } } }
  })
  console.log('\nEchantillon prospects attribues :')
  sample.forEach(p => {
    console.log(`  ${p.raisonSociale} → ${p.vendeur?.email || 'ORPHELIN!'}`)
  })

  // Performance index
  const t0 = Date.now()
  await prisma.prospect.findMany({
    where: { source: 'PREMIUM', vendeurId: null },
    take: 100
  })
  const dt1 = Date.now() - t0

  const t1 = Date.now()
  await prisma.prospect.findMany({
    where: { source: 'BRUTE', ville: { contains: 'Paris', mode: 'insensitive' } },
    take: 50
  })
  const dt2 = Date.now() - t1

  console.log(`\n⚡ Requete PREMIUM libres (100 lignes) : ${dt1}ms`)
  console.log(`⚡ Requete BRUTE ville=Paris (50 lignes) : ${dt2}ms`)

  console.log('\n✅ VERIFICATION TERMINEE')
  await prisma.$disconnect()
}

main().catch(e => { console.error(e); process.exit(1) })
