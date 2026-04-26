require('dotenv').config()
const { PrismaClient } = require('@prisma/client')
const p = new PrismaClient()

const PHONES = {
  'chloe.didier@liliwatt.fr':       '06 45 63 35 47',
  'sanae.elboutrouki@liliwatt.fr':  '07 81 77 46 17',
  'khadija.fatah@liliwatt.fr':      '07 61 30 92 37',
  'emmanuel.leray@liliwatt.fr':     '06 27 77 22 24',
  'zakaria.jamaoui@liliwatt.fr':    '06 63 63 51 89',
  'johan.mallet@liliwatt.fr':       '07 80 44 54 06',
  'kevin.moreau@liliwatt.fr':       '07 83 10 11 29',
  'jordan.gaillard@liliwatt.fr':    '07 80 61 17 32',
  'christophe.borrego@liliwatt.fr': '07 82 94 77 84',
  'stephane.bergerot@liliwatt.fr':  '06 33 02 37 85',
  'ludovic.nouri@liliwatt.fr':      '06 98 22 28 45',
  'mathis.hamard@liliwatt.fr':      '06 13 92 58 88',
  'daniel.desmul@liliwatt.fr':      '07 59 51 40 53',
}

async function main() {
  console.log('\nPUSH TELEPHONES VENDEURS (final)\n')
  const users = await p.user.findMany({ where: { isActive: true }, select: { id: true, email: true, firstName: true, lastName: true, phone: true, role: true }, orderBy: [{ role: 'desc' }, { firstName: 'asc' }] })
  console.log('Total users actifs:', users.length)
  console.log('Telephones dans le mapping:', Object.keys(PHONES).length, '\n')

  let updated = 0, unchanged = 0, notFound = 0
  for (const u of users) {
    const newPhone = PHONES[u.email.toLowerCase()]
    if (!newPhone) { console.log('  ' + u.email + ' → pas dans le mapping'); notFound++; continue }
    if (u.phone === newPhone) { console.log('  ' + u.firstName + ' → deja a jour (' + u.phone + ')'); unchanged++; continue }
    await p.user.update({ where: { id: u.id }, data: { phone: newPhone } })
    console.log('  ' + u.firstName + ' ' + (u.lastName || '') + ' → ' + newPhone + (u.phone ? ' (avant: ' + u.phone + ')' : ''))
    updated++
  }

  console.log('\nMis a jour:', updated, '| Inchanges:', unchanged, '| Pas trouves:', notFound)
  await p.$disconnect()
}
main().catch(e => { console.error(e); process.exit(1) })
