"""
SCRAPER GOOGLE PLACES → NEON
Usage: python3 scraper_neon.py <secteur> <ville>
"""
import sys, os, time, re, requests, psycopg2, uuid
from datetime import datetime

DATABASE_URL = os.environ.get('DATABASE_URL')
GOOGLE_API_KEY = os.environ.get('GOOGLE_PLACES_API_KEY')

if not DATABASE_URL:
    print("ERROR: DATABASE_URL non definie", file=sys.stderr); sys.exit(1)
if not GOOGLE_API_KEY:
    print("ERROR: GOOGLE_PLACES_API_KEY non definie", file=sys.stderr); sys.exit(1)

SECTEURS_QUERIES = {
    'restaurant': 'restaurant', 'hotel': 'hotel', 'boulangerie': 'boulangerie',
    'laverie': 'laverie automatique', 'garage': 'garage automobile',
    'supermarche': 'supermarche', 'salle_sport': 'salle de sport',
    'spa': 'spa institut beaute', 'bar': 'bar', 'camping': 'camping',
    'pressing': 'pressing', 'piscine': 'piscine'
}
SECTEUR_LABELS = {
    'restaurant': 'Restaurant', 'hotel': 'Hotel', 'boulangerie': 'Boulangerie',
    'laverie': 'Laverie', 'garage': 'Garage automobile', 'supermarche': 'Supermarche',
    'salle_sport': 'Salle de sport', 'spa': 'Spa', 'bar': 'Bar',
    'camping': 'Camping', 'pressing': 'Pressing', 'piscine': 'Piscine'
}

def cuid():
    return 'c' + uuid.uuid4().hex[:24]

def geocode(ville):
    r = requests.get('https://maps.googleapis.com/maps/api/geocode/json',
        params={'address': ville, 'key': GOOGLE_API_KEY, 'region': 'fr'}, timeout=10)
    data = r.json()
    if data.get('results'):
        loc = data['results'][0]['geometry']['location']
        return loc['lat'], loc['lng']
    return None, None

def search_places(query, lat, lng, radius=5000):
    url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json'
    results, page_token, pages = [], None, 0
    while pages < 3:
        params = {'location': f'{lat},{lng}', 'radius': radius, 'keyword': query, 'key': GOOGLE_API_KEY}
        if page_token:
            params['pagetoken'] = page_token; time.sleep(2)
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        results.extend(data.get('results', []))
        page_token = data.get('next_page_token')
        pages += 1
        if not page_token: break
    return results

def get_details(place_id):
    r = requests.get('https://maps.googleapis.com/maps/api/place/details/json',
        params={'place_id': place_id, 'fields': 'name,formatted_address,formatted_phone_number,website,rating,user_ratings_total,address_components',
                'key': GOOGLE_API_KEY, 'language': 'fr'}, timeout=10)
    return r.json().get('result', {})

def extract_ville_cp(comps, default_ville):
    ville, cp = default_ville, None
    for c in comps or []:
        if 'locality' in c.get('types', []): ville = c.get('long_name', ville)
        if 'postal_code' in c.get('types', []): cp = c.get('long_name')
    return ville, cp

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 scraper_neon.py <secteur> <ville>", file=sys.stderr); sys.exit(1)
    secteur, ville_input = sys.argv[1], sys.argv[2]
    if secteur not in SECTEURS_QUERIES:
        print(f"Secteur inconnu: {secteur}", file=sys.stderr); sys.exit(1)

    query = SECTEURS_QUERIES[secteur]
    label = SECTEUR_LABELS.get(secteur, secteur)
    print(f"🔍 Scraping {label} a {ville_input}...")

    lat, lng = geocode(ville_input)
    if not lat:
        print(f"❌ Geocodage echoue: {ville_input}", file=sys.stderr); sys.exit(1)
    print(f"📍 ({lat}, {lng})")

    places = search_places(query, lat, lng)
    print(f"📥 {len(places)} resultats Google Places")
    if not places:
        print("INSERTED:0|SKIPPED:0|UPDATED:0"); sys.exit(0)

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    inserted, skipped, errors = 0, 0, 0

    for p in places:
        try:
            pid = p.get('place_id')
            if not pid: skipped += 1; continue
            cur.execute('SELECT id FROM "Prospect" WHERE "placeId" = %s', (pid,))
            if cur.fetchone(): skipped += 1; continue

            det = get_details(pid); time.sleep(0.05)
            nom = det.get('name') or p.get('name')
            if not nom: skipped += 1; continue

            adr = det.get('formatted_address')
            ville, cp = extract_ville_cp(det.get('address_components'), ville_input)
            if not cp and adr:
                m = re.search(r'\b(\d{5})\b', adr)
                if m: cp = m.group(1)

            now = datetime.utcnow()
            cur.execute('''INSERT INTO "Prospect" (id,source,"placeId","raisonSociale",adresse,ville,"codePostal",
                secteur,telephone,"siteWeb","noteGoogle","nbAvis","rgpdEnvoye","isManuelle","isVerrouillee","createdAt","updatedAt")
                VALUES (%s,'BRUTE',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,false,false,false,%s,%s)''',
                (cuid(), pid, nom, adr, ville, cp, label,
                 det.get('formatted_phone_number'), det.get('website'),
                 det.get('rating'), det.get('user_ratings_total'), now, now))
            inserted += 1
            if inserted % 10 == 0: conn.commit()
        except Exception as e:
            errors += 1
            if errors <= 3: print(f"  ⚠️  {e}", file=sys.stderr)

    conn.commit(); cur.close(); conn.close()
    print(f"\n✅ {inserted} ajoutes · {skipped} doublons · {errors} erreurs")
    print(f"INSERTED:{inserted}|SKIPPED:{skipped}|UPDATED:0")

if __name__ == '__main__':
    main()
