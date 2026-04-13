import requests, csv, time, sys, json, os
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

API_KEY = os.environ.get('GOOGLE_PLACES_API_KEY', 'AIzaSyBJwFQ2Zco52OwpxrJlMqzfzWnG-k76rSE')
SHEET_ID = os.environ.get('MASTER_SHEET_ID', '1JFEAXFZbdvf40yDWZGVnuEgUN15XdOAx6WgqL69-AMA')
LOG_FILE = os.environ.get('SCRAPING_LOG', '/tmp/scraping_log.json')

# Credentials : base64 env > fichier local
CREDS_DICT = None
if os.environ.get('GOOGLE_DRIVE_CREDS_BASE64'):
    import base64
    CREDS_DICT = json.loads(base64.b64decode(os.environ['GOOGLE_DRIVE_CREDS_BASE64']).decode())
CREDS_FILE = os.path.join(os.path.dirname(__file__), '..', 'courtier-energie', 'liliwatt-drive-credentials.json')

SECTEURS = {
    'restaurant': 'restaurant',
    'hotel': 'lodging',
    'boulangerie': 'bakery',
    'laverie': 'laundry',
    'garage': 'car_repair',
    'supermarche': 'supermarket',
    'salle_sport': 'gym',
    'spa_bien_etre': 'spa',
    'bar_discoteque': 'night_club',
    'camping': 'campground',
    'pressing': 'clothing_store',
    'piscine': 'swimming_pool'
}

SECTEURS_FR = {
    'restaurant': 'Restaurant',
    'lodging': 'Hôtel / Hébergement',
    'bakery': 'Boulangerie / Pâtisserie',
    'laundry': 'Laverie / Pressing',
    'car_repair': 'Garage / Auto',
    'supermarket': 'Supermarché',
    'gym': 'Salle de sport',
    'spa': 'Spa / Bien-être',
    'night_club': 'Bar / Discothèque',
    'campground': 'Camping / Gîte',
    'clothing_store': 'Commerce / Prêt-à-porter',
    'swimming_pool': 'Piscine'
}

HEADERS = ['place_id', 'raison_sociale', 'adresse', 'ville', 'secteur',
           'note_google', 'nb_avis', 'telephone', 'site_web',
           'vendeur_attribue', 'statut_appel', 'note_appel', 'date_rappel']


# ===== LOG =====
def load_log():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_log(log):
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        json.dump(log, f, indent=2, ensure_ascii=False)

def get_dept(ville):
    """Extrait le département via geocoding."""
    try:
        r = requests.get("https://maps.googleapis.com/maps/api/geocode/json", params={
            'address': ville + ', France', 'key': API_KEY
        }).json()
        if r.get('results'):
            for comp in r['results'][0].get('address_components', []):
                if 'postal_code' in comp.get('types', []):
                    return comp['short_name'][:2]
    except:
        pass
    return '00'


# ===== GOOGLE PLACES =====
def nearby_search(type_lieu, ville, rayon=50000):
    geo_url = "https://maps.googleapis.com/maps/api/geocode/json"
    geo_r = requests.get(geo_url, params={
        'address': ville + ', France', 'key': API_KEY
    }).json()
    if not geo_r.get('results'):
        print(f"❌ Geocoding échoué: {geo_r.get('status')} {geo_r.get('error_message','')}")
        return []
    loc = geo_r['results'][0]['geometry']['location']
    lat, lng = loc['lat'], loc['lng']
    secteur_fr = SECTEURS_FR.get(type_lieu, type_lieu)
    resultats = []
    next_token = None
    for _ in range(3):
        params = {
            'location': f"{lat},{lng}",
            'radius': rayon,
            'type': type_lieu,
            'language': 'fr',
            'key': API_KEY
        }
        if next_token:
            params = {'pagetoken': next_token, 'key': API_KEY}
            time.sleep(2)
        url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
        r = requests.get(url, params=params).json()
        for place in r.get('results', []):
            resultats.append({
                'place_id': place.get('place_id', ''),
                'raison_sociale': place.get('name', ''),
                'adresse': place.get('vicinity', ''),
                'ville': ville,
                'secteur': secteur_fr,
                'note_google': str(place.get('rating', '')),
                'nb_avis': str(place.get('user_ratings_total', '')),
                'telephone': '',
                'site_web': '',
                'vendeur_attribue': '',
                'statut_appel': '',
                'note_appel': '',
                'date_rappel': ''
            })
        next_token = r.get('next_page_token')
        if not next_token:
            break
    return resultats


def get_details(place_id):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    r = requests.get(url, params={
        'place_id': place_id,
        'fields': 'formatted_phone_number,international_phone_number,website',
        'language': 'fr',
        'key': API_KEY
    }).json()
    result = r.get('result', {})
    tel = result.get('formatted_phone_number', '') or result.get('international_phone_number', '')
    return {
        'telephone': tel if tel else 'À compléter',
        'site_web': result.get('website', '')
    }


# ===== GOOGLE SHEETS =====
def upload_to_sheets(prospects):
    if CREDS_DICT:
        creds = Credentials.from_service_account_info(CREDS_DICT,
            scopes=['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    else:
        creds = Credentials.from_service_account_file(CREDS_FILE,
            scopes=['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)

    try:
        ws = sh.worksheet('BASE BRUTE')
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title='BASE BRUTE', rows=50000, cols=len(HEADERS))
        ws.update(values=[HEADERS], range_name='A1')
        print("📊 Feuille BASE BRUTE créée")

    existing = ws.get_all_values()
    if not existing or existing[0] != HEADERS:
        ws.update(values=[HEADERS], range_name='A1')
        existing = [HEADERS]

    existing_keys = set()
    for row in existing[1:]:
        if len(row) >= 4:
            existing_keys.add((row[1].strip().lower(), row[3].strip().lower()))

    nouvelles = []
    for p in prospects:
        key = (p['raison_sociale'].strip().lower(), p['ville'].strip().lower())
        if key not in existing_keys:
            nouvelles.append([p.get(h, '') for h in HEADERS])
            existing_keys.add(key)

    if nouvelles:
        for i in range(0, len(nouvelles), 100):
            ws.append_rows(nouvelles[i:i+100], value_input_option='RAW')
            time.sleep(1)
        print(f"📊 {len(nouvelles)} nouvelles lignes ajoutées dans Sheets")
    else:
        print("📊 Aucune nouvelle ligne (toutes déjà présentes)")

    return len(nouvelles)


# ===== MAIN =====
def main():
    secteur_nom = sys.argv[1] if len(sys.argv) > 1 else 'restaurant'
    ville = sys.argv[2] if len(sys.argv) > 2 else 'Paris'
    output = f"prospects_{secteur_nom}_{ville}.csv".replace(' ', '_')
    type_lieu = SECTEURS.get(secteur_nom, secteur_nom)

    # Vérifier le log
    dept = get_dept(ville)
    log_key = f"{secteur_nom}_{dept}"
    log = load_log()
    if log_key in log:
        print(f"⏭️  {secteur_nom} dans le {dept} déjà scrapé le {log[log_key]}, ignoré")
        print(f"   (supprime la clé '{log_key}' de scraping_log.json pour relancer)")
        return

    print(f"🔍 {secteur_nom} à {ville} (dept {dept})...")
    prospects = nearby_search(type_lieu, ville)

    if not prospects:
        print("❌ Aucun résultat trouvé")
        return

    print(f"📍 {len(prospects)} trouvés — récupération des téléphones...")
    for i, p in enumerate(prospects):
        if p['place_id']:
            details = get_details(p['place_id'])
            p['telephone'] = details['telephone']
            p['site_web'] = details['site_web']
        if (i + 1) % 10 == 0:
            print(f"  {i+1}/{len(prospects)}...")
        time.sleep(0.1)

    # CSV local
    with open(output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(prospects)

    avec_tel = sum(1 for p in prospects if p['telephone'] and p['telephone'] != 'À compléter')
    print(f"✅ {len(prospects)} prospects → {output}")
    print(f"📞 {avec_tel} avec téléphone | {len(prospects)-avec_tel} à compléter")

    # Upload Sheets
    print(f"\n📤 Upload vers Google Sheets...")
    nouvelles = upload_to_sheets(prospects)
    print(f"✅ Terminé — {nouvelles} nouvelles lignes dans Sheets")

    # Sauvegarder dans le log
    log[log_key] = datetime.now().strftime('%Y-%m-%d')
    save_log(log)
    print(f"📝 Log mis à jour : {log_key} = {log[log_key]}")


if __name__ == '__main__':
    main()
