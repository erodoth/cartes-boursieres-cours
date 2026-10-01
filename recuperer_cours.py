#!/usr/bin/env python3
"""
Job de récupération des cours en direct -- alimente cours_actuels et
cours_historique dans Supabase, une fois par heure (programmé via GitHub
Actions, cf. .github/workflows/cours.yml dans ce même paquet).

Reprend la logique validée dans analyse_yahoo.py (téléchargement yfinance par
paquets de 100 tickers avec pause de sécurité) mais :
  - ne télécharge QUE les tickers ouverts à l'heure UTC courante (places,
    commodities sauf maintenance 21h UTC, cryptos toujours) -- la liste est
    recalculée à chaque run depuis Supabase (societes.ticker_yahoo + devise,
    places.heures_ouvertes_utc + jours_ouvres, commodites/cryptos.ticker_yahoo),
    donc jamais désynchronisée avec l'univers réel de cartes ;
  - télécharge period="1d" interval="5m" (pas 2 jours) : on ne veut que le
    DERNIER cours connu, pas une variation calculée entre deux heures précises ;
  - PAS de conversion en USD : le score se calcule sur la performance
    (variation relative par rapport à un prix de référence), pas sur le
    niveau de prix absolu -- la devise se simplifie dans un ratio prix/prix_ref.
    taux_change ne sert qu'au calcul de capitalisation_usd, un besoin distinct
    déjà traité ailleurs dans le pipeline ; ce script n'y touche pas. On garde
    `devise` à titre informatif (affichage), mais on ne calcule pas prix_usd ;
  - écrit directement en base (upsert cours_actuels + insert cours_historique)
    au lieu d'un CSV -- connexion Postgres directe (DATABASE_URL), pas la
    session cloud Claude qui ne peut pas joindre Yahoo (403 côté proxy, testé
    le 2026-10-01).

Marché fermé à cette heure pour un ticker donné => on ne le retélécharge pas
du tout : son cours dans cours_actuels reste celui du dernier relevé (dernière
clôture connue), ce qui correspond à la règle "prix de référence = dernière
clôture quand le marché est fermé".
"""
import os
import sys
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import yfinance as yf

TAILLE_PAQUET = 100
PAUSE_ENTRE_PAQUETS_SEC = 4.0
HEURE_MAINTENANCE_COMMODITIES = 21  # pause quotidienne CME Globex, 21h-22h UTC
JOURS_FR = ["lun", "mar", "mer", "jeu", "ven", "sam", "dim"]


def jour_ouvre(jours_ouvres: str, jour_semaine_idx: int) -> bool:
    """jours_ouvres: 'lun-ven' ou 'dim-jeu'. jour_semaine_idx: 0=lundi .. 6=dimanche
    (datetime.weekday())."""
    if not jours_ouvres or "-" not in jours_ouvres:
        return True
    debut, fin = jours_ouvres.split("-")
    i_debut, i_fin = JOURS_FR.index(debut), JOURS_FR.index(fin)
    if i_debut <= i_fin:
        return i_debut <= jour_semaine_idx <= i_fin
    # plage qui traverse la semaine (ex. 'ven-lun'), pas utilisé actuellement
    # mais gardé par robustesse
    return jour_semaine_idx >= i_debut or jour_semaine_idx <= i_fin


def recuperer_tickers_ouverts(conn, maintenant_utc: datetime):
    """Retourne {ticker_yahoo: (id_societe, devise)} pour tout ce qui doit être
    coté à l'heure UTC courante."""
    heure = maintenant_utc.hour
    jour_idx = maintenant_utc.weekday()  # 0 = lundi
    tickers = {}

    with conn.cursor() as cur:
        # Sociétés cotées (hors "collector"), place ouverte à cette heure/jour
        cur.execute(
            """
            SELECT s.id_societe, s.ticker_yahoo, s.devise, p.heures_ouvertes_utc, p.jours_ouvres
            FROM societes s
            JOIN places p ON s.code_place = p.code_place
            WHERE s.statut_cotation = 'cotee'
              AND s.ticker_yahoo IS NOT NULL
              AND s.devise IS NOT NULL
            """
        )
        for id_societe, ticker_yahoo, devise, heures_csv, jours_ouvres in cur.fetchall():
            heures_ouvertes = {int(h) for h in (heures_csv or "").split(",") if h.strip() != ""}
            if heure in heures_ouvertes and jour_ouvre(jours_ouvres, jour_idx):
                tickers[ticker_yahoo] = (id_societe, devise)

        # Commodities : toutes les heures sauf maintenance CME Globex (21h UTC),
        # fermé le samedi (jour_idx == 5)
        if heure != HEURE_MAINTENANCE_COMMODITIES and jour_idx != 5:
            cur.execute(
                "SELECT id_societe, ticker_yahoo FROM commodites WHERE ticker_yahoo IS NOT NULL"
            )
            for id_societe, ticker_yahoo in cur.fetchall():
                tickers[ticker_yahoo] = (id_societe, "USD")

        # Cryptos : 24/7
        cur.execute("SELECT id_societe, ticker_yahoo FROM cryptos WHERE ticker_yahoo IS NOT NULL")
        for id_societe, ticker_yahoo in cur.fetchall():
            tickers[ticker_yahoo] = (id_societe, "USD")

    return tickers


def telecharger_derniers_prix(liste_tickers):
    """Télécharge par paquets de 100 et retourne {ticker: dernier_prix_close}.

    Pas de session requests personnalisée ici : yfinance (via curl_cffi) gère
    lui-même l'impersonation de navigateur nécessaire pour obtenir un "crumb"
    Yahoo sans se faire rate-limiter -- lui imposer notre propre session
    requests écrasait cette gestion et déclenchait du 429 immédiat (constaté
    le 2026-10-01 depuis un runner GitHub Actions). On retente aussi chaque
    paquet avec un backoff en cas de 429/vide, les IP partagées des runners
    étant plus vite bridées qu'une connexion résidentielle.
    """
    resultats = {}
    paquets = [liste_tickers[i:i + TAILLE_PAQUET] for i in range(0, len(liste_tickers), TAILLE_PAQUET)]
    print(f"📦 {len(liste_tickers)} tickers à jour, {len(paquets)} paquet(s) de {TAILLE_PAQUET} max.")

    for i, paquet in enumerate(paquets):
        print(f"  ➔ Paquet {i + 1}/{len(paquets)} ({len(paquet)} tickers)...")
        data = None
        for tentative in range(3):
            try:
                data = yf.download(
                    paquet, period="1d", interval="5m", progress=False,
                    group_by="ticker", threads=True,
                )
            except Exception as e:
                print(f"  ❌ Tentative {tentative + 1}/3 échouée sur paquet {i + 1} : {e}")
                data = None
            if data is not None and not data.empty:
                break
            if tentative < 2:
                pause = 15 * (tentative + 1)
                print(f"  ⏳ Pas de donnée (429 probable), nouvelle tentative dans {pause}s...")
                time.sleep(pause)

        if data is None or data.empty:
            print(f"  ⚠️  Paquet {i + 1} : aucune donnée renvoyée après 3 tentatives.")
        else:
            for ticker in paquet:
                try:
                    if len(paquet) == 1:
                        serie = data["Close"].dropna()
                    elif ticker in data.columns.get_level_values(0):
                        serie = data[ticker]["Close"].dropna()
                    else:
                        continue
                    if not serie.empty:
                        resultats[ticker] = float(serie.iloc[-1])
                except Exception:
                    continue

        if i + 1 < len(paquets):
            time.sleep(PAUSE_ENTRE_PAQUETS_SEC)

    return resultats


def ecrire_cours(conn, lignes, horodatage, heure_utc):
    """lignes: liste de (id_societe, prix, devise)."""
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO cours_actuels (id_societe, prix, devise, horodatage, heure_utc, source)
            VALUES %s
            ON CONFLICT (id_societe) DO UPDATE SET
                prix = EXCLUDED.prix,
                devise = EXCLUDED.devise,
                horodatage = EXCLUDED.horodatage,
                heure_utc = EXCLUDED.heure_utc,
                source = EXCLUDED.source
            """,
            [(id_s, prix, dev, horodatage, heure_utc, "yahoo") for id_s, prix, dev in lignes],
        )
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO cours_historique (id_societe, horodatage, prix, devise, source)
            VALUES %s
            ON CONFLICT (id_societe, horodatage) DO NOTHING
            """,
            [(id_s, horodatage, prix, dev, "yahoo") for id_s, prix, dev in lignes],
        )
    conn.commit()


def main():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("❌ Variable d'environnement DATABASE_URL manquante.")
        sys.exit(1)

    maintenant = datetime.now(timezone.utc)
    print(f"⚡ Run {maintenant.isoformat()} (heure UTC {maintenant.hour}, jour {JOURS_FR[maintenant.weekday()]})")

    conn = psycopg2.connect(database_url)
    try:
        tickers_ouverts = recuperer_tickers_ouverts(conn, maintenant)

        if not tickers_ouverts:
            print("⚠️  Aucun ticker ouvert à cette heure -- rien à faire.")
            return

        prix_par_ticker = telecharger_derniers_prix(list(tickers_ouverts.keys()))

        lignes = []
        manquants = []
        for ticker, (id_societe, devise) in tickers_ouverts.items():
            prix = prix_par_ticker.get(ticker)
            if prix is None:
                manquants.append(ticker)
                continue
            lignes.append((id_societe, prix, devise))

        if lignes:
            ecrire_cours(conn, lignes, maintenant, maintenant.hour)

        print(f"✨ {len(lignes)} cours écrits, {len(manquants)} tickers sans donnée récupérée.")
        if manquants:
            print(f"   Exemples de tickers manquants : {manquants[:20]}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
