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

Retard Yahoo (ajouté le 2026-10-01) : Yahoo peut avoir jusqu'à 15-20 min de
retard sur certaines places (licences de données temps réel variables selon
la bourse). cours_actuels/cours_historique gardent donc DEUX horodatages :
`horodatage_cours` (l'heure réelle de la bougie renvoyée par Yahoo -- c'est
la seule à utiliser pour savoir "de quand date ce prix") et
`horodatage_recuperation` (l'heure à laquelle ce script a tourné -- utile
seulement pour le suivi/debug du job lui-même). Le run affiche aussi le
retard médian/max observé à chaque exécution.
"""
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
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


def _detail_depuis_exception(e: Exception) -> tuple:
    """Extrait (code_http, message) d'une exception yfinance/requests, au
    mieux -- yfinance n'expose pas toujours le code HTTP de façon structurée,
    donc on retombe sur une détection par mots-clés dans le message quand
    l'objet exception ne porte pas de `.response.status_code` exploitable."""
    code_http = None
    resp = getattr(e, "response", None)
    if resp is not None:
        code_http = getattr(resp, "status_code", None)
    message = str(e) or e.__class__.__name__
    if code_http is None and ("429" in message or "Too Many Requests" in message):
        code_http = 429
    return code_http, message[:500]


def _extraire_serie(data, ticker, taille_paquet):
    """Retourne la série Close non-vide pour `ticker` dans `data` (résultat de
    yf.download), ou None si absente -- factorisé pour être appelé à la fois
    sur un paquet et sur un ticker seul (taille_paquet == 1)."""
    if data is None or data.empty:
        return None
    try:
        if taille_paquet == 1:
            serie = data["Close"]
        elif ticker in data.columns.get_level_values(0):
            serie = data[ticker]["Close"]
        else:
            return None
        # yfinance >= 0.2.51 renvoie des colonnes MultiIndex même pour UN seul ticker : data["Close"]
        # est alors un DataFrame (une colonne par ticker) et non une Series -- float(serie.iloc[-1])
        # plantait en TypeError ("float() argument must be ... not 'Series'"), ce qui faisait tomber
        # tout le run (constaté le 2026-10-05, repli individuel de l'étape paquet 6/36).
        if isinstance(serie, pd.DataFrame):
            if serie.shape[1] == 0:
                return None
            serie = serie.iloc[:, 0]
        serie = serie.dropna()
    except Exception:
        return None
    return serie if not serie.empty else None


def telecharger_un_ticker(ticker):
    """Repli individuel pour un ticker resté sans donnée après la tentative
    groupée -- permet de distinguer un ticker réellement cassé (échoue aussi
    seul, avec une erreur propre à capturer) d'un ticker simplement entraîné
    dans l'échec d'un paquet à cause d'UN AUTRE ticker du même paquet
    (constaté le 2026-10-02 sur l'Inde : yf.download() sur un paquet entier
    abandonne les 100 tickers d'un coup si un seul fait planter la requête
    groupée, sans jamais les retenter séparément). 2 tentatives avec un
    backoff court -- on est déjà dans le cas lent/dégradé, pas la peine de
    s'acharner comme sur un paquet complet.

    Retourne soit (prix, horodatage_cours, None) en cas de succès, soit
    (None, None, (code_http, detail_erreur)) en cas d'échec.
    """
    derniere_erreur = None
    for tentative in range(2):
        try:
            data = yf.download(ticker, period="1d", interval="5m", progress=False)
        except Exception as e:
            derniere_erreur = _detail_depuis_exception(e)
            data = None
        if data is not None and not data.empty:
            try:
                serie = _extraire_serie(data, ticker, 1)
                if serie is not None:
                    horodatage_cours = serie.index[-1]
                    if horodatage_cours.tzinfo is None:
                        horodatage_cours = horodatage_cours.tz_localize("UTC")
                    else:
                        horodatage_cours = horodatage_cours.tz_convert("UTC")
                    return float(serie.iloc[-1]), horodatage_cours.to_pydatetime(), None
            except Exception as e:
                # Un ticker au format de réponse inattendu ne doit JAMAIS faire tomber tout le run :
                # on le compte comme un échec (journalisé dans cours_echecs) et on continue.
                derniere_erreur = (None, f"réponse illisible : {type(e).__name__}: {e}"[:500])
            else:
                derniere_erreur = (None, "requête OK mais aucune cotation renvoyée pour ce ticker")
        elif derniere_erreur is None:
            derniere_erreur = (None, "aucune donnée renvoyée (réponse vide)")
        if tentative == 0:
            time.sleep(5)
    return None, None, derniere_erreur


def telecharger_derniers_prix(liste_tickers):
    """Télécharge par paquets de 100 et retourne (resultats, echecs_detail) où
    resultats = {ticker: (prix, horodatage_cours)} (horodatage_cours = heure
    RÉELLE de la bougie Yahoo en UTC, PAS l'heure à laquelle ce script tourne
    -- Yahoo peut avoir jusqu'à 15-20 min de retard selon la place) et
    echecs_detail = {ticker: (code_http, detail_erreur)} pour tout ticker
    resté sans prix, même après repli individuel.

    Pas de session requests personnalisée ici : yfinance (via curl_cffi) gère
    lui-même l'impersonation de navigateur nécessaire pour obtenir un "crumb"
    Yahoo sans se faire rate-limiter -- lui imposer notre propre session
    requests écrasait cette gestion et déclenchait du 429 immédiat (constaté
    le 2026-10-01 depuis un runner GitHub Actions). On retente aussi chaque
    paquet avec un backoff en cas de 429/vide, les IP partagées des runners
    étant plus vite bridées qu'une connexion résidentielle.

    2026-10-03 : un paquet qui échoue (ou dont il manque certains tickers
    après coup) ne fait plus abandonner tous ses tickers d'un bloc -- chacun
    des tickers manquants est retenté individuellement via
    telecharger_un_ticker avant d'être compté comme un vrai échec, ce qui
    isole le ou les tickers réellement cassés du reste du paquet (cf. cas de
    l'Inde, où un seul ticker en faute semblait faire tomber les 52 d'un
    coup, toutes les heures).
    """
    resultats = {}
    echecs_detail = {}
    paquets = [liste_tickers[i:i + TAILLE_PAQUET] for i in range(0, len(liste_tickers), TAILLE_PAQUET)]
    print(f"📦 {len(liste_tickers)} tickers à jour, {len(paquets)} paquet(s) de {TAILLE_PAQUET} max.")

    for i, paquet in enumerate(paquets):
        print(f"  ➔ Paquet {i + 1}/{len(paquets)} ({len(paquet)} tickers)...")
        data = None
        derniere_erreur_paquet = None
        for tentative in range(3):
            try:
                data = yf.download(
                    paquet, period="1d", interval="5m", progress=False,
                    group_by="ticker", threads=True,
                )
            except Exception as e:
                derniere_erreur_paquet = _detail_depuis_exception(e)
                print(f"  ❌ Tentative {tentative + 1}/3 échouée sur paquet {i + 1} : {e}")
                data = None
            if data is not None and not data.empty:
                break
            if tentative < 2:
                pause = 15 * (tentative + 1)
                print(f"  ⏳ Pas de donnée (429 probable), nouvelle tentative dans {pause}s...")
                time.sleep(pause)

        manquants_paquet = []
        if data is None or data.empty:
            print(f"  ⚠️  Paquet {i + 1} : aucune donnée renvoyée après 3 tentatives -- repli individuel.")
            manquants_paquet = list(paquet)
        else:
            for ticker in paquet:
                try:
                    serie = _extraire_serie(data, ticker, len(paquet))
                    if serie is not None:
                        horodatage_cours = serie.index[-1]
                        if horodatage_cours.tzinfo is None:
                            horodatage_cours = horodatage_cours.tz_localize("UTC")
                        else:
                            horodatage_cours = horodatage_cours.tz_convert("UTC")
                        resultats[ticker] = (float(serie.iloc[-1]), horodatage_cours.to_pydatetime())
                    else:
                        manquants_paquet.append(ticker)
                except Exception:
                    manquants_paquet.append(ticker)

        if manquants_paquet:
            print(f"  🔁 {len(manquants_paquet)} ticker(s) du paquet {i + 1} sans donnée -- repli individuel...")
            for ticker in manquants_paquet:
                try:
                    prix, horodatage_cours, erreur = telecharger_un_ticker(ticker)
                except Exception as e:  # filet de sécurité : un ticker ne fait jamais tomber le run
                    prix, horodatage_cours, erreur = None, None, (None, f"erreur inattendue : {type(e).__name__}: {e}"[:500])
                if prix is not None:
                    resultats[ticker] = (prix, horodatage_cours)
                    print(f"     ✅ {ticker} récupéré individuellement (le paquet l'avait perdu).")
                else:
                    echecs_detail[ticker] = erreur or derniere_erreur_paquet or (None, "échec sans détail")
                time.sleep(1)

        if i + 1 < len(paquets):
            time.sleep(PAUSE_ENTRE_PAQUETS_SEC)

    return resultats, echecs_detail


def ecrire_cours(conn, lignes, horodatage_recuperation, heure_utc):
    """lignes: liste de (id_societe, prix, devise, horodatage_cours).

    horodatage_cours = heure réelle de la cotation (celle de la bougie Yahoo).
    horodatage_recuperation = heure à laquelle ce run a tourné. Les deux sont
    gardés : leur écart, c'est le retard Yahoo observé pour ce ticker.
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO cours_actuels (id_societe, prix, devise, horodatage_cours, horodatage_recuperation, heure_utc, source)
            VALUES %s
            ON CONFLICT (id_societe) DO UPDATE SET
                prix = EXCLUDED.prix,
                devise = EXCLUDED.devise,
                horodatage_cours = EXCLUDED.horodatage_cours,
                horodatage_recuperation = EXCLUDED.horodatage_recuperation,
                heure_utc = EXCLUDED.heure_utc,
                source = EXCLUDED.source
            """,
            [(id_s, prix, dev, h_cours, horodatage_recuperation, heure_utc, "yahoo")
             for id_s, prix, dev, h_cours in lignes],
        )
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO cours_historique (id_societe, horodatage_cours, prix, devise, horodatage_recuperation, source)
            VALUES %s
            ON CONFLICT (id_societe, horodatage_cours) DO NOTHING
            """,
            [(id_s, h_cours, prix, dev, horodatage_recuperation, "yahoo")
             for id_s, prix, dev, h_cours in lignes],
        )
    conn.commit()


def ecrire_echecs(conn, manquants, horodatage, heure_utc):
    """manquants: liste de (id_societe, ticker_yahoo, code_http, detail_erreur)
    -- journalise les tickers attendus à cette heure mais pour lesquels Yahoo
    n'a renvoyé aucune donnée, même après repli individuel
    (telecharger_derniers_prix). code_http/detail_erreur (ajoutés le
    2026-10-03) permettent enfin de distinguer un rate-limit (429) d'un
    ticker structurellement cassé, sans avoir à rejouer le job pour deviner."""
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO cours_echecs (id_societe, ticker_yahoo, horodatage, heure_utc, code_http, detail_erreur)
            VALUES %s
            """,
            [(id_s, ticker, horodatage, heure_utc, code_http, detail_erreur)
             for id_s, ticker, code_http, detail_erreur in manquants],
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

        prix_par_ticker, echecs_detail = telecharger_derniers_prix(list(tickers_ouverts.keys()))

        lignes = []
        manquants = []
        for ticker, (id_societe, devise) in tickers_ouverts.items():
            info = prix_par_ticker.get(ticker)
            if info is None:
                code_http, detail_erreur = echecs_detail.get(ticker, (None, "échec sans détail"))
                manquants.append((id_societe, ticker, code_http, detail_erreur))
                continue
            prix, horodatage_cours = info
            lignes.append((id_societe, prix, devise, horodatage_cours))

        if lignes:
            ecrire_cours(conn, lignes, maintenant, maintenant.hour)
        if manquants:
            ecrire_echecs(conn, manquants, maintenant, maintenant.hour)

        print(f"✨ {len(lignes)} cours écrits, {len(manquants)} tickers sans donnée récupérée (après repli individuel).")
        if manquants:
            print(f"   Exemples de tickers manquants : {[(t, c, d) for _, t, c, d in manquants[:20]]}")

        if lignes:
            retards = sorted(
                ((maintenant - h_cours).total_seconds() / 60, id_s)
                for id_s, _, _, h_cours in lignes
            )
            retard_median = retards[len(retards) // 2][0]
            retard_max, pire_id = retards[-1]
            print(f"🕐 Retard Yahoo (run - heure réelle de la cotation) : "
                  f"médian {retard_median:.1f} min, max {retard_max:.1f} min ({pire_id}).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
