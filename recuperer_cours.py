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

« Sans transaction » (ajouté le 2026-10-06) : une valeur peu liquide qui n'a pas échangé aujourd'hui n'a aucun chandelier
de 5 minutes pour period="1d" (Yahoo répond « vide ») alors qu'elle a bien un cours. Ces tickers sont désormais retrouvés par
un téléchargement groupé sur 5 jours, comptés comme réussis (« dont N ticker(s) sans transaction aujourd'hui » dans le
journal) et non plus comme échecs ni retentés un par un.

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
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg2
import psycopg2.extras
import yfinance as yf

TAILLE_PAQUET = 100
PAUSE_ENTRE_PAQUETS_SEC = 4.0
HEURE_MAINTENANCE_COMMODITIES = 21  # pause quotidienne CME Globex, 21h-22h UTC
JOURS_FR = ["lun", "mar", "mer", "jeu", "ven", "sam", "dim"]

# 2026-10-05 -- fiabilisation (des heures entières étaient perdues : le run téléchargeait TOUT puis n'écrivait
# qu'à la fin, donc un run arrêté par la limite de 30 min de GitHub n'écrivait rien).
BUDGET_SEC = 22 * 60            # au-delà, on arrête de télécharger et on écrit ce qu'on a (limite GitHub : 40 min)
MAX_REPLIS_INDIVIDUELS = 60     # plafond de repli un par un par run (chacun coûte 5 à 12 s)
SEUIL_CHRONIQUE = 6             # >= 6 échecs sur 24 h ET aucun cours depuis 5 jours => ticker « chronique »


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


def ouvrir_connexion(database_url):
    """Connexion Postgres courte durée (keepalive actif). On n'en garde JAMAIS une ouverte pendant les
    téléchargements Yahoo (plusieurs minutes d'inactivité) : le pooleur peut la couper."""
    return psycopg2.connect(
        database_url, keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5, connect_timeout=20,
    )


def deja_complet(conn, heure_ref: datetime) -> bool:
    """Vrai si un chargement COMPLET existe déjà pour cette heure (le 2e passage de rattrapage s'arrête alors)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM journal_chargements_cours WHERE heure_utc = %s AND complet LIMIT 1", (heure_ref,)
        )
        return cur.fetchone() is not None


def tickers_chroniques(conn) -> set:
    """Tickers en échec permanent : >= SEUIL_CHRONIQUE échecs sur 24 h et aucun cours depuis 5 jours.
    Ils restent dans le téléchargement groupé (gratuit s'ils répondent) mais n'ont plus de repli individuel."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT e.ticker_yahoo
            FROM cours_echecs e
            WHERE e.horodatage >= now() - interval '24 hours'
            GROUP BY e.id_societe, e.ticker_yahoo
            HAVING count(*) >= %s
               AND NOT EXISTS (
                   SELECT 1 FROM cours_historique h
                   WHERE h.id_societe = e.id_societe AND h.horodatage_recuperation >= now() - interval '5 days'
               )
            """,
            (SEUIL_CHRONIQUE,),
        )
        return {r[0] for r in cur.fetchall()}


def journaliser(conn, heure_ref, demarre_le, nb_attendus, nb_ecrits, nb_echecs, nb_non_traites, complet, remarque=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO journal_chargements_cours
                (heure_utc, demarre_le, nb_attendus, nb_ecrits, nb_echecs, nb_non_traites, complet, remarque)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (heure_ref, demarre_le, nb_attendus, nb_ecrits, nb_echecs, nb_non_traites, complet, remarque),
        )
    conn.commit()


def recuperer_tickers_ouverts(conn, maintenant_utc: datetime):
    """Retourne ({ticker_yahoo: (id_societe, devise)}, {tickers de clôture}).

    Le dictionnaire contient tout ce qui doit être coté à l'heure UTC courante, PLUS (2026-10-06) les tickers dont le
    marché vient de fermer (ouvert à l'heure précédente, plus ouvert maintenant) : ce chargement « de clôture » capte
    le dernier cours de la séance, que les chargements pendant la séance ne voient pas (le dernier tombe à :11 de la
    dernière heure ouverte, soit 35 à 50 min avant la fermeture). Le 2e élément du tuple liste ces tickers de clôture."""
    heure = maintenant_utc.hour
    jour_idx = maintenant_utc.weekday()  # 0 = lundi
    precedente = maintenant_utc - timedelta(hours=1)
    heure_prec, jour_prec = precedente.hour, precedente.weekday()
    tickers = {}
    cloture = set()

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
            ouvert_maintenant = heure in heures_ouvertes and jour_ouvre(jours_ouvres, jour_idx)
            vient_de_fermer = (
                not ouvert_maintenant and heure_prec in heures_ouvertes and jour_ouvre(jours_ouvres, jour_prec)
            )
            if ouvert_maintenant:
                tickers[ticker_yahoo] = (id_societe, devise)
            elif vient_de_fermer:
                tickers[ticker_yahoo] = (id_societe, devise)
                cloture.add(ticker_yahoo)

        # Commodities : toutes les heures sauf maintenance CME Globex (21h UTC) et le samedi (jour_idx == 5).
        # À 21h (maintenance) on les charge quand même UNE fois si l'heure précédente était ouverte : c'est leur clôture.
        commodites_ouvertes = heure != HEURE_MAINTENANCE_COMMODITIES and jour_idx != 5
        commodites_cloture = heure == HEURE_MAINTENANCE_COMMODITIES and jour_prec != 5
        if commodites_ouvertes or commodites_cloture:
            cur.execute(
                "SELECT id_societe, ticker_yahoo FROM commodites WHERE ticker_yahoo IS NOT NULL"
            )
            for id_societe, ticker_yahoo in cur.fetchall():
                tickers[ticker_yahoo] = (id_societe, "USD")
                if commodites_cloture:
                    cloture.add(ticker_yahoo)

        # Cryptos : 24/7
        cur.execute("SELECT id_societe, ticker_yahoo FROM cryptos WHERE ticker_yahoo IS NOT NULL")
        for id_societe, ticker_yahoo in cur.fetchall():
            tickers[ticker_yahoo] = (id_societe, "USD")

    return tickers, cloture


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


def telecharger_sans_transaction(tickers):
    """2026-10-06 : pour des tickers sans AUCUN chandelier aujourd'hui (valeur peu liquide qui n'a pas échangé :
    Yahoo répond « vide » pour period="1d" alors que le titre a bien un cours, celui de sa dernière transaction),
    récupère la dernière cotation connue sur 5 jours en UN téléchargement groupé.

    Retourne {ticker: (prix, horodatage_cours)} pour ceux qui ont une cotation récente ; les autres (aucune donnée
    sur 5 jours) restent de vrais échecs et passent au repli individuel."""
    trouves = {}
    if not tickers:
        return trouves
    try:
        data = yf.download(tickers, period="5d", interval="5m", progress=False, group_by="ticker", threads=True)
    except Exception as e:
        print(f"  ⚠️  Recherche de la dernière cotation sur 5 jours échouée : {type(e).__name__}: {e}")
        return trouves
    for ticker in tickers:
        try:
            serie = _extraire_serie(data, ticker, len(tickers))
            if serie is None:
                continue
            horodatage_cours = serie.index[-1]
            if horodatage_cours.tzinfo is None:
                horodatage_cours = horodatage_cours.tz_localize("UTC")
            else:
                horodatage_cours = horodatage_cours.tz_convert("UTC")
            trouves[ticker] = (float(serie.iloc[-1]), horodatage_cours.to_pydatetime())
        except Exception:
            continue
    return trouves


def telecharger_derniers_prix(liste_tickers, deadline=None, chroniques=frozenset(), ecrire_lot=None):
    """Télécharge par paquets de 100 et retourne (resultats, echecs_detail, non_traites, sans_transaction) où
    resultats = {ticker: (prix, horodatage_cours)} (horodatage_cours = heure
    RÉELLE de la bougie Yahoo en UTC, PAS l'heure à laquelle ce script tourne
    -- Yahoo peut avoir jusqu'à 15-20 min de retard selon la place),
    echecs_detail = {ticker: (code_http, detail_erreur)} pour tout ticker
    resté sans prix, même après repli individuel, et sans_transaction = ensemble des tickers qui ont un cours
    mais n'ont pas échangé aujourd'hui (cours ancien, compté comme réussite et non comme échec).

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
    non_traites = []   # tickers laissés de côté faute de temps
    sans_transaction = set()
    nb_replis = 0
    paquets = [liste_tickers[i:i + TAILLE_PAQUET] for i in range(0, len(liste_tickers), TAILLE_PAQUET)]
    print(f"📦 {len(liste_tickers)} tickers à jour, {len(paquets)} paquet(s) de {TAILLE_PAQUET} max.")

    for i, paquet in enumerate(paquets):
        if deadline is not None and time.monotonic() > deadline:
            # Budget de temps épuisé : on n'attaque pas ce paquet ni les suivants, le run écrit ce qu'il a.
            for reste in paquets[i:]:
                non_traites.extend(reste)
            print(f"  ⏱️  Budget de temps dépassé : {len(non_traites)} ticker(s) non traités (paquets {i + 1} à {len(paquets)}).")
            break
        print(f"  ➔ Paquet {i + 1}/{len(paquets)} ({len(paquet)} tickers)...")
        lot = {}
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
                        resultats[ticker] = lot[ticker] = (float(serie.iloc[-1]), horodatage_cours.to_pydatetime())
                    else:
                        manquants_paquet.append(ticker)
                except Exception:
                    manquants_paquet.append(ticker)

        # Paquet bien reçu mais certains tickers sans chandelier du jour : ce sont en général des valeurs qui n'ont pas
        # échangé aujourd'hui (pas une panne). On cherche leur dernière cotation sur 5 jours AVANT tout repli individuel.
        # Si le paquet entier est vide (429 probable), on ne tente rien de tel : c'est une vraie panne, repli habituel.
        if manquants_paquet and data is not None and not data.empty and (deadline is None or time.monotonic() <= deadline):
            trouves = telecharger_sans_transaction(manquants_paquet)
            for ticker, valeur in trouves.items():
                resultats[ticker] = lot[ticker] = valeur
                sans_transaction.add(ticker)
            if trouves:
                print(f"  💤 {len(trouves)} ticker(s) du paquet {i + 1} sans transaction aujourd'hui (dernier cours conservé).")
            manquants_paquet = [t for t in manquants_paquet if t not in trouves]

        if manquants_paquet:
            print(f"  🔁 {len(manquants_paquet)} ticker(s) du paquet {i + 1} sans donnée -- repli individuel...")
            for ticker in manquants_paquet:
                # Pas de repli individuel pour les tickers chroniques, au-delà du plafond, ni hors budget de temps.
                if ticker in chroniques:
                    echecs_detail[ticker] = (None, "ticker en échec chronique : repli individuel ignoré")
                    continue
                if nb_replis >= MAX_REPLIS_INDIVIDUELS or (deadline is not None and time.monotonic() > deadline):
                    echecs_detail[ticker] = (None, "repli individuel ignoré (plafond ou budget de temps du run)")
                    continue
                nb_replis += 1
                try:
                    prix, horodatage_cours, erreur = telecharger_un_ticker(ticker)
                except Exception as e:  # filet de sécurité : un ticker ne fait jamais tomber le run
                    prix, horodatage_cours, erreur = None, None, (None, f"erreur inattendue : {type(e).__name__}: {e}"[:500])
                if prix is not None:
                    resultats[ticker] = lot[ticker] = (prix, horodatage_cours)
                    print(f"     ✅ {ticker} récupéré individuellement (le paquet l'avait perdu).")
                else:
                    echecs_detail[ticker] = erreur or derniere_erreur_paquet or (None, "échec sans détail")
                time.sleep(1)

        # Écriture IMMÉDIATE du paquet : un arrêt du run (limite de temps, panne) ne fait plus perdre l'heure entière.
        if lot and ecrire_lot is not None:
            try:
                ecrire_lot(lot)
            except Exception as e:  # une écriture ratée ne doit pas arrêter les autres paquets
                print(f"  ❌ Écriture du paquet {i + 1} échouée : {type(e).__name__}: {e}")
                for t in lot:
                    resultats.pop(t, None)
                    sans_transaction.discard(t)
                    echecs_detail[t] = (None, f"écriture en base échouée : {type(e).__name__}"[:500])

        if i + 1 < len(paquets):
            time.sleep(PAUSE_ENTRE_PAQUETS_SEC)

    return resultats, echecs_detail, non_traites, sans_transaction


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

    debut = time.monotonic()
    deadline = debut + BUDGET_SEC
    maintenant = datetime.now(timezone.utc)
    heure_ref = maintenant.replace(minute=0, second=0, microsecond=0)
    print(f"⚡ Run {maintenant.isoformat()} (heure UTC {maintenant.hour}, jour {JOURS_FR[maintenant.weekday()]})")

    # Connexion courte : lecture de la liste des tickers, puis fermée avant les téléchargements.
    conn = ouvrir_connexion(database_url)
    try:
        if deja_complet(conn, heure_ref):
            print("✅ Chargement déjà complet pour cette heure -- rien à faire (passage de rattrapage).")
            return
        tickers_ouverts, tickers_cloture = recuperer_tickers_ouverts(conn, maintenant)
        if not tickers_ouverts:
            print("⚠️  Aucun ticker ouvert à cette heure -- rien à faire.")
            journaliser(conn, heure_ref, maintenant, 0, 0, 0, 0, True, "aucun ticker ouvert")
            return
        chroniques = tickers_chroniques(conn)
        # Passage de rattrapage après un run incomplet : on ne retélécharge pas ce qui est déjà écrit pour cette heure
        # (sinon le rattrapage refait les mêmes premiers paquets et n'atteint jamais la fin de la liste).
        with conn.cursor() as cur:
            cur.execute("SELECT id_societe FROM cours_actuels WHERE horodatage_recuperation >= %s", (heure_ref,))
            deja_ecrits = {r[0] for r in cur.fetchall()}
        if deja_ecrits:
            avant = len(tickers_ouverts)
            tickers_ouverts = {t: v for t, v in tickers_ouverts.items() if v[0] not in deja_ecrits}
            print(f"↩️  Rattrapage : {avant - len(tickers_ouverts)} ticker(s) déjà écrits pour cette heure, {len(tickers_ouverts)} restant(s).")
            if not tickers_ouverts:
                journaliser(conn, heure_ref, maintenant, 0, 0, 0, 0, True, "rattrapage : tout était déjà écrit")
                return
    finally:
        conn.close()
    print(f"ℹ️  {len(tickers_ouverts)} tickers attendus, dont {len(tickers_cloture & set(tickers_ouverts))} de clôture"
          f" et {len(chroniques & set(tickers_ouverts))} en échec chronique.")
    nb_cloture = len(tickers_cloture & set(tickers_ouverts))

    nb_ecrits = 0

    def ecrire_lot(lot):
        """Écrit les cours d'un paquet avec une connexion neuve (2 tentatives)."""
        nonlocal nb_ecrits
        lignes_lot = [(tickers_ouverts[t][0], prix, tickers_ouverts[t][1], h_cours) for t, (prix, h_cours) in lot.items()]
        for tentative in range(2):
            try:
                c = ouvrir_connexion(database_url)
                try:
                    ecrire_cours(c, lignes_lot, maintenant, maintenant.hour)
                finally:
                    c.close()
                nb_ecrits += len(lignes_lot)
                return
            except Exception:
                if tentative == 1:
                    raise
                time.sleep(3)

    prix_par_ticker, echecs_detail, non_traites, sans_transaction = telecharger_derniers_prix(
        list(tickers_ouverts.keys()), deadline=deadline, chroniques=chroniques, ecrire_lot=ecrire_lot,
    )
    nb_sans_transaction = len(sans_transaction)

    manquants = []
    ensemble_non_traites = set(non_traites)
    for ticker, (id_societe, devise) in tickers_ouverts.items():
        if ticker in prix_par_ticker:
            continue
        if ticker in ensemble_non_traites:
            manquants.append((id_societe, ticker, None, "non téléchargé : budget de temps du run dépassé"))
            continue
        code_http, detail_erreur = echecs_detail.get(ticker, (None, "échec sans détail"))
        manquants.append((id_societe, ticker, code_http, detail_erreur))

    complet = not non_traites
    conn = ouvrir_connexion(database_url)
    try:
        if manquants:
            ecrire_echecs(conn, manquants, maintenant, maintenant.hour)
        journaliser(
            conn, heure_ref, maintenant, len(tickers_ouverts), nb_ecrits, len(manquants) - len(non_traites),
            len(non_traites), complet,
            " ; ".join(
                x for x in (
                    f"dont {nb_cloture} ticker(s) de clôture" if nb_cloture else "",
                    f"dont {nb_sans_transaction} ticker(s) sans transaction aujourd'hui" if nb_sans_transaction else "",
                    "" if complet else f"incomplet : budget de {BUDGET_SEC // 60} min dépassé",
                ) if x
            ) or None,
        )
    finally:
        conn.close()

    print(f"✨ {nb_ecrits} cours écrits (dont {nb_sans_transaction} sans transaction aujourd'hui), "
          f"{len(manquants)} tickers sans donnée récupérée"
          f" ({len(non_traites)} non traités faute de temps). Durée {time.monotonic() - debut:.0f}s.")
    if manquants:
        print(f"   Exemples de tickers manquants : {[(t, c, d) for _, t, c, d in manquants[:20]]}")

    # Les tickers « sans transaction » ont un cours vieux de plusieurs heures ou jours : on les écarte des statistiques de retard.
    prix_recents = {t: v for t, v in prix_par_ticker.items() if t not in sans_transaction}
    if prix_recents:
        retards = sorted(
            ((maintenant - h_cours).total_seconds() / 60, tickers_ouverts[t][0])
            for t, (_, h_cours) in prix_recents.items()
        )
        retard_median = retards[len(retards) // 2][0]
        retard_max, pire_id = retards[-1]
        print(f"🕐 Retard Yahoo (run - heure réelle de la cotation) : "
              f"médian {retard_median:.1f} min, max {retard_max:.1f} min ({pire_id}).")


if __name__ == "__main__":
    main()
