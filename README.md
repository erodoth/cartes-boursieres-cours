# Flux de cours en direct — cartes boursières

Récupère toutes les heures le dernier cours connu de chaque société/commodité/
crypto cotée à cette heure (via Yahoo Finance, `yfinance`), et l'écrit dans
Supabase (`cours_actuels` + `cours_historique`). Le score se calculant sur la
performance (variation relative), le prix est stocké tel quel dans sa devise
de cotation -- pas de conversion USD (précision du 2026-10-01 : la table
`taux_change` ne sert qu'au calcul de `capitalisation_usd`, un besoin distinct
déjà traité ailleurs dans le pipeline).

Ce job doit tourner **en dehors** de la session Claude (qui n'a pas d'accès
réseau vers Yahoo Finance) et idéalement en dehors de ton ordinateur (pour
qu'il tourne même ordinateur éteint). La solution retenue : **GitHub Actions**,
gratuit, avec un cron horaire.

## Mise en place (une seule fois)

1. **Crée un repo GitHub** (public ou privé, peu importe) et pousse-y tout le
   contenu de ce dossier (`recuperer_cours.py`, `requirements.txt`,
   `.github/workflows/cours.yml`).

   ```bash
   cd flux_cours_github
   git init
   git add .
   git commit -m "Flux de cours en direct"
   git branch -M main
   git remote add origin https://github.com/<ton-compte>/<ton-repo>.git
   git push -u origin main
   ```

2. **Récupère la chaîne de connexion Postgres de Supabase** :
   Dashboard Supabase → ton projet (`cartes-boursieres`) → *Project Settings*
   → *Database* → *Connection string* → onglet **URI** (prends la variante
   *Session pooler* ou *Transaction pooler*, pas la connexion directe, pour
   éviter les soucis IPv6 depuis les runners GitHub). Remplace `[YOUR-PASSWORD]`
   par le vrai mot de passe de la base.

3. **Ajoute-la comme secret GitHub** : sur la page du repo → *Settings* →
   *Secrets and variables* → *Actions* → *New repository secret* →
   nom `DATABASE_URL`, valeur = la chaîne de connexion de l'étape 2.

4. **Vérifie que les Actions sont activées** (onglet *Actions* du repo — GitHub
   les active par défaut). Le job se lance automatiquement toutes les heures à
   la minute 5. Tu peux aussi le lancer à la main : onglet *Actions* →
   *Récupération des cours* → *Run workflow*.

5. **Premier lancement manuel recommandé** pour vérifier que tout fonctionne
   avant de laisser tourner le cron (regarde les logs dans l'onglet Actions).

## Ce que fait le script à chaque run

- Détermine l'heure UTC courante et le jour de la semaine.
- Interroge Supabase pour savoir quels tickers sont censés coter à cet instant
  (sociétés dont la place est ouverte, commodités sauf maintenance CME 21h UTC
  et week-end, cryptos toujours).
- Télécharge leur dernier cours via `yfinance`, par paquets de 100 tickers
  (pause de 4s entre paquets, comme validé dans le script d'origine
  `analyse_yahoo.py`).
- Met à jour `cours_actuels` (upsert, un seul cours par carte) et ajoute une
  ligne dans `cours_historique` (append, pour garder l'historique complet).
  Le prix est stocké tel quel dans sa devise de cotation (`devise`, à titre
  informatif) -- pas de conversion USD.

## Limites connues / à surveiller

- **Yahoo Finance est une API non officielle.** Elle peut changer, se mettre à
  bloquer les IP des runners GitHub, ou devenir instable sans préavis. Si le
  job échoue soudainement en masse, c'est la première piste à vérifier.
- **Pas encore de contrôle d'anomalie à 25%** (prévu par les règles v5 mais
  non implémenté ici) : un cours Yahoo aberrant serait pris tel quel. À ajouter
  si besoin (ex. rejeter un nouveau cours qui varie de plus de 25% par rapport
  au précédent `cours_actuels`, et logguer l'anomalie au lieu de l'écrire).
- **RLS toujours désactivé** sur les tables Supabase (signalé précédemment) --
  ce script utilise la connexion Postgres directe (pas la clé `anon`), donc il
  n'est pas concerné, mais le site public le sera : à traiter avant mise en
  ligne.
