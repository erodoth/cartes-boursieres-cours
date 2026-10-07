# Chargeur des cours (chargeur_vps)

Programme qui télécharge les derniers cours Yahoo Finance de tous les titres dont la place est ouverte et les écrit dans
Supabase (`cours_actuels`, `cours_historique`, `cours_echecs`, `journal_chargements_cours`). Les scores horaires du jeu
(`calculer_scores_horaires`, cron à :00 UTC) se calculent à partir de ces cours.

## Où et quand il tourne

- **Serveur** : OVHcloud VPS-1, Ubuntu 24.04 (`vps-ac008e3c.vps.ovh.net`), code dans `/opt/chargeur`, secrets dans
  `/etc/chargeur.env` (`DATABASE_URL`), journal `/var/log/chargeur.log`.
- **Quand** : cron à **:40** de chaque heure (UTC), via `lancer.sh` (`flock` : jamais deux runs en même temps, `timeout` 30 min).
  Budget de téléchargement : 15 min (`BUDGET_SEC`), le run finit donc avant :55, avant le calcul des scores de :00.
- **Pourquoi :40** : les scores de l'heure H utilisent le dernier chargement antérieur à H:00. Retard Yahoo observé : US ~0 min,
  Europe ~15 min, Israël / Thaïlande ~40 min. Avec :40, un joueur qui regarde à :00 voit des cours vieux de 20 à 60 min.
- **GitHub Actions** (`erodoth/cartes-boursieres-cours`) : plus de lancement automatique, seulement `workflow_dispatch`
  (secours manuel si l'alerte `chargement_manquant` se déclenche).
- L'heure du journal est l'heure du lancement (le run de H:40 charge l'heure H). Si l'heure est déjà complète, un run
  répond « déjà complet » et s'arrête.

## Installation / mise à jour

```
sudo bash installer_chargeur.sh                         # une fois, en root
scp recuperer_cours.py ubuntu@vps-ac008e3c.vps.ovh.net:~/
sudo cp ~/recuperer_cours.py /opt/chargeur/recuperer_cours.py     # mise à jour du code
```

## Fonctionnement

1. `recuperer_tickers_ouverts` : tickers des places ouvertes à l'heure (`places.heures_ouvertes_utc`, `jours_ouvres`),
   commodités et cryptos. Un chargement de **clôture** est fait quand le marché était ouvert à H-1 et ne l'est plus à H.
2. Téléchargement par paquets de 100 (`yf.download`, 1 jour en bougies de 5 min), **écriture immédiate** après chaque paquet.
3. Replis en cascade pour les titres sans bougie du jour (comptés comme réussites) :
   5 jours / 5 min  ->  3 mois / 1 jour (dernière clôture connue, avec sa vraie date)  ->  repli individuel
   (plafond 60 par run, ignoré pour les tickers chroniques : >= 6 échecs sur 24 h et aucun cours depuis 5 jours).
4. **Cours d'ouverture** : l'Open de la première bougie du jour est écrit avec le cours (`prix_ouverture`,
   `horodatage_ouverture`). `calculer_variations_heure` part de l'ouverture quand la fenêtre chevauche l'ouverture de la séance
   (écart de la nuit / du week-end jamais compté).
5. Journal : une ligne par run dans `journal_chargements_cours` (attendus, écrits, échecs, complet). Le cron pg_cron
   `controle_chargement_cours` (:57) crée une alerte `chargement_manquant` s'il manque une heure.

## Points de vigilance

- **Deux sociétés ne doivent jamais avoir le même `ticker_yahoo`** : le programme utilise un dictionnaire par ticker, l'une des
  deux perdrait son cours en silence.
- Formats Yahoo par place : suffixe selon la bourse (`.PA`, `.L`, `.KS` / `.KQ` pour la Corée, `.KL` numérique pour la
  Malaisie, `.MX` sans tiret pour le Mexique, `.CL` pour la Colombie, `.CA` avec code ISIN `EGS…C0xx` pour l'Égypte, `.IR`
  pour Dublin ; Londres en pence = devise `GBp`).
- Suppression de sociétés : `begin; delete from exemplaires …; delete from cours_echecs/cours_historique/cours_actuels …;
  delete from societes …; commit;` (dans l'éditeur SQL Supabase : les DELETE via l'outil MCP expirent). Déplacer ou supprimer
  d'abord les lignes de `indices_composition`.
- Pour les joueurs : ne pas annoncer la latence exacte (arbitrage).

## Commandes utiles (sur le VPS)

```
tail -n 50 /var/log/chargeur.log
cat /etc/cron.d/chargeur
sudo /opt/chargeur/lancer.sh        # lancement manuel
```
