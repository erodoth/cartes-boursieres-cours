#!/usr/bin/env bash
# Installation du chargeur de cours sur un VPS Ubuntu 24.04 (OVHcloud VPS-1).
# À lancer UNE FOIS, en root :  sudo bash installer_chargeur.sh
# Prérequis : recuperer_cours.py et requirements.txt dans le MÊME dossier que ce script.
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then echo "Lancez ce script avec sudo."; exit 1; fi
DOSSIER_SCRIPT="$(cd "$(dirname "$0")" && pwd)"
for f in recuperer_cours.py requirements.txt; do
  [ -f "$DOSSIER_SCRIPT/$f" ] || { echo "Fichier manquant : $f (à copier à côté de ce script)"; exit 1; }
done

echo "== 1/6 Paquets système, heure UTC =="
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y python3 python3-venv python3-pip cron util-linux
timedatectl set-timezone UTC
timedatectl set-ntp true || true

echo "== 2/6 Dossier et environnement Python =="
mkdir -p /opt/chargeur
cp "$DOSSIER_SCRIPT/recuperer_cours.py" "$DOSSIER_SCRIPT/requirements.txt" /opt/chargeur/
python3 -m venv /opt/chargeur/venv
/opt/chargeur/venv/bin/pip install --upgrade pip
/opt/chargeur/venv/bin/pip install -r /opt/chargeur/requirements.txt

echo "== 3/6 Fichier de secrets (DATABASE_URL), lisible par root seulement =="
if [ ! -f /etc/chargeur.env ]; then
  echo "Collez la valeur de DATABASE_URL (rien ne s'affiche pendant la saisie), puis Entrée :"
  read -r -s URL
  echo
  [ -n "$URL" ] || { echo "Valeur vide, abandon."; exit 1; }
  printf 'DATABASE_URL=%s\n' "$URL" > /etc/chargeur.env
  unset URL
fi
chmod 600 /etc/chargeur.env
chown root:root /etc/chargeur.env

echo "== 4/6 Script de lancement (un seul run à la fois, 30 min maximum) =="
cat > /opt/chargeur/lancer.sh <<'EOF'
#!/usr/bin/env bash
# Lancé par cron. flock : si un run précédent tourne encore, on n'en démarre pas un second.
set -a
. /etc/chargeur.env
set +a
exec /usr/bin/flock -n /var/lock/chargeur.lock \
  /usr/bin/timeout 30m \
  /opt/chargeur/venv/bin/python /opt/chargeur/recuperer_cours.py >> /var/log/chargeur.log 2>&1
EOF
chmod 755 /opt/chargeur/lancer.sh
touch /var/log/chargeur.log

echo "== 5/6 Planification : à :40 de chaque heure (heure UTC) =="
cat > /etc/cron.d/chargeur <<'EOF'
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
# Le chargement de l'heure H démarre à H:40 (cours Yahoo différés de 15-20 min, fini avant le calcul des scores de H+1:00). GitHub : lancement manuel uniquement.
40 * * * * root /opt/chargeur/lancer.sh
EOF
chmod 644 /etc/cron.d/chargeur

cat > /etc/logrotate.d/chargeur <<'EOF'
/var/log/chargeur.log {
  weekly
  rotate 4
  compress
  missingok
  notifempty
  copytruncate
}
EOF

echo "== 6/6 Terminé =="
echo "Test manuel (optionnel) :  sudo /opt/chargeur/lancer.sh && tail -n 30 /var/log/chargeur.log"
echo "Suivi en direct :          tail -f /var/log/chargeur.log"
echo "Prochain passage automatique : à :05 de l'heure prochaine (UTC). Heure actuelle du serveur : $(date -u '+%H:%M UTC')"
