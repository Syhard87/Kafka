#Requires -Version 5.1
<#
  TP Kafka / Flink (3iL) - Étapes 3, 4 et 5 du guide d'installation, sous Windows :
  Python + confluent-kafka dans un environnement virtuel, téléchargement des images,
  puis tests Kafka / Kafka UI, Python vers Kafka et Flink, avec les deux captures à faire.

  Docker Desktop doit être lancé. Dans un PowerShell NORMAL (pas administrateur) :
      powershell -ExecutionPolicy Bypass -File C:\Dev\Kafka\scripts\2-tester-kit.ps1 -Kit C:\tp-kafka

  Options :
      -Kit C:\tp-kafka      dossier du kit (celui qui contient docker-compose.yml) ; par défaut le dossier courant
      -SansTelechargement   ne pas refaire le pull/build des images (si c'est déjà fait)
#>
param(
    [string]$Kit = (Get-Location).Path,
    [switch]$SansTelechargement
)

$ErrorActionPreference = 'Continue'
[Console]::OutputEncoding = [Text.Encoding]::UTF8
$env:PYTHONIOENCODING = 'utf-8'

function Ok($m)    { Write-Host "  [OK] $m" -ForegroundColor Green }
function Warn($m)  { Write-Host "  [!]  $m" -ForegroundColor Yellow }
function Err($m)   { Write-Host "  [X]  $m" -ForegroundColor Red }
function Titre($m) { Write-Host ""; Write-Host "=== $m ===" -ForegroundColor Cyan }
function Oui($question) { (Read-Host "  $question (O/N)") -match '^\s*[oOyY]' }

$bilan = [ordered]@{}
function Noter($etape, $etat) { $bilan[$etape] = $etat }

function Afficher-Bilan {
    Titre "Bilan"
    foreach ($e in $bilan.Keys) {
        switch ($bilan[$e]) {
            'OK'    { Ok $e }
            'KO'    { Err $e }
            default { Warn "$e : $($bilan[$e])" }
        }
    }
}

function Arreter($m) {
    Err $m
    Afficher-Bilan
    Pop-Location
    exit 1
}

$dossierCaptures = Join-Path (Split-Path $PSScriptRoot -Parent) 'captures'
New-Item -ItemType Directory -Force -Path $dossierCaptures | Out-Null

function Capture($numero, $quoi, $nomFichier) {
    Write-Host ""
    Write-Host "  >>> CAPTURE n°$numero : $quoi" -ForegroundColor Magenta
    Write-Host "      Win + Maj + S pour capturer, puis enregistre l'image sous :"
    Write-Host "      $(Join-Path $dossierCaptures $nomFichier)"
    [void](Read-Host "      Appuie sur Entrée quand c'est fait")
}

# --- Dossier du kit (étape 2) ------------------------------------------------
Titre "Dossier du kit (étape 2)"
$kitResolu = Resolve-Path -LiteralPath $Kit -ErrorAction SilentlyContinue
if (-not $kitResolu) {
    Err "Le dossier « $Kit » n'existe pas. Dézippe Kit_TP_Kafka_Flink.zip (dans C:\tp-kafka par exemple)."
    exit 1
}
$Kit = $kitResolu.Path
if (-not (Test-Path (Join-Path $Kit 'docker-compose.yml'))) {
    # Le zip crée souvent un sous-dossier : on le cherche un ou deux niveaux plus bas
    $trouve = Get-ChildItem -Path $Kit -Filter 'docker-compose.yml' -Recurse -Depth 2 -ErrorAction SilentlyContinue |
              Where-Object { Test-Path (Join-Path $_.DirectoryName 'generator.py') } |
              Select-Object -First 1
    if ($trouve) {
        $Kit = $trouve.DirectoryName
        Warn "docker-compose.yml trouvé dans le sous-dossier $Kit : on travaille là."
    }
}
foreach ($f in 'docker-compose.yml', 'generator.py', 'requirements.txt') {
    if (-not (Test-Path (Join-Path $Kit $f))) {
        Err "$f introuvable dans $Kit : ce n'est pas le dossier du kit. Indique-le avec -Kit C:\tp-kafka"
        exit 1
    }
}
if ($Kit -match '\s' -or $Kit -match '[^\x00-\x7F]') {
    Warn "Le chemin « $Kit » contient des espaces ou des accents : le guide demande un dossier simple, comme C:\tp-kafka."
}
Ok "Kit : $Kit"
Push-Location $Kit

# --- Docker (étape 1) --------------------------------------------------------
Titre "Docker (étape 1)"
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Arreter "Commande docker introuvable : installe Docker Desktop (script 1), puis ouvre un nouveau terminal."
}
docker info *> $null
if ($LASTEXITCODE -ne 0) {
    Arreter "Docker Desktop n'est pas lancé. Lance-le, attends que la baleine soit immobile, puis relance ce script."
}
docker compose version *> $null
if ($LASTEXITCODE -ne 0) { Arreter "« docker compose » ne répond pas : mets Docker Desktop à jour." }
Write-Host "  $(docker --version)"
Write-Host "  $(docker compose version)"
$hello = (docker run --rm hello-world 2>$null) -join "`n"
if ($hello -match 'Hello from Docker') {
    Ok "docker run hello-world affiche « Hello from Docker! »"
    Noter "Docker" 'OK'
} else {
    Arreter "docker run hello-world n'affiche pas « Hello from Docker! ». Voir la section Dépannage du guide."
}

# --- Python et confluent-kafka (étape 3) -------------------------------------
Titre "Python et confluent-kafka (étape 3)"
$venvPy = Join-Path $Kit '.venv\Scripts\python.exe'
if (-not (Test-Path $venvPy)) {
    $version = $null
    if (Get-Command py -ErrorAction SilentlyContinue) {
        foreach ($v in '3.12', '3.11') {
            py "-$v" -c "pass" *> $null
            if ($LASTEXITCODE -eq 0) { $version = $v; break }
        }
    }
    if (-not $version) {
        Warn "Python 3.12 (ou 3.11) introuvable."
        if ((Get-Command winget -ErrorAction SilentlyContinue) -and (Oui "Installer Python 3.12 avec winget ?")) {
            winget install -e --id Python.Python.3.12 --accept-package-agreements --accept-source-agreements
        } else {
            Write-Host "       Télécharge Python 3.12 sur python.org et coche « Add python.exe to PATH » au premier écran."
        }
        Arreter "Ouvre un NOUVEAU terminal (pour que Python soit trouvé), puis relance ce script."
    }
    Write-Host "  Création de l'environnement virtuel .venv avec Python $version..."
    py "-$version" -m venv .venv
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path $venvPy)) { Arreter "Impossible de créer l'environnement virtuel .venv." }
}
$venvVersion = & $venvPy -c "import sys; print('%d.%d' % sys.version_info[:2])"
if ($venvVersion -notin '3.11', '3.12') {
    Warn "L'environnement .venv utilise Python $venvVersion (le guide demande 3.11 ou 3.12). Si pip échoue : supprime le dossier .venv et relance."
}
# On appelle directement le Python du .venv : pas besoin d'« activate » dans le script
& $venvPy -m pip install --disable-pip-version-check -r requirements.txt
if ($LASTEXITCODE -ne 0) {
    Arreter "pip install a échoué. Cause fréquente : la version de Python. Installe Python 3.12, supprime le dossier .venv, relance ce script."
}
$versionCk = & $venvPy -c "import confluent_kafka; print(confluent_kafka.version()[0])"
if ($LASTEXITCODE -ne 0) { Arreter "« import confluent_kafka » échoue (message ci-dessus)." }
Ok "confluent-kafka $versionCk installé dans .venv"
& $venvPy generator.py --dry-run
if ($LASTEXITCODE -eq 0) {
    Ok "python generator.py --dry-run fonctionne"
    Noter "Python + confluent-kafka" 'OK'
} else {
    Arreter "python generator.py --dry-run a échoué (message ci-dessus)."
}

# --- Images Docker (étape 4) -------------------------------------------------
if ($SansTelechargement) {
    Noter "Images Docker" 'non retéléchargées (-SansTelechargement)'
} else {
    Titre "Téléchargement des images (étape 4) : 10 à 30 min la première fois"
    docker compose --profile flink --profile monitoring pull --ignore-buildable
    $pullOk = ($LASTEXITCODE -eq 0)
    docker compose --profile flink build
    if ($LASTEXITCODE -ne 0) {
        Warn "La construction de l'image Flink a échoué (souvent le téléchargement du connecteur Kafka). Nouvel essai..."
        docker compose --profile flink build
    }
    $buildOk = ($LASTEXITCODE -eq 0)
    if ($pullOk -and $buildOk) {
        Ok "Images téléchargées et image Flink construite"
        Noter "Images Docker" 'OK'
    } else {
        Arreter "Téléchargement ou construction en échec : vérifie ta connexion (sans VPN) et relance le script."
    }
}

# --- 5.1 Kafka et Kafka UI ---------------------------------------------------
Titre "5.1 Kafka et Kafka UI"
docker compose up -d kafka1 kafka2 kafka3 kafka-ui
if ($LASTEXITCODE -ne 0) {
    Arreter "Démarrage impossible. Si le message parle de « port is already allocated » : ferme le programme qui utilise ce port, ou lance « docker compose down »."
}
$services = 'kafka1', 'kafka2', 'kafka3', 'kafka-ui'
$limite = (Get-Date).AddMinutes(3)
$nomCluster = ''
do {
    Start-Sleep -Seconds 5
    $actifs = @(docker compose ps --status running --services 2>$null)
    $nbActifs = @($services | Where-Object { $actifs -contains $_ }).Count
    $brokers = 0
    try {
        # API de Kafka UI : liste des clusters avec leur nombre de brokers
        $clusters = Invoke-RestMethod -Uri 'http://localhost:8080/api/clusters' -TimeoutSec 5
        foreach ($c in @($clusters)) {
            if ([int]$c.brokerCount -gt $brokers) { $brokers = [int]$c.brokerCount; $nomCluster = $c.name }
        }
    } catch { }
    Write-Host "  ... conteneurs démarrés : $nbActifs/4, brokers vus par Kafka UI : $brokers/3"
} until (($nbActifs -eq 4 -and $brokers -ge 3) -or (Get-Date) -gt $limite)

if ($nbActifs -eq 4 -and $brokers -ge 3) {
    Ok "kafka1, kafka2, kafka3 et kafka-ui tournent ; Kafka UI voit le cluster $nomCluster avec $brokers brokers"
    Noter "Kafka (3 brokers) + Kafka UI" 'OK'
} elseif ($nbActifs -eq 4) {
    Warn "Les 4 conteneurs tournent, mais je n'ai pas pu lire le nombre de brokers : vérifie dans la page qui s'ouvre."
    Noter "Kafka (3 brokers) + Kafka UI" 'à vérifier sur http://localhost:8080'
} else {
    docker compose ps
    Arreter "Les conteneurs Kafka ne sont pas tous « running ». Regarde « docker compose logs kafka1 ». « Exited (137) » = mémoire insuffisante (voir .wslconfig)."
}
Start-Process 'http://localhost:8080'
Capture 1 "Kafka UI, cluster tp3il avec 3 brokers (http://localhost:8080)" 'capture1-kafka-ui.png'

# --- 5.2 Python vers Kafka ---------------------------------------------------
Titre "5.2 Python vers Kafka"
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic test-install --partitions 3 --replication-factor 3 --bootstrap-server kafka1:9092
if ($LASTEXITCODE -ne 0) {
    Err "Création du topic test-install impossible (message ci-dessus)."
    Noter "Python vers Kafka (50 messages)" 'KO'
} else {
    $sortie = @(& $venvPy generator.py --topic test-install --rate 10 --duration 5)
    $codeGen = $LASTEXITCODE
    foreach ($ligne in $sortie) { Write-Host "  $ligne" }
    docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --delete --topic test-install --bootstrap-server kafka1:9092
    if (($sortie -join "`n") -match '50 confirm\S*, 0 erreur') {
        Ok "50 messages envoyés, 50 confirmés, 0 erreurs"
        Noter "Python vers Kafka (50 messages)" 'OK'
    } elseif ($codeGen -eq 0) {
        Warn "Le générateur s'est terminé : vérifie que sa dernière ligne indique « 50 confirmés, 0 erreurs »."
        Noter "Python vers Kafka (50 messages)" 'à vérifier (dernière ligne du générateur)'
    } else {
        Err "Le générateur a échoué (message ci-dessus). Voir le Dépannage du guide."
        Noter "Python vers Kafka (50 messages)" 'KO'
    }
}

# --- 5.3 Flink ---------------------------------------------------------------
Titre "5.3 Flink"
docker compose --profile flink up -d
if ($LASTEXITCODE -ne 0) { Arreter "Démarrage de Flink impossible (message ci-dessus)." }
$limite = (Get-Date).AddMinutes(3)
do {
    Start-Sleep -Seconds 5
    try {
        # API REST de Flink : nombre de TaskManagers et de slots
        $vue = Invoke-RestMethod -Uri 'http://localhost:8081/overview' -TimeoutSec 5
        $tm = [int]$vue.taskmanagers
        $slots = [int]$vue.'slots-total'
    } catch { $tm = 0; $slots = 0 }
    Write-Host "  ... TaskManagers : $tm/1, slots : $slots/8"
} until (($tm -ge 1 -and $slots -gt 0) -or (Get-Date) -gt $limite)

if ($tm -ge 1) {
    if ($slots -eq 8) { Ok "Flink : $tm TaskManager, $slots slots" }
    else { Warn "Flink : $tm TaskManager, $slots slots (le guide en annonce 8)" }
    Noter "Flink (1 TaskManager)" 'OK'
} else {
    docker compose ps
    Arreter "L'interface Flink ne répond pas ou n'a pas de TaskManager. Regarde « docker compose logs jobmanager taskmanager »."
}
Start-Process 'http://localhost:8081'
Capture 2 "interface Flink avec 1 TaskManager et 8 slots (http://localhost:8081)" 'capture2-flink.png'

# --- Client SQL de Flink -----------------------------------------------------
Titre "Client SQL de Flink"
$tmp = Join-Path $env:TEMP 'tp-kafka-show-tables.sql'
[IO.File]::WriteAllText($tmp, "SHOW TABLES;`n", (New-Object Text.UTF8Encoding $false))
docker compose cp $tmp jobmanager:/tmp/show-tables.sql *> $null
$sql = (docker compose exec -T jobmanager ./bin/sql-client.sh -i /opt/sql/init.sql -f /tmp/show-tables.sql 2>&1) -join "`n"
Remove-Item $tmp -ErrorAction SilentlyContinue
if ($sql -match '\|\s*commandes\s*\|') {
    Ok "SHOW TABLES; affiche la table « commandes »"
    Noter "Client SQL Flink (table commandes)" 'OK'
} else {
    Warn "Je n'ai pas vu la table « commandes ». Fais le test à la main :"
    Write-Host "       docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/init.sql"
    Write-Host "       puis à l'invite Flink SQL> : SHOW TABLES;   puis   QUIT;"
    Noter "Client SQL Flink (table commandes)" 'à vérifier à la main'
}

# --- Fin (5.4) ---------------------------------------------------------------
Afficher-Bilan
Write-Host ""
Write-Host "  À envoyer à l'enseignant : les 2 captures du dossier $dossierCaptures" -ForegroundColor Magenta
Write-Host ""
if (Oui "Arrêter les conteneurs maintenant ? Les données sont conservées") {
    docker compose --profile flink --profile monitoring stop
}
Pop-Location
