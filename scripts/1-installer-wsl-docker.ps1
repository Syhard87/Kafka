#Requires -Version 5.1
<#
  TP Kafka / Flink (3iL) - Étape 1 du guide d'installation, sous Windows :
  vérifie la machine, installe ou met à jour WSL 2, écrit le fichier .wslconfig
  et installe Docker Desktop si besoin.

  À lancer dans un PowerShell ouvert EN ADMINISTRATEUR :
      powershell -ExecutionPolicy Bypass -File C:\Dev\Kafka\scripts\1-installer-wsl-docker.ps1

  Le script ne redémarre jamais l'ordinateur tout seul : il dit quand le faire.
#>

$ErrorActionPreference = 'Continue'
$env:WSL_UTF8 = '1'   # sans ça, wsl.exe écrit en UTF-16 et la sortie est illisible

function Ok($m)    { Write-Host "  [OK] $m" -ForegroundColor Green }
function Warn($m)  { Write-Host "  [!]  $m" -ForegroundColor Yellow }
function Err($m)   { Write-Host "  [X]  $m" -ForegroundColor Red }
function Titre($m) { Write-Host ""; Write-Host "=== $m ===" -ForegroundColor Cyan }
function Oui($question) { (Read-Host "  $question (O/N)") -match '^\s*[oOyY]' }

$redemarrer = $false

# --- Droits administrateur ---------------------------------------------------
$identite = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
if (-not $identite.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Err "Ce script doit être lancé dans un PowerShell ouvert en administrateur."
    Write-Host "       Clic droit sur PowerShell > « Exécuter en tant qu'administrateur », puis relance la commande."
    exit 1
}

# --- Machine -----------------------------------------------------------------
Titre "Vérification de la machine"
$systeme = Get-CimInstance Win32_ComputerSystem
$cpu     = Get-CimInstance Win32_Processor | Select-Object -First 1
$ramGo   = [math]::Round($systeme.TotalPhysicalMemory / 1GB, 1)
$libreGo = [math]::Round((Get-PSDrive -Name C).Free / 1GB, 1)

if ($ramGo -lt 7.5)    { Err  "Mémoire : $ramGo Go, moins que les 8 Go demandés. Préviens l'enseignant avant le premier cours (travail en binôme)." }
elseif ($ramGo -lt 15) { Warn "Mémoire : $ramGo Go. Ça passe, mais ferme le navigateur et les applis gourmandes pendant les TP." }
else                   { Ok   "Mémoire : $ramGo Go" }

if ($libreGo -lt 20) { Err "Disque C: : $libreGo Go libres, il en faut 20. Fais de la place avant de continuer." }
else                 { Ok  "Disque C: : $libreGo Go libres" }

# HypervisorPresent = virtualisation déjà utilisée par Windows ; sinon on regarde le BIOS
if ($systeme.HypervisorPresent -or $cpu.VirtualizationFirmwareEnabled) {
    Ok "Virtualisation activée"
} else {
    Err "Virtualisation désactivée. Active-la dans le BIOS (option Intel VT-x, AMD-V, SVM ou « Virtualization »), puis relance ce script."
    exit 1
}

# --- WSL 2 -------------------------------------------------------------------
Titre "WSL 2"
if (-not (Get-Command wsl.exe -ErrorAction SilentlyContinue)) {
    Err "wsl.exe introuvable : fais les mises à jour Windows (Windows 10 version 2004 ou plus récente), puis relance ce script."
    exit 1
}
wsl.exe --status *> $null
if ($LASTEXITCODE -eq 0) {
    Ok "WSL est déjà installé : mise à jour et version 2 par défaut"
    wsl.exe --update
    wsl.exe --set-default-version 2
} else {
    Write-Host "  Installation de WSL 2 (quelques minutes)..."
    # Docker n'a pas besoin de distribution Linux ; les anciens WSL ne connaissent pas l'option
    wsl.exe --install --no-distribution
    if ($LASTEXITCODE -ne 0) { wsl.exe --install }
    if ($LASTEXITCODE -eq 0) {
        Ok "WSL 2 installé"
        $redemarrer = $true
    } else {
        Err "L'installation de WSL a échoué. Lance « wsl --install » à la main et lis le message."
        exit 1
    }
}

# --- .wslconfig --------------------------------------------------------------
Titre "Fichier .wslconfig (mémoire allouée à Docker)"
$memoire = if ($ramGo -lt 10) { '5GB' } else { '6GB' }   # guide : 5GB sur un PC de 8 Go, 6GB sinon
$coeurs  = [math]::Min(4, [int]$systeme.NumberOfLogicalProcessors)
$contenu = "[wsl2]`r`nmemory=$memoire`r`nprocessors=$coeurs`r`n"
$fichier = Join-Path $env:USERPROFILE '.wslconfig'
$wslconfigModifie = $false

if ((Test-Path $fichier) -and ((Get-Content $fichier -Raw) -eq $contenu)) {
    Ok "$fichier est déjà correct"
} else {
    if (Test-Path $fichier) {
        Copy-Item $fichier "$($fichier).sauvegarde" -Force
        Warn "Un .wslconfig existait déjà : l'ancien est gardé dans $($fichier).sauvegarde"
    }
    # Écrit directement le bon nom, sans le « .txt » qu'ajoute le Bloc-notes
    [IO.File]::WriteAllText($fichier, $contenu, [Text.Encoding]::ASCII)
    Ok "$fichier écrit : memory=$memoire, processors=$coeurs"
    $wslconfigModifie = $true
}
if (Test-Path "$($fichier).txt") {
    Warn "Le fichier $($fichier).txt est ignoré par WSL (mauvais nom) : tu peux le supprimer."
}

# --- Docker Desktop ----------------------------------------------------------
Titre "Docker Desktop"
$dockerDesktop = Join-Path $env:ProgramFiles 'Docker\Docker\Docker Desktop.exe'
if (Test-Path $dockerDesktop) {
    Ok "Docker Desktop est déjà installé"
} elseif ((Get-Command winget -ErrorAction SilentlyContinue) -and (Oui "Docker Desktop n'est pas installé. L'installer maintenant avec winget ?")) {
    winget install -e --id Docker.DockerDesktop --accept-package-agreements --accept-source-agreements
    if ($LASTEXITCODE -eq 0) {
        Ok "Docker Desktop installé"
        $redemarrer = $true
    } else {
        Err "winget a échoué : télécharge « Docker Desktop for Windows » sur docker.com et installe-le à la main."
    }
} else {
    Warn "Télécharge « Docker Desktop for Windows » sur docker.com et installe-le en gardant « Use WSL 2 instead of Hyper-V »."
}

# --- Suite -------------------------------------------------------------------
Titre "Et maintenant"
if ($redemarrer) {
    Write-Host "  1. REDÉMARRE l'ordinateur maintenant." -ForegroundColor Yellow
} elseif ($wslconfigModifie) {
    Write-Host "  1. Pour appliquer le .wslconfig : quitte Docker Desktop, tape « wsl --shutdown », puis relance Docker Desktop."
} else {
    Write-Host "  1. Rien à redémarrer."
}
Write-Host "  2. Lance Docker Desktop : accepte les conditions, « Skip » pour la création de compte."
Write-Host "     Attends que l'icône de baleine (en bas à droite) soit immobile."
Write-Host "  3. Dézippe Kit_TP_Kafka_Flink.zip (fourni par l'enseignant) dans C:\tp-kafka"
Write-Host "  4. Dans un PowerShell NORMAL (pas administrateur) :"
Write-Host "       powershell -ExecutionPolicy Bypass -File `"$PSScriptRoot\2-tester-kit.ps1`" -Kit C:\tp-kafka"
