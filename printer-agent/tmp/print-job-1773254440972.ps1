
$ErrorActionPreference = 'Stop'

$browser = 'C:\Program Files\Google\Chrome\Application\chrome.exe'
$html = 'C:\bfc24-wms\printer-agent\tmp\wb-sticker-15.html'
$profile = 'C:\bfc24-wms\printer-agent\tmp\browser-profile'
$printer = 'Xprinter XP-D365B'

if (-not (Test-Path $browser)) {
  throw "Браузер не найден: $browser"
}

if (-not (Test-Path $html)) {
  throw "HTML для печати не найден: $html"
}

try {
  $ws = New-Object -ComObject WScript.Network
  $ws.SetDefaultPrinter($printer)
} catch {
  throw "Не удалось установить принтер по умолчанию: $printer"
}

$fileUrl = "file:///" + ($html -replace "\\","/")

$args = @(
  '--kiosk-printing',
  '--disable-popup-blocking',
  '--disable-print-preview',
  '--no-first-run',
  '--no-default-browser-check',
  "--user-data-dir=$profile",
  '--new-window',
  $fileUrl
)

$p = Start-Process -FilePath $browser -ArgumentList $args -PassThru

Start-Sleep -Seconds 6

try {
  if ($p -and -not $p.HasExited) {
    Stop-Process -Id $p.Id -Force
  }
} catch {}
