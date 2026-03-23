const axios = require("axios");
const fs = require("fs");
const { exec } = require("child_process");
const path = require("path");

const config = JSON.parse(
  fs.readFileSync(path.join(__dirname, "config.json"), "utf8")
);

const API = config.api_base_url;
const TOKEN = config.token;
const PRINTER_ID = Number(config.printer_id);
const POLL_INTERVAL = Number(config.poll_interval_ms || 1500);

console.log("BFC24 PRINTER AGENT STARTED");
console.log("PRINTER ID:", PRINTER_ID);

let isProcessing = false;

function execAsync(command, timeout = 30000) {
  return new Promise((resolve, reject) => {
    exec(command, { windowsHide: true, timeout }, (err, stdout, stderr) => {
      if (err) {
        err.stdout = stdout;
        err.stderr = stderr;
        return reject(err);
      }
      resolve({ stdout, stderr });
    });
  });
}

function escapePs(str) {
  return String(str || "").replace(/'/g, "''");
}

function parsePayload(rawPayload) {
  if (!rawPayload) return {};

  if (typeof rawPayload === "object") {
    return rawPayload;
  }

  if (typeof rawPayload === "string") {
    const clean = rawPayload.trim();
    if (!clean) return {};
    return JSON.parse(clean);
  }

  throw new Error("Некорректный payload_json");
}

function decodeStickerToSvg(payload) {
  if (!payload) return null;

  const raw = payload.wb_sticker || payload.base64 || null;
  if (!raw) return null;

  const clean = String(raw).trim();
  if (!clean) return null;

  if (clean.startsWith("<svg")) {
    return clean;
  }

  if (clean.startsWith("<?xml")) {
    return clean;
  }

  const commaIndex = clean.indexOf(",");
  const pureBase64 =
    clean.startsWith("data:") && commaIndex >= 0
      ? clean.slice(commaIndex + 1)
      : clean;

  try {
    const decoded = Buffer.from(pureBase64, "base64").toString("utf8");
    if (
      decoded.includes("<svg") ||
      decoded.includes("<?xml") ||
      decoded.includes("</svg>")
    ) {
      return decoded;
    }
    return null;
  } catch (e) {
    console.log("SVG DECODE ERROR:", e.message);
    return null;
  }
}

function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

async function markJob(jobId, status, errorText = null) {
  await axios.patch(
    `${API}/print-jobs/${jobId}`,
    {
      status,
      error_text: errorText
    },
    {
      headers: { Authorization: `Bearer ${TOKEN}` }
    }
  );
}

function buildPrintHtml(svgFilePath) {
  const svgFileUrl = "file:///" + svgFilePath.replace(/\\/g, "/");

  return `<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8" />
  <title>WB Sticker Print</title>
  <style>
    @page { size: auto; margin: 0; }
    html, body {
      margin: 0;
      padding: 0;
      background: #fff;
      width: 100%;
      height: 100%;
      overflow: hidden;
    }
    body {
      display: flex;
      align-items: flex-start;
      justify-content: flex-start;
    }
    img {
      display: block;
      width: auto;
      height: auto;
      max-width: none;
      max-height: none;
    }
  </style>
</head>
<body>
  <img id="sticker" src="${svgFileUrl}" alt="WB sticker" />
  <script>
    (function () {
      const img = document.getElementById("sticker");

      function doPrint() {
        setTimeout(() => {
          try { window.focus(); } catch(e) {}
          try { window.print(); } catch(e) {}
          setTimeout(() => {
            try { window.close(); } catch(e) {}
          }, 1500);
        }, 300);
      }

      if (img.complete) {
        doPrint();
      } else {
        img.onload = doPrint;
        img.onerror = function () {
          document.body.innerHTML = "<div style='padding:20px;font-family:sans-serif'>Ошибка загрузки стикера</div>";
        };
      }
    })();
  </script>
</body>
</html>`;
}

function getBrowserCandidates() {
  const candidates = [];

  if (config.browser_path) {
    candidates.push(config.browser_path);
  }

  candidates.push(
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
    "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
    "C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe",
    "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
    "C:\\Users\\admin\\AppData\\Local\\Yandex\\YandexBrowser\\Application\\browser.exe",
    "C:\\Users\\Администратор\\AppData\\Local\\Yandex\\YandexBrowser\\Application\\browser.exe",
    "C:\\Users\\Administrator\\AppData\\Local\\Yandex\\YandexBrowser\\Application\\browser.exe"
  );

  return candidates.filter(Boolean);
}

function resolveBrowserPath() {
  const candidates = getBrowserCandidates();

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }

  return null;
}

async function printHtmlSilently(htmlFilePath, printerName) {
  const browserPath = resolveBrowserPath();

  if (!browserPath) {
    throw new Error("Не найден Chrome/Edge/Yandex Browser для kiosk-печати");
  }

  const userDataDir = path.join(__dirname, "tmp", "browser-profile");
  ensureDir(userDataDir);

  const psScript = `
$ErrorActionPreference = 'Stop'

$browser = '${escapePs(browserPath)}'
$html = '${escapePs(htmlFilePath)}'
$profile = '${escapePs(userDataDir)}'
$printer = '${escapePs(printerName)}'

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

$fileUrl = "file:///" + ($html -replace "\\\\","/")

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
`;

  const psFilePath = path.join(__dirname, "tmp", `print-job-${Date.now()}.ps1`);
  fs.writeFileSync(psFilePath, psScript, "utf8");

  const command = `powershell -NoProfile -ExecutionPolicy Bypass -File "${psFilePath}"`;

  const result = await execAsync(command, 40000);

  if (result.stdout) {
    console.log("PRINT STDOUT:", result.stdout);
  }
  if (result.stderr) {
    console.log("PRINT STDERR:", result.stderr);
  }
}

async function processJob(job) {
  console.log("PRINT JOB FOUND:", job.id);

  await markJob(job.id, "processing");

  const payload = parsePayload(job.payload_json);
  const svgText = decodeStickerToSvg(payload);

  if (!svgText) {
    throw new Error("В print_job нет wb_sticker/base64");
  }

  const tempDir = path.join(__dirname, "tmp");
  ensureDir(tempDir);

  const svgFilePath = path.join(tempDir, `wb-sticker-${job.id}.svg`);
  const htmlFilePath = path.join(tempDir, `wb-sticker-${job.id}.html`);

  fs.writeFileSync(svgFilePath, svgText, "utf8");
  fs.writeFileSync(htmlFilePath, buildPrintHtml(svgFilePath), "utf8");

  const printerName =
    config.printer_name ||
    payload.printer_name ||
    "XPrinter Home";

  console.log("PRINT TO PRINTER:", printerName);
  console.log("SVG FILE:", svgFilePath);
  console.log("HTML FILE:", htmlFilePath);

  await printHtmlSilently(htmlFilePath, printerName);

  await markJob(job.id, "printed");
  console.log("PRINTED:", job.id);
}

async function checkJobs() {
  if (isProcessing) return;
  isProcessing = true;

  try {
    const res = await axios.get(`${API}/print-jobs`, {
      headers: { Authorization: `Bearer ${TOKEN}` }
    });

    const jobs = Array.isArray(res.data?.data) ? res.data.data : [];

    for (const job of jobs) {
      if (job.status !== "new") continue;
      if (Number(job.printer_id) !== PRINTER_ID) continue;

      try {
        await processJob(job);
      } catch (jobErr) {
        console.log("PRINT ERROR:", jobErr.message);
        try {
          await markJob(job.id, "error", jobErr.message);
        } catch (markErr) {
          console.log("MARK ERROR:", markErr.message);
        }
      }
    }
  } catch (err) {
    console.log("AGENT ERROR:", err.message);
  } finally {
    isProcessing = false;
  }
}

checkJobs();
setInterval(checkJobs, POLL_INTERVAL);