const axios = require("axios");
const fs = require("fs");
const { exec } = require("child_process");
const path = require("path");
const { print } = require("pdf-to-printer");

const configPath = path.join(__dirname, "config.json");
const config = JSON.parse(fs.readFileSync(configPath, "utf8"));

const API = config.api_base_url;
const TOKEN = config.token;
const PRINTER_ID = config.printer_id;
const POLL_INTERVAL = config.poll_interval_ms || 1500;
const PRINTER_NAME = config.printer_name || "Xprinter XP-D365B";

const CHROME_PATH =
  config.chrome_path ||
  "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";

console.log("BFC24 PRINTER AGENT STARTED");
console.log("PRINTER ID:", PRINTER_ID);
console.log("PRINTER NAME:", PRINTER_NAME);
console.log("CHROME PATH:", CHROME_PATH);

let isProcessing = false;

function execAsync(command) {
  return new Promise((resolve, reject) => {
    exec(command, { windowsHide: true, maxBuffer: 1024 * 1024 * 20 }, (err, stdout, stderr) => {
      if (err) {
        err.stdout = stdout;
        err.stderr = stderr;
        return reject(err);
      }
      resolve({ stdout, stderr });
    });
  });
}

function safeText(value) {
  if (value == null) return "";
  return String(value);
}

function getReadableError(err) {
  if (!err) return "Неизвестная ошибка";

  const parts = [];

  if (err.message) parts.push(String(err.message));
  if (err.stderr) parts.push(String(err.stderr).trim());
  if (err.stdout) parts.push(String(err.stdout).trim());

  return parts.filter(Boolean).join("\n").trim() || "Неизвестная ошибка";
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

function normalizePayload(payloadJson) {
  if (!payloadJson) return {};

  if (typeof payloadJson === "object") {
    return payloadJson;
  }

  if (typeof payloadJson === "string") {
    return JSON.parse(payloadJson);
  }

  throw new Error("payload_json имеет неподдерживаемый тип");
}

function decodeStickerToSvg(payload) {
  if (!payload) return null;

  const raw = payload.wb_sticker || payload.base64 || null;
  if (!raw) return null;

  const clean = String(raw).trim();

  if (clean.startsWith("<svg")) {
    return clean;
  }

  const commaIndex = clean.indexOf(",");
  const pureBase64 = clean.startsWith("data:")
    ? clean.slice(commaIndex + 1)
    : clean;

  try {
    return Buffer.from(pureBase64, "base64").toString("utf8");
  } catch (e) {
    console.log("SVG DECODE ERROR:", e.message);
    return null;
  }
}

function makePrintableHtml(svgText) {
  return `<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8" />
  <title>WB Sticker</title>
  <style>
    @page {
      size: 40mm 58mm;
      margin: 0;
    }

    html, body {
      margin: 0;
      padding: 0;
      width: 40mm;
      height: 58mm;
      overflow: hidden;
      background: white;
    }

    body {
      position: relative;
    }

    .sheet {
      position: absolute;
      top: 0;
      left: 40mm;
      width: 58mm;
      height: 40mm;
      transform: rotate(90deg);
      transform-origin: top left;
      display: flex;
      align-items: center;
      justify-content: center;
      overflow: hidden;
      background: white;
    }

    .sheet svg {
      width: 58mm;
      height: 40mm;
      display: block;
    }
  </style>
</head>
<body>
  <div class="sheet">
    ${svgText}
  </div>
</body>
</html>`;
}

async function renderHtmlToPdf(htmlFilePath, pdfFilePath) {
  if (!fs.existsSync(CHROME_PATH)) {
    throw new Error(`Chrome не найден: ${CHROME_PATH}`);
  }

  const userDataDir = path.join(__dirname, "tmp", "chrome-headless-profile");

  if (!fs.existsSync(userDataDir)) {
    fs.mkdirSync(userDataDir, { recursive: true });
  }

  const fileUrl = "file:///" + htmlFilePath.replace(/\\/g, "/");

  const command = [
    `"${CHROME_PATH}"`,
    "--headless=new",
    "--disable-gpu",
    "--no-first-run",
    "--no-default-browser-check",
    `--user-data-dir="${userDataDir}"`,
    `--print-to-pdf="${pdfFilePath}"`,
    `"${fileUrl}"`
  ].join(" ");

  await execAsync(command);

  if (!fs.existsSync(pdfFilePath)) {
    throw new Error("Chrome не создал PDF");
  }
}

async function printPdfSilently(pdfFilePath, printerName) {
  if (!fs.existsSync(pdfFilePath)) {
    throw new Error(`PDF файл не найден: ${pdfFilePath}`);
  }

  await print(pdfFilePath, {
    printer: printerName,
    silent: true
  });
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
      if (Number(job.printer_id) !== Number(PRINTER_ID)) continue;

      console.log("PRINT JOB FOUND:", job.id);

      try {
        await markJob(job.id, "processing");

        let payload = {};
        try {
          payload = normalizePayload(job.payload_json);
        } catch (e) {
          console.log("RAW payload_json:", job.payload_json);
          console.log("payload_json type:", typeof job.payload_json);
          throw new Error("Некорректный payload_json");
        }

        const svgText = decodeStickerToSvg(payload);
        if (!svgText) {
          throw new Error("В print_job нет wb_sticker/base64");
        }

        const tempDir = path.join(__dirname, "tmp");
        if (!fs.existsSync(tempDir)) {
          fs.mkdirSync(tempDir, { recursive: true });
        }

        const htmlFilePath = path.join(tempDir, `wb-sticker-${job.id}.html`);
        const pdfFilePath = path.join(tempDir, `wb-sticker-${job.id}.pdf`);

        const htmlText = makePrintableHtml(svgText);
        fs.writeFileSync(htmlFilePath, htmlText, "utf8");

        console.log("HTML READY:", htmlFilePath);

        await renderHtmlToPdf(htmlFilePath, pdfFilePath);
        console.log("PDF READY:", pdfFilePath);

        await printPdfSilently(pdfFilePath, PRINTER_NAME);
        console.log("PRINT SENT:", job.id);

        await markJob(job.id, "printed");
        console.log("PRINTED:", job.id);
      } catch (jobErr) {
        const fullError = getReadableError(jobErr);
        console.log("PRINT ERROR:", fullError);
        await markJob(job.id, "error", safeText(fullError).slice(0, 4000));
      }
    }
  } catch (err) {
    console.log("AGENT ERROR:", getReadableError(err));
  } finally {
    isProcessing = false;
  }
}

checkJobs();
setInterval(checkJobs, POLL_INTERVAL);