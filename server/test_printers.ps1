# test_printers.ps1
# ----------------
# Скрипт для тестирования CRUD принтеров в BFC24 WMS

# ==== Настройка токена и адреса сервера ====
$baseUrl = "http://localhost:3000/printers"
$token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MSwidXNlcm5hbWUiOiJhZG1pbiIsInJvbGUiOiJvd25lciIsImlhdCI6MTc3MzA5NTEyOSwiZXhwIjoxNzczMTM4MzI5fQ.THFLNSK57IkS6mezVPZx8w_EJsTPRqmzEOw-LxVRGPA"

$headers = @{
    "Authorization" = "Bearer $token"
}

# ==== 1️⃣ GET всех принтеров ====
Write-Host "`n=== GET ALL PRINTERS ===`n"
Invoke-RestMethod -Uri $baseUrl -Method GET -Headers $headers -ContentType "application/json" | ConvertTo-Json

# ==== 2️⃣ POST новый принтер ====
$newPrinterJson = '{ "printer_code": "PRN01", "printer_name": "XPrinter365B", "printer_type": "label", "connection_type": "agent", "warehouse_code": "MAIN", "zone_code": "PACK" }'

Write-Host "`n=== CREATE NEW PRINTER ===`n"
$newPrinter = Invoke-RestMethod -Uri $baseUrl -Method POST -Headers $headers -Body $newPrinterJson -ContentType "application/json"
$newPrinter | ConvertTo-Json

# Сохраняем ID нового принтера
$printerId = $newPrinter.data.id

# ==== 3️⃣ GET принтера по ID ====
Write-Host "`n=== GET PRINTER BY ID ===`n"
Invoke-RestMethod -Uri "$baseUrl/$printerId" -Method GET -Headers $headers -ContentType "application/json" | ConvertTo-Json

# ==== 4️⃣ PATCH (обновление) принтера ====
$updateJson = '{ "printer_name": "XPrinter365B Updated" }'
Write-Host "`n=== UPDATE PRINTER ===`n"
Invoke-RestMethod -Uri "$baseUrl/$printerId" -Method PATCH -Headers $headers -Body $updateJson -ContentType "application/json" | ConvertTo-Json

# ==== 5️⃣ DELETE принтера ====
Write-Host "`n=== DELETE PRINTER ===`n"
Invoke-RestMethod -Uri "$baseUrl/$printerId" -Method DELETE -Headers $headers -ContentType "application/json" | ConvertTo-Json

# ==== 6️⃣ GET всех принтеров после удаления ====
Write-Host "`n=== GET ALL PRINTERS AFTER DELETE ===`n"
Invoke-RestMethod -Uri $baseUrl -Method GET -Headers $headers -ContentType "application/json" | ConvertTo-Json