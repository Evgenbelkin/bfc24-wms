BFC24 WMS / ERP

Складская система нового поколения для FBS / FBO / WB-клиентов



BFC24 WMS — это профессиональная складская система (WMS), разрабатываемая для личного фулфилмента, интеграции с маркетплейсами и автоматизации всех операций склада:

приёмка → размещение → сборка → упаковка → отгрузка → контроль → аналитика → ERP-функции.



Система уже используется в реальных условиях: 80–100 заказов/день, мультиклиентский режим, поддержка WB API.



🚀 Возможности системы

🔵 1. Модули склада



Приёмка по штрихкоду



Размещение по МХ (ячейкам)



Складский остаток с учётом клиентов



Сборка заказов (picking)



Пропуск → автоматическая инвентаризация



Упаковка (packing)



Отгрузка (shipping, GI)



История операций (движения)



Инвентаризации с блокировками МХ



Синхронизация остатков с WB



🟣 2. Интеграции с маркетплейсами



Wildberries Content API



Wildberries Orders API



Загрузка карточек с фото



Загрузка остатков FBS/FBO



Синхронизация заказов



Формирование FBS-волн



Подтверждение отгрузок



🟩 3. Веб-интерфейс под ТСД / телефон



PWA



Сканирование штрихкодов



Упаковка с печатью стикеров



Автоматическая печать QR-кодов (WB)



Крупные кнопки, интерфейс под пальцы



Роли сотрудников: picker, packer, admin, owner



🟧 4. Аналитика / ERP-функции



Движения (incoming / move / writeoff / inventory / picking)



Отчёты по сборке



Отчёты по остаткам



Заработок по дням (ставки за сборку, упаковку, литраж)



Авторасчёт стоимости обработки



Панель управления (dashboard)



Оборачиваемость 30 дней



История действий каждого сотрудника



🛠 Технологический стек



Node.js 18+



Express.js



PostgreSQL



pg-pool / pg-native



HTML + JS (клиентская часть)



REST API



JSON Web Token (JWT)



Wildberries Content API



Wildberries Supplier API



📂 Структура проекта

projectsbfc24-wms/

│   README.md

│   .gitignore

│

└───bfc24-wms/

&nbsp;   └───server/

&nbsp;       │   index.js                # основной сервер

&nbsp;       │   db.js                   # подключение к PostgreSQL

&nbsp;       │   authMiddleware.js

&nbsp;       │   reset\_admin.js

&nbsp;       │   create\_user.js

&nbsp;       │   wbService.js            # WB API

&nbsp;       │   serviceswbApi.js

&nbsp;       │   serviceswbNormalize.js

&nbsp;       │   serviceswbSync.js

&nbsp;       │   mpClients.js

&nbsp;       │   routesauth.js

&nbsp;       │   ...

&nbsp;       │

&nbsp;       ├── public/                 # фронт для приёмки/сборки/упаковки

&nbsp;       ├── services/               # сервисы и бизнес-логика

&nbsp;       ├── node\_modules/           # зависимости

&nbsp;       ├── .env                    # токены, доступы, секреты

&nbsp;       └── .gitignore              # игнор для server

⚙️ Запуск проекта

🔹 1. Клонирование репозитория

git clone https://github.com/Evgenbelkin/bfc24-wms.git

cd bfc24-wms/server

🔹 2. Установка зависимостей

npm install

🔹 3. Настройки .env



Создать файл:



PORT=3000

POSTGRES\_HOST=localhost

POSTGRES\_DB=bfc24

POSTGRES\_USER=postgres

POSTGRES\_PASSWORD=пароль



WB\_API\_KEY=ваш\_ключ

WB\_SUPPLIER\_KEY=ваш\_ключ\_поставщика

JWT\_SECRET=секрет

🔹 4. Запуск сервера

npm start



или режим разработчика:



npm run dev

🔌 API (кратко)

Авторизация

POST /auth/login

POST /auth/create

Склад

POST /receiving/accept

POST /picking/next

POST /picking/confirm

POST /picking/skip

POST /packing/confirm

POST /shipping/next

POST /shipping/confirm

Wildberries

GET /wb/items

GET /wb/orders

GET /wb/stocks

POST /wb/sync



(потом при желании сделаем полную документацию OpenAPI)



🔒 Безопасность



.env всегда в игноре



Все ключи через переменные окружения



Роль-база: owner / admin / picker / packer



Блокировка МХ при skip



Логирование всех движений товара



🧱 Архитектура в двух словах



Микромодульный backend



Каждая операция создаёт запись в movements



SKU Registry и таблица items — источник истины



Основная логика: stock/move, stock/adjust, picking\_tasks, inventory\_tasks



Все операции привязаны к barcode (основной ключ)



🧩 Планы развития



Отдельная админ-панель



Клиентский кабинет (вход для продавцов)



Баланс, инвойсы, тарифы



Поддержка Ozon / Ya.Market



Мобильное приложение



SaaS-режим (мульти-тенантность)

