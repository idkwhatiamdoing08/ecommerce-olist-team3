<!DOCTYPE html>
<html lang="ru">
<body>

<!-- Заголовок и бейджи -->
<div align="center">
  <h1>🛒 Витрина продаж e-commerce и когортная аналитика</h1>
  <p><b>Полноцикловый ETL-пайплайн обработки данных бразильского маркетплейса Olist</b></p>
  <img src="https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/SQLite-003B57?style=for-the-badge&logo=sqlite&logoColor=white" alt="SQLite">
  <img src="https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white" alt="Airflow">
  <img src="https://img.shields.io/badge/Dash-1E90FF?style=for-the-badge&logo=plotly&logoColor=white" alt="Dash">
</div>

<hr>

<!-- Оглавление -->
<details open>
  <summary><b>📌 Оглавление</b></summary>
  <ol>
    <li><a href="#о-проекте">О проекте</a></li>
    <li><a href="#наша-команда">Наша Команда</a></li>
    <li><a href="#архитектура-пайплайна">Архитектура пайплайна</a></li>
    <li><a href="#функциональность">Функциональность</a></li>
    <li><a href="#скриншоты">Скриншоты</a></li>
    <li><a href="#технологический-стек">Технологический стек</a></li>
    <li><a href="#структура-проекта">Структура проекта</a></li>
    <li><a href="#быстрый-старт">Быстрый старт</a></li>
    <li><a href="#полная-документация">Полная документация</a></li>
    <li><a href="#развертывание">Развертывание</a></li>
  </ol>
</details>

<hr>

<h2 id="о-проекте">📖 О проекте</h2>
<p>
  Цель проекта: построить воспроизводимый ETL-конвейер, превращающий CSV-файлы Olist в структурированную SQLite витрину, с анализом когорт, RFM и SLA доставки.
</p>
<p><b>Основные задачи:</b></p>
<ul>
  <li>ETL: загрузка и очистка CSV данных</li>
  <li>Data Modeling: схема "Звезда" с fact_orders и измерениями dim_customer, dim_product и др.</li>
  <li>Analytics: когортный анализ (Retention), RFM, SLA доставки</li>
  <li>Quality: контроль качества данных на каждом этапе</li>
</ul>

<h2 id="наша-команда">👥 Наша Команда</h2>
<table align="center">
  <tr>
    <td align="center"><b>Project-manager</b></td>
    <td align="center"><b>Разработчик</b></td>
    <td align="center"><b>Тестировщики / QA</b></td>
  </tr>
  <tr>
    <td align="center"><img src="https://github.com/identicons/de.png" width="80px;"><br>Планирование спринтов, управление рисками, общая отчётность</td>
    <td align="center"><img src="https://github.com/identicons/da.png" width="80px;"><br>Написание кода transform и load, настройка БД. Дизайн Airflow DAG, реализация клиентов API</td>
    <td align="center"><img src="https://github.com/identicons/qa.png" width="80px;"><br>Проектирование витрины и дашборда, разработка DQ-правил, финальная аналитика. Реализация визуализаций в BI-инструменте</td>
  </tr>
</table>

<h2 id="архитектура-пайплайна">🏗 Архитектура пайплайна</h2>
<p>Система построена по принципу <b>Medallion Architecture</b>:</p>
<ol>
  <li><b>Bronze (Raw):</b> загрузка CSV в папку <code>data/raw/</code>.</li>
  <li><b>Silver (Cleaned):</b> очистка, нормализация дат и валют, дедупликация.</li>
  <li><b>Gold (Business):</b> создание fact/dim таблиц и витрин для анализа.</li>
</ol>
<div align="center">
  <img src="https://via.placeholder.com/700x300.png?text=Pipeline+Flow" alt="Pipeline Flow">
</div>

🛠 Функциональность
<a name="функциональность"></a>
<details>
<summary><b>1. Схема данных (Star Schema)</b></summary>
Реализована нормализованная структура:
- **Факты:** fact_orders, fact_order_items.
- **Измерения:** dim_customer, dim_product, dim_geography, dim_calendar.
</details>

<h2 id="скриншоты">📊 Скриншоты</h2>
<div align="center">
  <p><i>Когортный анализ</i></p>
  <img src="./docs/dashboard_cohort.png" alt="Cohort Analysis">

  <p><i>Продажи / топ-категории</i></p>
  <img src="./docs/dashboard_rfm.png" alt="Sales Analysis">

  <p><i>SLA доставки</i></p>
  <img src="./docs/dashboard_sla.png" alt="SLA Analysis">
</div>

<h2 id="технологический-стек">💻 Технологический стек</h2>
<p>
  <b>Storage:</b> SQLite <br>
  <b>ETL/Processing:</b> Python 3.11 (Pandas, SQLAlchemy) <br>
  <b>Orchestration:</b> Apache Airflow <br>
  <b>Quality Control:</b> простые unit-тесты + проверки логики данных <br>
  <b>Visualization:</b> Dash / Plotly
</p>

<h2 id="структура-проекта">📂 Структура проекта</h2>
<pre>
CityPulse/
├── src/
│   ├── etl/
│   │   ├── etl_pipeline.ipynb
│   │   ├── data_quality_checks.py
│   │   ├── sql_schema.sql
│   │   └── init.py
│   │
│   ├── analysis/
│   │   ├── cohort_analysis.ipynb
│   │   ├── rfm_analysis.ipynb
│   │   └── sla_analysis.ipynb
│   │
│   ├── airflow_dag/
│   │   └── ecommerce_etl_dag.py
│   │
│   └── init.py
│
├── data/
│   ├── olist_orders.csv
│   ├── olist_customers.csv
│   ├── olist_products.csv
│   ├── olist_order_items.csv
│   ├── olist_geolocation.csv
│   └── ecommerce.db
│
├── docs/
│   ├── README.md
│   ├── ARCHITECTURE.md
│   ├── DATA_README.md
│   ├── TESTING.md
│   ├── CLEANING_RULES.md
│   └── REPORT.md
│
├── requirements.txt
├── .gitignore
└── README.md
</pre>

<h2 id="быстрый-старт">🚀 Быстрый старт</h2>
<ol>
  <li>Склонируйте репозиторий.</li>
  <li>Создайте виртуальное окружение и установите зависимости:
    <pre><code>python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt</code></pre>
  </li>
  <li>Поместите CSV-файлы Olist в <code>data/raw/</code>.</li>
  <li>Запустите ETL DAG через Airflow или отдельные скрипты Python.</li>
  <li>Для дашборда:
    <pre><code>python src/analysis/dashboard_app.py</code></pre>
  </li>
</ol>

<h2 id="полная-документация">📖 Полная документация</h2>
<details>
  <summary><b>Методология очистки и метрики</b></summary>
  <ul>
    <li>Дедупликация: сопоставление клиентов по email и zip_code</li>
    <li>Аномалии: цены > 0, фрахт/вес в разумных диапазонах</li>
    <li>Метрики:
      <ul>
        <li>GMV (Gross Merchandise Volume)</li>
        <li>AOV (Average Order Value)</li>
        <li>Late Delivery Rate</li>
      </ul>
    </li>
  </ul>
</details>

<h2 id="развертывание">🌐 Развертывание</h2>
<p>
  Можно запускать локально через Python + Airflow. Для полной имитации CI/CD можно использовать Docker Compose.
</p>

<hr>
<div align="center">
  <b>Проект выполнен в рамках учебного задания по Data Engineering / Analytics, 2026.</b>
</div>

</body>
</html>
