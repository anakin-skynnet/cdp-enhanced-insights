# Databricks notebook source
# MAGIC %md
# MAGIC # PagoNxt Getnet CDP — Synthetic Data Generator
# MAGIC
# MAGIC Generates realistic synthetic data for all five source systems and writes
# MAGIC them as files to Unity Catalog Volumes, exactly where the Bronze Auto Loader
# MAGIC ingestion layer expects them.
# MAGIC
# MAGIC | Source | Format | Volume Path |
# MAGIC |--------|--------|-------------|
# MAGIC | Salesforce Contacts | JSON | `/Volumes/{catalog}/{schema}/raw/salesforce/contacts/` |
# MAGIC | Salesforce Accounts | JSON | `/Volumes/{catalog}/{schema}/raw/salesforce/accounts/` |
# MAGIC | Zendesk Tickets | JSON | `/Volumes/{catalog}/{schema}/raw/zendesk/tickets/` |
# MAGIC | Getnet Transactions | Parquet | `/Volumes/{catalog}/{schema}/raw/transactions/` |
# MAGIC | Genesys Interactions | JSON | `/Volumes/{catalog}/{schema}/raw/genesys/` |
# MAGIC
# MAGIC **Run this once** to seed the lakehouse, then let the pipelines and jobs
# MAGIC propagate data through Bronze → Silver → Gold → ML → Agents.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "ahs_demos_catalog"
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "cdp_360"
num_merchants = int(dbutils.widgets.get("num_merchants")) if dbutils.widgets.get("num_merchants") else 150

VOLUME_BASE = f"/Volumes/{catalog}/{schema}/raw"

print(f"Catalog:        {catalog}")
print(f"Schema:         {schema}")
print(f"Volume base:    {VOLUME_BASE}")
print(f"Num merchants:  {num_merchants}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Volume directories

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw")

for sub in ["salesforce/contacts", "salesforce/accounts", "zendesk/tickets", "transactions", "genesys"]:
    dbutils.fs.mkdirs(f"{VOLUME_BASE}/{sub}/")

print("Volume directories ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Shared merchant seed
# MAGIC
# MAGIC Build a consistent merchant universe so IDs link across all sources.

# COMMAND ----------

import random
import uuid
import json
from datetime import datetime, timedelta, date
from decimal import Decimal

random.seed(42)

COUNTRIES = [
    {"code": "BR", "cities": ["São Paulo", "Rio de Janeiro", "Curitiba", "Brasília", "Belo Horizonte", "Porto Alegre", "Salvador", "Recife", "Fortaleza", "Manaus"]},
    {"code": "MX", "cities": ["Ciudad de México", "Guadalajara", "Monterrey", "Puebla", "Cancún"]},
    {"code": "AR", "cities": ["Buenos Aires", "Córdoba", "Rosario", "Mendoza"]},
    {"code": "CL", "cities": ["Santiago", "Valparaíso", "Concepción"]},
]

BUSINESS_TYPES = [
    "Restaurante", "Café", "Farmácia", "Supermercado", "Loja de Roupas",
    "Padaria", "Posto de Gasolina", "Hotel", "Clínica", "Academia",
    "Pet Shop", "Livraria", "Eletrônicos", "Autopeças", "Construção",
    "Beleza & Estética", "Ótica", "Joalheria", "Brinquedos", "Esportes",
]

FIRST_NAMES = [
    "Carlos", "Maria", "João", "Ana", "Pedro", "Lucia", "Rafael", "Juliana",
    "Fernando", "Camila", "Diego", "Valentina", "Gabriel", "Isabella", "Lucas",
    "Sofia", "Mateo", "Martina", "Santiago", "Catalina", "Andrés", "Paula",
]

LAST_NAMES = [
    "Silva", "Santos", "Oliveira", "Souza", "Pereira", "Costa", "Ferreira",
    "Rodrigues", "Almeida", "Nascimento", "García", "Martínez", "López",
    "González", "Hernández", "Müller", "Schmidt", "Schneider", "Fischer",
]

STREETS = [
    "Av. Paulista", "Rua Augusta", "Rua Oscar Freire", "Av. Faria Lima",
    "Rua Haddock Lobo", "Av. Insurgentes", "Calle Reforma", "Av. 9 de Julio",
    "Rua das Flores", "Av. Atlântica", "Rua do Comércio", "Av. Brasil",
]

PHONE_PREFIXES = {"BR": "55", "MX": "52", "AR": "54", "CL": "56"}

merchants = []
for i in range(num_merchants):
    country = random.choice(COUNTRIES)
    city = random.choice(country["cities"])
    biz = random.choice(BUSINESS_TYPES)
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    prefix = PHONE_PREFIXES.get(country["code"], "55")
    merchant = {
        "sf_account_id": f"001{uuid.uuid4().hex[:12].upper()}",
        "sf_contact_id": f"003{uuid.uuid4().hex[:12].upper()}",
        "merchant_id": f"GTN-{country['code']}-{100000 + i}",
        "name": f"{biz} {last}",
        "contact_first": first,
        "contact_last": last,
        "email": f"{first.lower()}.{last.lower()}@{biz.lower().replace(' ', '').replace('&', '')}.com",
        "phone": f"+{prefix}{random.randint(11,99)}{random.randint(900000000, 999999999)}",
        "city": city,
        "country": country["code"],
        "street": f"{random.choice(STREETS)}, {random.randint(1, 9999)}",
        "created_date": (datetime.now() - timedelta(days=random.randint(90, 900))).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "monthly_volume_avg": random.randint(5000, 500000),
    }
    merchants.append(merchant)

print(f"Generated {len(merchants)} merchant seeds.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Salesforce Accounts

# COMMAND ----------

sf_accounts = []
for m in merchants:
    sf_accounts.append({
        "Id": m["sf_account_id"],
        "Name": m["name"],
        "Phone": m["phone"],
        "BillingStreet": m["street"],
        "BillingCity": m["city"],
        "BillingCountry": m["country"],
        "Industry": random.choice(["Retail", "Food & Beverage", "Healthcare", "Services", "Hospitality", "Automotive", "Technology"]),
        "Type": random.choice(["Customer", "Prospect", "Partner"]),
        "Rating": random.choice(["Hot", "Warm", "Cold"]),
        "AnnualRevenue": m["monthly_volume_avg"] * 12,
        "NumberOfEmployees": random.randint(1, 200),
        "OwnerId": f"005{uuid.uuid4().hex[:12].upper()}",
        "CreatedDate": m["created_date"],
    })

path = f"{VOLUME_BASE}/salesforce/accounts/"
spark.createDataFrame(sf_accounts).coalesce(1).write.mode("overwrite").json(path)
print(f"Wrote {len(sf_accounts)} Salesforce accounts to {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Salesforce Contacts

# COMMAND ----------

sf_contacts = []
for m in merchants:
    sf_contacts.append({
        "Id": m["sf_contact_id"],
        "Email": m["email"],
        "Phone": m["phone"],
        "FirstName": m["contact_first"],
        "LastName": m["contact_last"],
        "AccountId": m["sf_account_id"],
        "Title": random.choice(["Owner", "Manager", "Director", "CFO", "COO", "Operations Lead"]),
        "Department": random.choice(["Finance", "Operations", "Management", "Sales"]),
        "MailingCity": m["city"],
        "MailingCountry": m["country"],
        "CreatedDate": m["created_date"],
    })
    if random.random() < 0.3:
        extra_first = random.choice(FIRST_NAMES)
        extra_last = random.choice(LAST_NAMES)
        sf_contacts.append({
            "Id": f"003{uuid.uuid4().hex[:12].upper()}",
            "Email": f"{extra_first.lower()}.{extra_last.lower()}@{m['name'].lower().replace(' ', '')}.com",
            "Phone": f"+{PHONE_PREFIXES.get(m['country'], '55')}{random.randint(11,99)}{random.randint(900000000, 999999999)}",
            "FirstName": extra_first,
            "LastName": extra_last,
            "AccountId": m["sf_account_id"],
            "Title": random.choice(["Accountant", "Assistant", "Analyst"]),
            "Department": random.choice(["Finance", "Operations"]),
            "MailingCity": m["city"],
            "MailingCountry": m["country"],
            "CreatedDate": m["created_date"],
        })

path = f"{VOLUME_BASE}/salesforce/contacts/"
spark.createDataFrame(sf_contacts).coalesce(1).write.mode("overwrite").json(path)
print(f"Wrote {len(sf_contacts)} Salesforce contacts to {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Getnet Transactions (Parquet)
# MAGIC
# MAGIC Payment transactions over the last 12 months with realistic volume patterns.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, IntegerType

txns = []
today = date.today()
STATUSES = ["approved", "approved", "approved", "approved", "declined", "refunded", "chargeback"]
PAYMENT_METHODS = ["credit_card", "debit_card", "pix", "boleto"]

for m in merchants:
    vol = m["monthly_volume_avg"]
    monthly_txn_count = max(10, vol // random.randint(80, 300))
    for month_offset in range(12):
        txn_date_base = today - timedelta(days=30 * month_offset)
        seasonal_factor = 1.0 + 0.3 * (1 if txn_date_base.month in [11, 12, 1] else 0)
        count = int(monthly_txn_count * seasonal_factor * random.uniform(0.7, 1.3))
        for _ in range(count):
            txn_day = txn_date_base - timedelta(days=random.randint(0, 29))
            txns.append((
                f"TXN-{uuid.uuid4().hex[:16].upper()}",
                m["sf_account_id"],
                Decimal(str(round(random.uniform(5.0, vol / monthly_txn_count * 3), 2))),
                txn_day,
                random.choice(STATUSES),
                random.choice(PAYMENT_METHODS),
                "BRL" if m["country"] == "BR" else "MXN" if m["country"] == "MX" else "ARS" if m["country"] == "AR" else "CLP",
                random.choice(["Visa", "Mastercard", "Elo", "Amex", "Hipercard"]),
                random.choice([1, 1, 1, 2, 3, 6, 12]),
                f"POS-{m['merchant_id'][-6:]}-{random.randint(1,5)}",
            ))

txn_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("amount", DecimalType(18, 2), False),
    StructField("transaction_date", DateType(), False),
    StructField("status", StringType(), False),
    StructField("payment_method", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("card_brand", StringType(), True),
    StructField("installments", IntegerType(), True),
    StructField("terminal_id", StringType(), True),
])

path = f"{VOLUME_BASE}/transactions/"
spark.createDataFrame(txns, schema=txn_schema).coalesce(4).write.mode("overwrite").parquet(path)
print(f"Wrote {len(txns)} transactions to {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Zendesk Support Tickets

# COMMAND ----------

TICKET_SUBJECTS = [
    "POS terminal not connecting", "Settlement delay", "Chargeback dispute",
    "Rate review request", "New terminal request", "Billing discrepancy",
    "API integration help", "Transaction declined without reason",
    "Monthly statement question", "Refund not processed", "Account closure request",
    "Password reset", "Fee structure inquiry", "Device malfunction",
    "App not loading transactions", "Missing deposit", "Tax document request",
]
PRIORITIES = ["low", "normal", "normal", "normal", "high", "urgent"]
TICKET_TYPES = ["question", "incident", "problem", "task"]
SATISFACTION = ["good", "good", "good", "bad", "unoffered", "unoffered"]
TAGS_POOL = ["billing", "terminal", "settlement", "api", "dispute", "onboarding", "hardware", "fees"]

tickets = []
for m in merchants:
    ticket_count = random.randint(0, 15)
    for _ in range(ticket_count):
        created = datetime.now() - timedelta(days=random.randint(1, 365), hours=random.randint(0, 23))
        priority = random.choice(PRIORITIES)
        status = random.choices(
            ["new", "open", "pending", "solved", "closed"],
            weights=[5, 15, 10, 40, 30],
        )[0]
        first_resp = created + timedelta(minutes=random.randint(5, 480)) if status != "new" else None
        solved = created + timedelta(hours=random.randint(1, 72)) if status in ("solved", "closed") else None
        closed = solved + timedelta(hours=random.randint(0, 24)) if status == "closed" and solved else None

        tickets.append({
            "id": f"ZTK-{uuid.uuid4().hex[:10].upper()}",
            "subject": random.choice(TICKET_SUBJECTS),
            "status": status,
            "priority": priority,
            "type": random.choice(TICKET_TYPES),
            "requester_id": m["sf_contact_id"],
            "assignee_id": f"AGT-{random.randint(1000,1050)}",
            "group_id": f"GRP-{random.choice(['billing','support','tech','onboarding'])}",
            "tags": random.sample(TAGS_POOL, k=random.randint(1, 3)),
            "satisfaction_rating": random.choice(SATISFACTION) if status in ("solved", "closed") else "unoffered",
            "created_at": created.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "updated_at": (solved or first_resp or created).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "first_response_at": first_resp.strftime("%Y-%m-%dT%H:%M:%SZ") if first_resp else None,
            "solved_at": solved.strftime("%Y-%m-%dT%H:%M:%SZ") if solved else None,
            "closed_at": closed.strftime("%Y-%m-%dT%H:%M:%SZ") if closed else None,
        })

path = f"{VOLUME_BASE}/zendesk/tickets/"
spark.createDataFrame(tickets).coalesce(1).write.mode("overwrite").json(path)
print(f"Wrote {len(tickets)} Zendesk tickets to {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Genesys Contact Center Interactions

# COMMAND ----------

INTERACTION_TYPES = ["voice", "voice", "voice", "chat", "email", "callback"]
MEDIA_TYPES = ["voice", "chat", "email"]
DIRECTIONS = ["inbound", "inbound", "inbound", "outbound"]
QUEUES = ["billing_support", "technical_support", "general_inquiry", "escalation", "onboarding", "retention"]
DISPOSITIONS = ["completed", "completed", "completed", "transferred", "abandoned", "voicemail"]
DISCONNECT_TYPES = ["agent", "customer", "system", "transfer"]
WRAP_UP_CODES = ["resolved", "follow_up", "escalated", "info_provided", "callback_scheduled"]

TRANSCRIPT_TEMPLATES = [
    "Customer called about {topic}. Agent resolved the issue by {resolution}.",
    "Merchant inquired about {topic}. Provided detailed explanation on {resolution}.",
    "Inbound call regarding {topic}. Customer was {sentiment} with the response. {resolution}.",
    "Follow-up call for {topic}. Previous issue was {status}. Agent {resolution}.",
    "Merchant reported {topic}. Verified account details and {resolution}.",
]
TOPICS = ["settlement delay", "terminal malfunction", "rate negotiation", "billing error",
          "chargeback process", "new feature request", "account verification", "refund status"]
RESOLUTIONS = ["providing step-by-step guidance", "escalating to tier 2", "issuing a credit",
               "scheduling a technician visit", "updating account settings", "sending documentation"]
SENTIMENTS_TEMPLATE = ["satisfied", "neutral", "frustrated", "very satisfied", "upset"]
CALL_STATUSES = ["resolved on first contact", "pending follow-up", "escalated"]

interactions = []
AGENT_IDS = [f"AGT-CC-{i}" for i in range(1001, 1025)]

for m in merchants:
    call_count = random.randint(0, 12)
    for _ in range(call_count):
        conv_date = datetime.now() - timedelta(days=random.randint(1, 365), hours=random.randint(0, 23))
        duration = random.randint(30, 1800)
        talk_time = int(duration * random.uniform(0.5, 0.85))
        hold_time = int(duration * random.uniform(0, 0.2))
        acw = random.randint(10, 180)

        template = random.choice(TRANSCRIPT_TEMPLATES)
        transcript = template.format(
            topic=random.choice(TOPICS),
            resolution=random.choice(RESOLUTIONS),
            sentiment=random.choice(SENTIMENTS_TEMPLATE),
            status=random.choice(CALL_STATUSES),
        )

        interactions.append({
            "conversation_id": f"CONV-{uuid.uuid4().hex[:14].upper()}",
            "user_id": random.choice(AGENT_IDS),
            "customer_id": m["sf_contact_id"],
            "external_contact_id": m["sf_account_id"],
            "conversation_date": conv_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "interaction_type": random.choice(INTERACTION_TYPES),
            "media_type": random.choice(MEDIA_TYPES),
            "direction": random.choice(DIRECTIONS),
            "queue_name": random.choice(QUEUES),
            "duration_seconds": duration,
            "talk_time_seconds": talk_time,
            "hold_time_seconds": hold_time,
            "queue_wait_seconds": random.randint(0, 300),
            "after_call_work_seconds": acw,
            "disposition": random.choice(DISPOSITIONS),
            "disconnect_type": random.choice(DISCONNECT_TYPES),
            "transcript_text": transcript,
            "wrap_up_code": random.choice(WRAP_UP_CODES),
        })

path = f"{VOLUME_BASE}/genesys/"
spark.createDataFrame(interactions).coalesce(1).write.mode("overwrite").json(path)
print(f"Wrote {len(interactions)} Genesys interactions to {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Summary

# COMMAND ----------

print("=" * 60)
print("  CDP Synthetic Data Generation Complete")
print("=" * 60)
print(f"  Merchants:         {len(merchants)}")
print(f"  SF Accounts:       {len(sf_accounts)}")
print(f"  SF Contacts:       {len(sf_contacts)}")
print(f"  Transactions:      {len(txns)}")
print(f"  Zendesk Tickets:   {len(tickets)}")
print(f"  Genesys Calls:     {len(interactions)}")
print(f"  Volume Base:       {VOLUME_BASE}")
print("=" * 60)
print()
print("Next steps:")
print("  1. Run pipeline 01 — CDP Bronze Ingestion")
print("  2. Run pipeline 02 — CDP Silver Transformations")
print("  3. Run job 03     — CDP Identity Resolution")
print("  4. Run pipeline 04 — CDP Gold Analytics")
print("  5. Run jobs 05-13 — ML models & analytics")
print("  6. Run job 14     — Deploy AI Agents")
