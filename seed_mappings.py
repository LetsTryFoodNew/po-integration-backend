"""
Seed product mappings for all partners.
This table maps each partner's SKU/product name → your internal SAP product.

Example:
  Blinkit "BLK-NK-001"   "Let's Try Namkin"   → SAP MAT-10045 (same product)
  Zepto   "ZPT-NK-9821"  "Let's Try Namkeen"  → SAP MAT-10045 (same product)
  Swiggy  "SWG-SNCK-442" "LT Namkeen 200g"    → SAP MAT-10045 (same product)

Run: python seed_mappings.py
"""
import sys
sys.path.insert(0, ".")
from app.database import SessionLocal, engine, Base
from app import models

# Create tables if not exist
Base.metadata.create_all(bind=engine)

db = SessionLocal()

# Get all internal products indexed by SKU
products = {p.sku: p for p in db.query(models.Product).all()}

print("📦 Available internal products:")
for sku, p in products.items():
    print(f"   {sku} → '{p.name}' (SAP: {p.sap_material_code})")
print()

# ── Mapping Data ──────────────────────────────────────────────────────────────
# Format:
#   partner_code    = BLINKIT | ZEPTO | SWIGGY | BIGBASKET
#   partner_sku     = The SKU/item code partner uses in their PO
#   partner_name    = The product name as shown in partner's system
#   internal_sku    = YOUR internal SKU (from products table)

MAPPINGS = [
    # ── Blinkit ──────────────────────────────────────────────────────────
    {"partner_code": "BLK", "partner_sku": "AMUL-BTR-500",    "partner_name": "Amul Butter 500g",           "internal_sku": "AMUL-BTR-500"},
    {"partner_code": "BLK", "partner_sku": "AMUL-MLK-1L",     "partner_name": "Amul Full Cream Milk 1Ltr",  "internal_sku": "AMUL-MLK-1L"},
    {"partner_code": "BLK", "partner_sku": "BLK-SRF-001",     "partner_name": "Surf Excel 1KG Detergent",   "internal_sku": "HUL-SRF-1KG"},
    {"partner_code": "BLK", "partner_sku": "BLK-AATA-5K",     "partner_name": "Aashirvaad Wheat Atta 5KG",  "internal_sku": "ITC-AATA-5KG"},
    {"partner_code": "BLK", "partner_sku": "BLK-MAGI-12",     "partner_name": "Maggi 2-Min Noodles 12pk",   "internal_sku": "NESTLE-MAGI-12"},
    {"partner_code": "BLK", "partner_sku": "BLK-LUX-3",       "partner_name": "Lux Soap Bar 3-Pack",        "internal_sku": "HUL-LUX-3PK"},
    {"partner_code": "BLK", "partner_sku": "BLK-ARIEL-1K",    "partner_name": "Ariel Matic Detergent 1KG",  "internal_sku": "P&G-ARIEL-1KG"},
    {"partner_code": "BLK", "partner_sku": "BLK-COKE-750",    "partner_name": "Coca Cola 750ml",            "internal_sku": "COKE-750ML"},

    # ── Zepto ─────────────────────────────────────────────────────────────
    {"partner_code": "ZPT", "partner_sku": "ZPT-BTR-AMU500",  "partner_name": "Amul Butter 500 Grams",      "internal_sku": "AMUL-BTR-500"},
    {"partner_code": "ZPT", "partner_sku": "ZPT-MILK-AM1L",   "partner_name": "Amul Milk Full Fat 1L",      "internal_sku": "AMUL-MLK-1L"},
    {"partner_code": "ZPT", "partner_sku": "ZPT-9821-SURF",   "partner_name": "Surf Excel Detergent 1 KG",  "internal_sku": "HUL-SRF-1KG"},
    {"partner_code": "ZPT", "partner_sku": "ZPT-AATA-ITC5K",  "partner_name": "Aashirvaad Atta 5KG Pack",   "internal_sku": "ITC-AATA-5KG"},
    {"partner_code": "ZPT", "partner_sku": "ZPT-MAGI-NES12",  "partner_name": "Maggi Noodles 12 Pack",      "internal_sku": "NESTLE-MAGI-12"},
    {"partner_code": "ZPT", "partner_sku": "ZPT-LUX-HUL3",   "partner_name": "Lux Soap 3 Pcs Pack",        "internal_sku": "HUL-LUX-3PK"},
    {"partner_code": "ZPT", "partner_sku": "ZPT-ARIEL-PG1K",  "partner_name": "Ariel 1KG Washing Powder",   "internal_sku": "P&G-ARIEL-1KG"},
    {"partner_code": "ZPT", "partner_sku": "ZPT-COKE-750ML",  "partner_name": "Coke 750ml Glass Bottle",    "internal_sku": "COKE-750ML"},

    # ── Swiggy Instamart ──────────────────────────────────────────────────
    {"partner_code": "SWG", "partner_sku": "SWG-AMB500",      "partner_name": "Amul Butter 500g Pack",      "internal_sku": "AMUL-BTR-500"},
    {"partner_code": "SWG", "partner_sku": "SWG-AML1L",       "partner_name": "Amul Cow Milk 1 Litre",      "internal_sku": "AMUL-MLK-1L"},
    {"partner_code": "SWG", "partner_sku": "SWG-SURF1K",      "partner_name": "Surf Excel 1KG",             "internal_sku": "HUL-SRF-1KG"},
    {"partner_code": "SWG", "partner_sku": "SWG-AATA5K",      "partner_name": "Aashirvaad Chakki Atta 5KG", "internal_sku": "ITC-AATA-5KG"},
    {"partner_code": "SWG", "partner_sku": "SWG-MAGI12",      "partner_name": "Maggi Masala Noodles 12pk",  "internal_sku": "NESTLE-MAGI-12"},
    {"partner_code": "SWG", "partner_sku": "SWG-LUX3",        "partner_name": "Lux Moisturising Soap 3pk",  "internal_sku": "HUL-LUX-3PK"},
    {"partner_code": "SWG", "partner_sku": "SWG-ARI1K",       "partner_name": "Ariel Front Load 1KG",       "internal_sku": "P&G-ARIEL-1KG"},
    {"partner_code": "SWG", "partner_sku": "SWG-CK750",       "partner_name": "Coca-Cola 750ml Bottle",     "internal_sku": "COKE-750ML"},

    # ── BigBasket ─────────────────────────────────────────────────────────
    {"partner_code": "BBK", "partner_sku": "BB-AMUL-BTR5",    "partner_name": "Amul Pasteurised Butter 500g","internal_sku": "AMUL-BTR-500"},
    {"partner_code": "BBK", "partner_sku": "BB-AMUL-MLK1",    "partner_name": "Amul Taza Milk 1L",          "internal_sku": "AMUL-MLK-1L"},
    {"partner_code": "BBK", "partner_sku": "BB-SURF-1KG",     "partner_name": "Surf Excel Easy Wash 1KG",   "internal_sku": "HUL-SRF-1KG"},
    {"partner_code": "BBK", "partner_sku": "BB-AATA-5KG",     "partner_name": "Aashirvaad Select Atta 5KG", "internal_sku": "ITC-AATA-5KG"},
    {"partner_code": "BBK", "partner_sku": "BB-MAGI-12PK",    "partner_name": "Maggi Noodles Value Pack 12", "internal_sku": "NESTLE-MAGI-12"},
    {"partner_code": "BBK", "partner_sku": "BB-LUX-SOAP3",    "partner_name": "Lux International Soap 3pk", "internal_sku": "HUL-LUX-3PK"},
    {"partner_code": "BBK", "partner_sku": "BB-ARIEL-1KG",    "partner_name": "Ariel Detergent Powder 1KG", "internal_sku": "P&G-ARIEL-1KG"},
    {"partner_code": "BBK", "partner_sku": "BB-COKE-750",     "partner_name": "Coca-Cola Cold Drink 750ml", "internal_sku": "COKE-750ML"},
]

inserted = 0
skipped  = 0

for m in MAPPINGS:
    product = products.get(m["internal_sku"])
    if not product:
        print(f"   ⚠️  Internal SKU not found: {m['internal_sku']} — skipping")
        skipped += 1
        continue

    # Check if already exists
    exists = db.query(models.ProductMapping).filter(
        models.ProductMapping.partner_code == m["partner_code"],
        models.ProductMapping.partner_sku  == m["partner_sku"]
    ).first()

    if exists:
        skipped += 1
        continue

    mapping = models.ProductMapping(
        partner_code         = m["partner_code"],
        partner_sku          = m["partner_sku"],
        partner_product_name = m["partner_name"],
        product_id           = product.id,
        sap_material_code    = product.sap_material_code,
        confidence_score     = 1.0,
        mapped_by            = "MANUAL"
    )
    db.add(mapping)
    inserted += 1
    print(f"   ✅ [{m['partner_code']}] '{m['partner_name']}' ({m['partner_sku']}) → '{product.name}' (SAP: {product.sap_material_code})")

db.commit()
db.close()

print(f"\n{'='*60}")
print(f"✅ Done! Inserted: {inserted} | Skipped (already exist): {skipped}")
print(f"{'='*60}")
print("\nYou can now test the mapping resolver:")
print("  GET /api/product-mappings")
print("  GET /api/product-mappings/resolve?partner_code=BLK&partner_sku=BLK-SRF-001")
