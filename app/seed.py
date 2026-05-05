from app.database import SessionLocal
from app import models

def seed():
    db = SessionLocal()
    try:
        # Companies
        companies = [
            models.Company(name="Zepto", code="ZPT", logo_color="#8B5CF6", contact_email="po@zepto.com"),
            models.Company(name="Swiggy Instamart", code="SWG", logo_color="#F97316", contact_email="po@swiggy.com"),
            models.Company(name="Blinkit", code="BLK", logo_color="#EAB308", contact_email="po@blinkit.com"),
            models.Company(name="BigBasket", code="BBK", logo_color="#22C55E", contact_email="po@bigbasket.com"),
        ]
        for c in companies:
            existing = db.query(models.Company).filter(models.Company.code == c.code).first()
            if not existing:
                db.add(c)
        db.commit()

        # Products
        products = [
            models.Product(sku="AMUL-BTR-500", name="Amul Butter 500g", category="Dairy", unit="pcs", price_per_unit=275.0, stock_quantity=500, reorder_level=100, sap_material_code="MAT-001"),
            models.Product(sku="AMUL-MLK-1L", name="Amul Full Cream Milk 1L", category="Dairy", unit="ltr", price_per_unit=68.0, stock_quantity=1200, reorder_level=200, sap_material_code="MAT-002"),
            models.Product(sku="HUL-SRF-1KG", name="Surf Excel 1KG", category="Home Care", unit="pcs", price_per_unit=215.0, stock_quantity=300, reorder_level=80, sap_material_code="MAT-003"),
            models.Product(sku="ITC-AATA-5KG", name="Aashirvaad Atta 5KG", category="Staples", unit="pcs", price_per_unit=270.0, stock_quantity=800, reorder_level=150, sap_material_code="MAT-004"),
            models.Product(sku="NESTLE-MAGI-12", name="Maggi Noodles 12-Pack", category="Noodles", unit="pack", price_per_unit=144.0, stock_quantity=40, reorder_level=100, sap_material_code="MAT-005"),
            models.Product(sku="HUL-LUX-3PK", name="Lux Soap 3-Pack", category="Personal Care", unit="pack", price_per_unit=120.0, stock_quantity=600, reorder_level=100, sap_material_code="MAT-006"),
            models.Product(sku="P&G-ARIEL-1KG", name="Ariel Detergent 1KG", category="Home Care", unit="pcs", price_per_unit=310.0, stock_quantity=25, reorder_level=80, sap_material_code="MAT-007"),
            models.Product(sku="COKE-750ML", name="Coca-Cola 750ml Bottle", category="Beverages", unit="btl", price_per_unit=45.0, stock_quantity=2000, reorder_level=300, sap_material_code="MAT-008"),
        ]
        for p in products:
            existing = db.query(models.Product).filter(models.Product.sku == p.sku).first()
            if not existing:
                db.add(p)
        db.commit()
        print("✅ Seed data inserted successfully!")
    finally:
        db.close()

if __name__ == "__main__":
    seed()
