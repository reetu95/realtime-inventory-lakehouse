import pandas as pd

xl = pd.ExcelFile("online_retail_II.xlsx")
print("Sheets found:", xl.sheet_names)

df = pd.concat([xl.parse(s) for s in xl.sheet_names], ignore_index=True)
df.columns = [c.strip() for c in df.columns]
print("Columns:", list(df.columns))
print("Total rows:", len(df))

# --- Clean ---
df = df.dropna(subset=["Description"])
df = df[df["Price"] > 0]
df = df[~df["Invoice"].astype(str).str.startswith("C")]      # remove cancellations
df = df[df["StockCode"].astype(str).str.match(r"^\d{5}")]    # real 5-digit product codes only
print("Rows after cleaning:", len(df))

# --- Products reference ---
products = (
    df.groupby("StockCode")
    .agg(
        description=("Description", "first"),
        unit_price=("Price", "median")
    )
    .reset_index()
    .head(300)
)

products.to_csv("ref_products.csv", index=False)

# --- Warehouses from top countries ---
top = df["Country"].value_counts().head(8).index

pd.DataFrame({
    "warehouse_id": [f"WH-{i+1:02d}" for i in range(len(top))],
    "country": top,
}).to_csv("ref_warehouses.csv", index=False)

print(f"Done: {len(products)} products, {len(top)} warehouses written.")
