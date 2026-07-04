import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re


# Volume Path anonymisiert aus Sicherheitsgründen.
VOLUME_PATH = "/Volumes/<catalog>/<schema>/<volume>/<subfolder>/"
STL_AUSSCHLIESSEN = ["WD20", "WW10", "WW20"]


def bt(name):
    return f"`{name}`"


@udf(ArrayType(StringType()))
def parse_line(line):
    if not line or not line.strip():
        return None
    line = line.strip()
    if line.startswith("-") or line.startswith("="):
        return None
    if "Material" in line and "Quantity" in line:
        return None
    for delim in ["\t", "|", ";"]:
        if delim in line:
            parts = [p.strip() for p in line.split(delim)]
            while parts and parts[0] == "":
                parts.pop(0)
            while parts and parts[-1] == "":
                parts.pop()
            if len(parts) in (14, 15) and re.match(r"^\d{4}$", parts[0]):
                return parts
    parts = re.split(r"\s{2,}", line)
    if len(parts) in (14, 15) and re.match(r"^\d{4}$", parts[0]):
        return parts
    return None


@udf(DoubleType())
def normalize_number(value):
    if value is None:
        return None
    v = str(value).strip().replace(" ", "")
    if v == "":
        return None
    if "," in v and "." in v:
        if v.rfind(",") > v.rfind("."):
            v = v.replace(".", "").replace(",", ".")
        else:
            v = v.replace(",", "")
    elif "," in v:
        after = v.split(",")[-1]
        if len(after) == 3:
            v = v.replace(",", "")
        else:
            v = v.replace(",", ".")
    try:
        return float(v)
    except:
        return None


# Bronze
@dlt.table(
    name="blocked_stock_bronze",
    comment="Rohdaten aus SAP Blocked-Stock TXT-Datei",
    table_properties={"delta.columnMapping.mode": "name"}
)
def blocked_stock_bronze():
    raw = (spark.read
        .option("encoding", "latin1")
        .text(f"{VOLUME_PATH}/EUROPE_BATCH*Blocked_Stock*.txt"))

    return (raw
        .withColumn("parsed", parse_line(col("value")))
        .filter(col("parsed").isNotNull())
        .withColumn("n_cols", size(col("parsed")))
        .drop("value"))


# Silver
@dlt.table(
    name="blocked_stock_silver",
    comment="Bereinigte und transformierte Blocked-Stock-Daten",
    table_properties={"delta.columnMapping.mode": "name"}
)
@dlt.expect_or_drop("valid_fy", "FY IS NOT NULL")
@dlt.expect_or_drop("valid_plnt", "Plnt IS NOT NULL")
def blocked_stock_silver():
    df = dlt.read("blocked_stock_bronze")

    df = (df
        .withColumn("FY",                   col("parsed")[0])
        .withColumn("Plnt",                 col("parsed")[1])
        .withColumn("Stl",                  col("parsed")[2])
        .withColumn("Mvt",                  col("parsed")[3])
        .withColumn("Material number",      col("parsed")[4])
        .withColumn("Material description", col("parsed")[5])
        .withColumn("Quantity",             col("parsed")[6])
        .withColumn("UOM",                  col("parsed")[7])
        .withColumn("Std. Cgs.",            col("parsed")[8])
    )

    df = df.withColumn("PrUnt",
        when(col("n_cols") == 15, col("parsed")[9])
        .otherwise(lit(None)))

    df = (df
        .withColumn("Blk val",
            when(col("n_cols") == 15, col("parsed")[10])
            .otherwise(col("parsed")[9]))
        .withColumn("Cur",
            when(col("n_cols") == 15, col("parsed")[11])
            .otherwise(col("parsed")[10]))
        .withColumn("Created",
            when(col("n_cols") == 15, col("parsed")[12])
            .otherwise(col("parsed")[11]))
        .withColumn("Lst chg",
            when(col("n_cols") == 15, col("parsed")[13])
            .otherwise(col("parsed")[12]))
        .withColumn("Age",
            when(col("n_cols") == 15, col("parsed")[14])
            .otherwise(col("parsed")[13]))
    )

    df = df.drop("parsed", "n_cols")

    for c in ["FY", "Plnt", "Stl", "Mvt", "Material number",
              "Material description", "UOM", "Cur"]:
        df = df.withColumn(c, trim(col(bt(c))))

    for c in ["Quantity", "Std. Cgs.", "Blk val", "PrUnt"]:
        df = df.withColumn(c, normalize_number(col(bt(c))))

    df = df.withColumn("Age", col(bt("Age")).cast(IntegerType()))

    for c in ["Created", "Lst chg"]:
        df = df.withColumn(c, to_date(col(bt(c)), "dd.MM.yyyy"))

    df = df.filter(~col(bt("Stl")).isin(STL_AUSSCHLIESSEN))

    df = df.withColumn("Mvt",
        when(col(bt("Mvt")).isin("", "nan", "None") | col(bt("Mvt")).isNull(),
             lit("Sonstiges"))
        .otherwise(col(bt("Mvt"))))

    df = df.withColumn("Tableau_Timestamp", current_timestamp())

    df = df.withColumn("KW _Reporting",
        weekofyear(date_add(col(bt("Tableau_Timestamp")).cast("date"), 7)))

    df = df.withColumn("Age_Category",
        when(col(bt("Age")) < 30,                                       lit("-30 days"))
        .when((col(bt("Age")) >= 30) & (col(bt("Age")) <= 60),          lit("30-60 days"))
        .when(col(bt("Age")) > 60,                                      lit("+60 days"))
        .otherwise(lit(None)))

    return df


# Gold
@dlt.table(
    name="blocked_stock_gold",
    comment="Aggregierte Blocked-Stock-Daten – finale Ausgabe",
    table_properties={"delta.columnMapping.mode": "name"}
)
def blocked_stock_gold():
    df = dlt.read("blocked_stock_silver")

    KEY_COLUMNS = [
        "FY", "KW _Reporting", "Plnt", "Stl", "Mvt",
        "Material number", "Age_Category", "Created", "Lst chg", "Age"
    ]

    agg_exprs = [
        sum(col(bt("Quantity"))).alias("Quantity"),
        sum(col(bt("Blk val"))).alias("Blk val"),
        max(col(bt("Tableau_Timestamp"))).alias("Tableau_Timestamp"),
        last(col(bt("Material description"))).alias("Material description"),
        last(col(bt("UOM"))).alias("UOM"),
        last(col(bt("Cur"))).alias("Cur"),
        count("*").alias("combined_row_count"),
        (sum(col(bt("PrUnt")) * col(bt("Quantity"))) / sum(col(bt("Quantity"))))
            .alias("PrUnt"),
    ]

    df = df.groupBy([bt(c) for c in KEY_COLUMNS]).agg(*agg_exprs)

    df = df.withColumn("Std. Cgs.",
        when(col(bt("Quantity")) != 0,
             col(bt("Blk val")) / col(bt("Quantity")))
        .otherwise(lit(None)))

    final_cols = [
        "Age_Category", "Tableau_Timestamp", "KW _Reporting", "FY",
        "Plnt", "Stl", "Mvt", "Material number", "Material description",
        "UOM", "Quantity", "Std. Cgs.", "PrUnt", "Blk val", "Cur",
        "Created", "Lst chg", "Age", "combined_row_count"
    ]

    return df.select([bt(c) for c in final_cols])