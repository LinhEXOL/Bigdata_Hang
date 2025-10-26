# -*- coding: utf-8 -*-
# PySpark 3.x

import re
import unicodedata
from pyspark.sql import functions as F, Window as W
from pyspark.sql.types import ArrayType, IntegerType, StringType
from pyspark.sql import functions as F


# =========================
# 0) UDFs
# =========================
@F.udf(returnType=ArrayType(IntegerType()))
def extract_exp_pattern(kinh_nghiem: str):
    # lấy ra tất cả các số nguyên <-> số năm kinh nghiệm
    if kinh_nghiem is None:
        return []
    exp = re.findall(r'\b\d+\b', kinh_nghiem)
    exp_range = [int(number) for number in exp]
    if len(exp_range) == 2:
        end = exp_range[1]
        start = exp_range[0]
        if end >= 10:
            end = 10
        start = int(start / 1)
        end = int(end / 1)
        exp_range = [1 * i for i in range(start, end + 1)]
    return exp_range

def _strip_accents(s: str) -> str:
    if s is None:
        return ""
    # bỏ dấu, lower, loại ký tự không cần thiết
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

@F.udf(returnType=StringType())
def normalize_text(s: str) -> str:
    return _strip_accents(s)

# Loại các từ hình thức DN & filler khỏi tên công ty để so khớp khoan dung hơn
COMPANY_STOP = set("""
cong ty tnhh mtv mtvv co phan cp tap doan doanh nghiep tmdv tm dv thuong mai dich vu
company jsc llc ltd co., co, inc., inc group viet nam vietnam vn
""".split())

@F.udf(returnType=StringType())
def signature_tokens(s: str) -> str:
    s = _strip_accents(s)
    toks = [t for t in s.split() if t not in COMPANY_STOP]
    # dùng tập token đã sắp xếp → ổn định trước khác biệt nhỏ về thứ tự
    toks = sorted(set(toks))
    return " ".join(toks)

@F.udf(returnType=StringType())
def render_exp_from_list(exp_list):
    # chuyển [1,2,3] -> "1 - 3 năm"; [2] -> "2 năm"
    if not exp_list:
        return None
    xs = sorted(set([int(x) for x in exp_list if x is not None]))
    if not xs:
        return None
    mn, mx = xs[0], xs[-1]
    if mn == mx:
        return f"{mn} năm"
    return f"{mn} - {mx} năm"

def pick_longest(colname: str):
    # lấy giá trị dài nhất (thường đầy đủ hơn) trong nhóm, bỏ qua null/rỗng
    expr = f"max_by(coalesce(`{colname}`, ''), length(coalesce(`{colname}`, '')))"
    return F.expr(expr).alias(colname)

# =========================
# 1) Đọc dữ liệu 3 nguồn (giữ nguyên schema đã có)
# =========================
# Giả định bạn đã có SparkSession `spark` và biến `schema`
raw_1 = spark.read.schema(schema).option("multiline", "true").json("hdfs://node01:8020/result/1.json").withColumn("_src", F.lit("s1"))
raw_2 = spark.read.schema(schema).option("multiline", "true").json("hdfs://node01:8020/result/2.json").withColumn("_src", F.lit("s2"))
raw_3 = spark.read.schema(schema).option("multiline", "true").json("hdfs://node01:8020/result/3.json").withColumn("_src", F.lit("s3"))

df = raw_1.unionByName(raw_2).unionByName(raw_3)

# =========================
# 2) Chuẩn hoá các cột dùng cho so khớp
# =========================
COL_TITLE = "tên công việc"
COL_COMP  = "tên công ty"
COL_CITY  = "địa điểm"
COL_SAL   = "mức lương"
COL_EXP   = "kinh nghiệm"

df_std = (
    df
    .withColumn("_title_norm", normalize_text(F.col(COL_TITLE)))
    .withColumn("_comp_norm",  normalize_text(F.col(COL_COMP)))
    .withColumn("_city_norm",  normalize_text(F.col(COL_CITY)))
    .withColumn("_title_sig",  signature_tokens(F.col(COL_TITLE)))
    .withColumn("_comp_sig",   signature_tokens(F.col(COL_COMP)))
    .withColumn("_exp_list",   extract_exp_pattern(F.col(COL_EXP)))
)

# Khóa chặn (blocking key) — thành phố + chữ ký tiêu đề + chữ ký công ty
df_blk = df_std.withColumn(
    "_block_key",
    F.sha2(F.concat_ws("|", F.col("_city_norm"), F.col("_title_sig"), F.col("_comp_sig")), 256)
)

# =========================
# 3) Hợp nhất theo cụm trùng (survivorship)
# - Gộp theo _block_key
# - Lấy bản "đầy đủ nhất" cho từng trường (chuẩn: dài nhất)
# - Điền kinh nghiệm nếu thiếu bằng danh sách gom lại
# =========================
agg_cols = [
    pick_longest("stt"),
    pick_longest(COL_TITLE),
    pick_longest(COL_COMP),
    pick_longest(COL_CITY),
    pick_longest(COL_SAL),
    pick_longest(COL_EXP),
    pick_longest("mô tả công việc"),
    pick_longest("kĩ năng yêu cầu"),
    pick_longest("quyền lợi"),
    pick_longest("thời gian làm việc"),
    pick_longest("hạn nộp"),
    F.array_distinct(F.flatten(F.collect_list(F.col("_exp_list")))).alias("_exp_list_all"),
]

merged = (
    df_blk
    .groupBy("_block_key")
    .agg(*agg_cols)
    # nếu cột "kinh nghiệm" đang rỗng, điền từ _exp_list_all
    .withColumn(
        COL_EXP,
        F.when(
            (F.col(COL_EXP).isNull()) | (F.length(F.trim(F.col(COL_EXP))) == 0),
            render_exp_from_list(F.col("_exp_list_all"))
        ).otherwise(F.col(COL_EXP))
    )
)

# =========================
# 4) Làm sạch & đánh lại STT, giữ nguyên schema gốc
# =========================
# điền STT mới (1..N)
win = W.orderBy(F.col(COL_TITLE).asc_nulls_last())
merged2 = merged.withColumn("stt", F.row_number().over(win).cast("int"))

# chọn đúng thứ tự cột theo schema (giữ nguyên)
schema_cols = [f.name for f in schema]  # đảm bảo đúng tên & thứ tự
final_df = merged2.select(*schema_cols)

# =========================
# 5) Ghi ra HDFS (1 part)
# =========================
out_path = "hdfs://node01:8020/result/merged_dedup"
(
    final_df
    .coalesce(1)                   # 1 file part
    .write
    .mode("overwrite")
    .json(out_path)                # JSON lines; trong HDFS sẽ là 1 thư mục có part-*.json
)

print("Done. Output folder:", out_path)
