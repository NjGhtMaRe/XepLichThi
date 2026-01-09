import pandas as pd
from ortools.sat.python import cp_model
from itertools import combinations
from collections import defaultdict
import os

# Lấy thư mục hiện tại của file script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

model = cp_model.CpModel()

# ======================
# 1. ĐỌC FILE
# ======================
path_lhp = os.path.join(BASE_DIR, "danhsachLHP.xlsx")
path_data = os.path.join(BASE_DIR, "Data.xlsx")
path_cfg = os.path.join(BASE_DIR, "cau_hinh.xlsx")
path_sv = os.path.join(BASE_DIR, "danhsachSV.xlsx")

df_lhp = pd.read_excel(path_lhp)
df_data = pd.read_excel(path_data)
df_cfg = pd.read_excel(path_cfg)

# Chuẩn hóa MaHP trong df_lhp để khớp với df_sv
df_lhp["MaHP"] = df_lhp["MaHP"].astype(str).str.strip()

df_sv = pd.read_excel(path_sv)

# Chuẩn hóa
df_sv["MaSV"] = df_sv["MaSV"].astype(str).str.strip()
df_sv["Ten"] = df_sv["Ten"].astype(str).str.strip()
df_sv["MaHP"] = df_sv["MaHP"].astype(str).str.strip()

# CRITICAL: Loại bỏ duplicate (MaSV, MaHP) để tránh sinh viên bị gán nhiều lần
original_count = len(df_sv)
df_sv = df_sv.drop_duplicates(subset=["MaSV", "MaHP"], keep="first")
sv_after_dedup = len(df_sv)
duplicates_removed = original_count - sv_after_dedup

print(f"\n📊 DEBUG - DATAFLOW TRACKING:")
print(f"   1. df_sv (raw from danhsachSV): {original_count} dòng")
print(f"   2. df_sv (sau dedup MaSV+MaHP): {sv_after_dedup} dòng (bỏ {duplicates_removed} duplicates)")
print(f"   3. Unique students: {df_sv['MaSV'].nunique()}")
print(f"   4. Unique courses: {df_sv['MaHP'].nunique()}")

df_hk = pd.read_excel(path_cfg, sheet_name="HK")
df_thoigianthi = pd.read_excel(
    path_cfg,
    sheet_name="ThoiGianThi"
)
df_thoigianthi.columns = df_thoigianthi.columns.str.strip()
df_hk = pd.read_excel(
    path_cfg,
    sheet_name="HK"
)

df_hk.columns = df_hk.columns.str.strip()

NAM_TH = int(df_hk.loc[0, "NamTH"])
HK_TH = int(df_hk.loc[0, "HKTH"])

df_ca_thi = pd.read_excel(
    path_cfg,
    sheet_name="CaThi"
)

df_ca_thi.columns = df_ca_thi.columns.str.strip()

df_phongthi = pd.read_excel(
    path_cfg,
    sheet_name="PhongThi"
)

df_phongthi.columns = df_phongthi.columns.str.strip()

# Load PhongThiMay sheet for I-Test (machine-based exams)
try:
    df_phongthi_may = pd.read_excel(path_cfg, sheet_name="PhongThiMay")
    df_phongthi_may.columns = df_phongthi_may.columns.str.strip()
    ITEST_ENABLED = True
    print(f"   ✅ Đã load PhongThiMay: {len(df_phongthi_may)} dòng")
    print(f"      Columns: {list(df_phongthi_may.columns)}")
    print(f"      Data preview:\n{df_phongthi_may.head()}")
except Exception as e:
    df_phongthi_may = pd.DataFrame()
    ITEST_ENABLED = False
    print(f"   ⚠️ Không tìm thấy sheet PhongThiMay, bỏ qua I-Test phase: {e}")

# Debug: Check HinhThucThi in df_lhp
if "HinhThucThi" in df_lhp.columns:
    itest_count = len(df_lhp[df_lhp["HinhThucThi"] == 1])
    print(f"   📊 df_lhp có cột HinhThucThi, {itest_count} môn có HinhThucThi=1")
else:
    print(f"   ⚠️ df_lhp KHÔNG có cột HinhThucThi. Columns: {list(df_lhp.columns)}")

df_quytac = pd.read_excel(
    path_cfg,
    sheet_name="QuyTac"
)

df_quytac.columns = df_quytac.columns.str.strip()

QUY_TAC = dict(
    zip(df_quytac["ThamSo"], df_quytac["GiaTri"])
)

df_uutien = pd.read_excel(
    path_cfg,
    sheet_name="UuTien"
)

df_uutien.columns = df_uutien.columns.str.strip()

HE_SO_UU_TIEN = dict(
    zip(df_uutien["TieuChi"], df_uutien["HeSo"])
)

# ======================
# 2. LẤY NĂM + HỌC KỲ CẦN TỔ CHỨC THI
# ======================
NAM_TH = int(df_hk.loc[0, "NamTH"])
HK_TH = int(df_hk.loc[0, "HKTH"])

# ======================
# 3. LỌC DATA THEO NĂM + HK
# ======================
df_data_thi = df_data[
    (df_data["NamTH"] == NAM_TH) &
    (df_data["HKTH"] == HK_TH)
]

# ======================
# 4. MERGE LHP + DATA
# ======================
df_merge = df_lhp.merge(
    df_data_thi[["MaHP", "Khoa", "CTDT"]],
    on="MaHP",
    how="left"
)

# ======================
# 5. PHÂN LOẠI MaHP
# ======================

# 5.1 MaHP không thuộc CTĐT nào
mon_khong_ctdt = df_merge[df_merge["CTDT"].isna()].copy()

# 5.2 MaHP có CTĐT
mon_co_ctdt = df_merge[df_merge["CTDT"].notna()].copy()

# ======================
# 6. XÁC ĐỊNH MÔN CHUNG / RIÊNG (CHUẨN NGHIỆP VỤ)
# ======================

# Ghép CTDT - Khóa để nhận diện duy nhất
mon_co_ctdt["CTDT_Khoa"] = (
    mon_co_ctdt["CTDT"].astype(str) + "-" +
    mon_co_ctdt["Khoa"].astype(str)
)

# Group theo MaHP → gom ngang
df_mon = (
    mon_co_ctdt
    .groupby("MaHP")
    .agg(
        So_CTDT_Khoa=("CTDT_Khoa", "nunique"),
        DS_CTDT_Khoa=("CTDT_Khoa", lambda x: "; ".join(sorted(set(x))))
    )
    .reset_index()
)
# ======================
# 6.5. TẠO LIST CTĐT + KHÓA → DANH SÁCH MaHP
# ======================

list_ctdt_khoa = (
    df_data_thi
    .dropna(subset=["CTDT", "Khoa", "MaHP"])
    .groupby(["CTDT", "Khoa"])
    .agg(
        SoMon=("MaHP", "nunique"),
        DanhSach_MaHP=("MaHP", lambda x: "; ".join(sorted(x.unique())))
    )
    .reset_index()
)

# ======================
# 7. TẠO LIST MÔN CHUNG / RIÊNG (1 DÒNG / 1 MaHP)
# ======================

# Môn chung: nhiều CTĐT / Khóa
list_mon_chung = df_mon[df_mon["So_CTDT_Khoa"] > 1].copy()

# Môn riêng: chỉ 1 CTĐT / Khóa
list_mon_rieng = df_mon[df_mon["So_CTDT_Khoa"] == 1].copy()

# ======================
# 8. IN KẾT QUẢ
# ======================

print("===== MÔN THI CHUNG (nhiều CTĐT / Khóa) =====")
print(
    list_mon_chung[
        ["MaHP", "So_CTDT_Khoa", "DS_CTDT_Khoa"]
    ].sort_values("MaHP")
)

print("\n===== MÔN THI RIÊNG (1 CTĐT / Khóa) =====")
print(
    list_mon_rieng[
        ["MaHP", "So_CTDT_Khoa", "DS_CTDT_Khoa"]
    ].sort_values("MaHP")
)

# ======================
# 8.1. XUẤT FILE DANH SÁCH MÔN CHUNG / RIÊNG
# ======================
output_mon_path = os.path.join(BASE_DIR, "danh_sach_mon_chung_rieng.xlsx")

with pd.ExcelWriter(output_mon_path, engine="xlsxwriter") as writer:
    # Sheet môn chung
    list_mon_chung[
        ["MaHP", "So_CTDT_Khoa", "DS_CTDT_Khoa"]
    ].sort_values("So_CTDT_Khoa", ascending=False).to_excel(
        writer,
        sheet_name="MonChung",
        index=False
    )
    
    # Sheet môn riêng
    list_mon_rieng[
        ["MaHP", "So_CTDT_Khoa", "DS_CTDT_Khoa"]
    ].sort_values("MaHP").to_excel(
        writer,
        sheet_name="MonRieng",
        index=False
    )

print(f"\n✅ Đã xuất danh sách môn chung/riêng: {output_mon_path}")
print(f"   📊 Môn chung: {len(list_mon_chung)}, Môn riêng: {len(list_mon_rieng)}")

print("\n===== MÔN KHÔNG THUỘC CTĐT =====")
print(
    mon_khong_ctdt[
        ["MaHP", "HinhThucThi", "PhongThi"]
    ]
    .drop_duplicates()
    .sort_values("MaHP")
)

print("\n===== DANH SÁCH MÔN THEO CTĐT + KHÓA =====")
print(
    list_ctdt_khoa[
        ["CTDT", "Khoa", "SoMon", "DanhSach_MaHP"]
    ]
)

# ======================
# 9. Danh sách môn thi thực tế
# ======================
ds_mahp_thi = (
    df_lhp["MaHP"]
    .drop_duplicates()
)
df_data_thi_mon = (
    df_data_thi[
        df_data_thi["MaHP"].isin(ds_mahp_thi)
    ]
    .copy()
)

# ======================
# 10. Danh sách ngày thi hợp lệ
# ======================
ngay_thi = (
    df_thoigianthi
    .query("SuDung == 1")["NgayThi"]
    .sort_values()
    .tolist()
)

# ======================
# 10.1 Danh sách phòng thi từ cau_hinh.xlsx
# ======================
PHONG_KHA_DUNG = (
    df_phongthi["PhongThi"]
    .dropna()
    .astype(str)
    .str.strip()
    .tolist()
)

# Phân loại phòng theo loại (PH: Phòng Học, PM: Phòng Máy)
PHONG_PH = [p for p in PHONG_KHA_DUNG if p.startswith("PH")]
PHONG_PM = [p for p in PHONG_KHA_DUNG if p.startswith("PM")]

# I-Test: Extract rooms and dates from PhongThiMay
PHONG_ITEST = []
NGAY_ITEST = []
NGAY_ITEST_IDX = []  # Index for solver

if ITEST_ENABLED and not df_phongthi_may.empty:
    print(f"   🔍 PhongThiMay columns: {list(df_phongthi_may.columns)}")
    
    # Get I-Test rooms (try multiple column names)
    phong_col = next((c for c in df_phongthi_may.columns if "PhongThi" in c or "Phong" in c), None)
    if phong_col:
        PHONG_ITEST = df_phongthi_may[phong_col].dropna().astype(str).str.strip().unique().tolist()
        print(f"   ✅ Found PhongThi column: '{phong_col}'")
    else:
        print(f"   ⚠️ Không tìm thấy cột PhongThi!")
    
    # Get I-Test dates (try multiple column names)
    ngay_col = next((c for c in df_phongthi_may.columns if "NgayThi" in c or "Ngay" in c), None)
    if ngay_col:
        NGAY_ITEST = df_phongthi_may[ngay_col].dropna().unique().tolist()
        print(f"   ✅ Found NgayThi column: '{ngay_col}'")
    else:
        print(f"   ⚠️ Không tìm thấy cột NgayThi!")
    
    print(f"   📊 I-Test: {len(PHONG_ITEST)} phòng, {len(NGAY_ITEST)} ngày")
    print(f"      Phòng I-Test: {PHONG_ITEST}")
    print(f"      Ngày I-Test: {NGAY_ITEST}")
    
    # CRITICAL: Exclude I-Test rooms from main room pools
    PHONG_PH = [p for p in PHONG_PH if p not in PHONG_ITEST]
    PHONG_PM = [p for p in PHONG_PM if p not in PHONG_ITEST]
    print(f"   ⚠️ Đã loại bỏ phòng I-Test khỏi phòng thi thường. PH còn: {len(PHONG_PH)}, PM còn: {len(PHONG_PM)}")

print(f"\n📊 Phòng thi (sau loại I-Test): {len(PHONG_PH)} phòng PH, {len(PHONG_PM)} phòng PM")

# Sức chứa phòng thi
SUC_CHUA_PHONG = dict(
    zip(
        df_phongthi["PhongThi"].astype(str).str.strip(),
        df_phongthi["SucChua"]
    )
)

print("\n===== DANH SÁCH PHÒNG THI KHẢ DỤNG =====")
for phong in PHONG_KHA_DUNG:
    print(f"  {phong}: {SUC_CHUA_PHONG.get(phong, 'N/A')} chỗ")

NGAY = list(range(1, len(ngay_thi) + 1))
map_ngay = dict(zip(NGAY, ngay_thi))

# Map I-Test dates to indices
if NGAY_ITEST:
    print(f"   🔍 Debug - NGAY_ITEST: {NGAY_ITEST}")
    print(f"   🔍 Debug - NGAY_ITEST types: {[type(d).__name__ for d in NGAY_ITEST]}")
    print(f"   🔍 Debug - map_ngay values: {list(map_ngay.values())[:5]}")
    print(f"   🔍 Debug - map_ngay types: {[type(d).__name__ for d in list(map_ngay.values())[:3]]}")
    
    for itest_date in NGAY_ITEST:
        matched = False
        for idx, regular_date in map_ngay.items():
            # Normalize both dates to comparable format
            try:
                itest_dt = pd.to_datetime(itest_date)
                regular_dt = pd.to_datetime(regular_date)
                
                # Compare dates only (ignore time)
                if itest_dt.date() == regular_dt.date():
                    NGAY_ITEST_IDX.append(idx)
                    print(f"      ✅ Matched: {itest_date} ({itest_dt.date()}) -> index {idx}")
                    matched = True
                    break
            except Exception as ex:
                # Fallback to string comparison
                if str(itest_date) == str(regular_date):
                    NGAY_ITEST_IDX.append(idx)
                    print(f"      ✅ Matched (str): {itest_date} -> index {idx}")
                    matched = True
                    break
        
        if not matched:
            print(f"      ❌ No match for: {itest_date} (type: {type(itest_date).__name__})")
    
    print(f"   📊 I-Test ngày index: {NGAY_ITEST_IDX}")
    if not NGAY_ITEST_IDX:
        print(f"   ⚠️ WARNING: No I-Test dates matched! Phase 0 will be skipped.")

# ======================
# 11. Danh sách ca thi
# ======================
ca_thi = (
    df_ca_thi["Ca"]
    .sort_values()
    .tolist()
)

# ======================
# 12. Thông tin phòng theo môn
# ======================
phong_theo_mon = (
    df_lhp
    .set_index("MaHP")[["ToThi", "PhongThi"]]
    .to_dict("index")
)

# DEBUG: Track total ToThi from input
total_tothi_input = sum(info["ToThi"] for info in phong_theo_mon.values())
print(f"\n📊 DEBUG - phong_theo_mon: {len(phong_theo_mon)} môn, Tổng ToThi: {total_tothi_input}")

# ======================
# 13. CTĐT + Khóa - Danh sách môn thi
# ======================
ctdt_khoa_mon_thi = (
    df_data_thi_mon
    .groupby(["CTDT", "Khoa"])
    .agg(
        SoMon=("MaHP", "nunique"),
        DanhSachMonThi=("MaHP", lambda x: ", ".join(sorted(x.unique())))
    )
    .reset_index()
)

# ======================
# 14. MaHP → DANH SÁCH CTĐT / KHÓA THAM GIA
# ======================

map_mon_ctdt = (
    df_data_thi_mon
    .merge(
        df_lhp[["MaHP"]].drop_duplicates(),
        on="MaHP",
        how="inner"
    )
    .groupby("MaHP")
    .apply(
        lambda x: list(
            set(zip(x["CTDT"], x["Khoa"]))
        )
    )
    .to_dict()
)

# ======================
# 15. DANH SÁCH MÔN KHÔNG THUỘC CTĐT
# ======================
mon_khong_ctdt = (
    df_lhp[
        ~df_lhp["MaHP"].isin(df_data_thi["MaHP"])
    ]["MaHP"]
    .drop_duplicates()
    .tolist()
)

# ======================
# 16. In thử bước 2
# ======================

print("===== TỔNG SỐ MÔN THI =====")
print(len(ds_mahp_thi))

print("\n===== DANH SÁCH NGÀY THI =====")
print(ngay_thi)

print("\n===== DANH SÁCH CA THI =====")
print(ca_thi)

print("\n===== CTĐT - KHÓA - SỐ MÔN =====")
print(ctdt_khoa_mon_thi[["CTDT", "Khoa", "SoMon", "DanhSachMonThi"]])

print("\n===== MÔN KHÔNG THUỘC CTĐT =====")
print(mon_khong_ctdt)


# ======================
# 3.2. SV -> danh sách môn thi
# ======================

mon_sv = (
    df_sv
    .groupby("MaHP")
    .agg(
        SoSV=("MaSV", "nunique"),
        DanhSachSV=("MaSV", lambda x: sorted(x.unique()))
    )
    .reset_index()
)

# ======================
# 3.3. RẢI SINH VIÊN VÀO TỔ THI (CHIA ĐỀU – ABC)
# ======================
ds_sv_to_thi = []

# DEBUG: Track skipped students/courses
skipped_courses = []
skipped_students = 0

# DEBUG: Compare courses between df_sv and phong_theo_mon
sv_courses = set(df_sv["MaHP"].unique())
lhp_courses = set(phong_theo_mon.keys())
courses_only_in_sv = sv_courses - lhp_courses
courses_only_in_lhp = lhp_courses - sv_courses
common_courses = sv_courses & lhp_courses

print(f"\n📊 DEBUG - COURSE COMPARISON:")
print(f"   Courses in df_sv: {len(sv_courses)}")
print(f"   Courses in phong_theo_mon (df_lhp): {len(lhp_courses)}")
print(f"   Common courses: {len(common_courses)}")
print(f"   Courses ONLY in df_sv (will be skipped): {len(courses_only_in_sv)}")
print(f"   Courses ONLY in df_lhp (no students): {len(courses_only_in_lhp)}")

# Expected SLSV check
if "SLSV" in df_lhp.columns:
    expected_slsv = df_lhp["SLSV"].sum()
    print(f"   Expected SLSV (from df_lhp.SLSV): {expected_slsv}")
    
    # Breakdown by common vs missing courses
    df_lhp_common = df_lhp[df_lhp["MaHP"].isin(common_courses)]
    df_lhp_missing = df_lhp[df_lhp["MaHP"].isin(courses_only_in_lhp)]
    
    slsv_common = df_lhp_common["SLSV"].sum()
    slsv_missing = df_lhp_missing["SLSV"].sum()
    
    print(f"\n📊 DEBUG - SLSV BREAKDOWN:")
    print(f"   SLSV from common courses (có SV data): {slsv_common}")
    print(f"   SLSV from missing courses (KHÔNG có SV data): {slsv_missing}")
    print(f"   Total check: {slsv_common + slsv_missing} (should = {expected_slsv})")
    
    if slsv_missing > 0:
        print(f"\n⚠️ CÁC MÔN THIẾU SINH VIÊN (trong df_lhp nhưng không có trong df_sv):")
        missing_detail = df_lhp_missing[["MaHP", "SLSV", "ToThi"]].drop_duplicates().head(22)
        print(missing_detail.to_string())

for mahp, df_mhp in df_sv.groupby("MaHP"):
    # Chỉ xử lý môn có trong danh sách thi
    if mahp not in phong_theo_mon:
        skipped_courses.append(mahp)
        skipped_students += len(df_mhp)
        continue

    so_to = int(phong_theo_mon[mahp]["ToThi"])

    # Sắp xếp SV theo tên ABC
    df_mhp_sorted = (
        df_mhp
        .sort_values("Ten")
        .reset_index(drop=True)
    )

    N = len(df_mhp_sorted)
    if N == 0:
        continue

    base = N // so_to
    du = N % so_to

    start_idx = 0

    for to in range(1, so_to + 1):
        # Các tổ đầu được +1 SV nếu còn dư
        so_sv_to = base + (1 if to <= du else 0)

        df_to = df_mhp_sorted.iloc[start_idx:start_idx + so_sv_to]

        for _, row in df_to.iterrows():
            ds_sv_to_thi.append({
                "MaSV": row["MaSV"],
                "Ten": row["Ten"],
                "MaHP": mahp,
                "ToThi": to
            })

        start_idx += so_sv_to

# ======================
# 3.4. DataFrame kết quả rải SV
# ======================
df_sv_to_thi = pd.DataFrame(ds_sv_to_thi)
print(df_sv_to_thi)

# DEBUG: Report skipped data
print(f"\n📊 DEBUG - STUDENT DISTRIBUTION RESULT:")
print(f"   df_sv_to_thi: {len(df_sv_to_thi)} dòng (MaSV + MaHP + ToThi)")
print(f"   Unique SV được rải: {df_sv_to_thi['MaSV'].nunique() if len(df_sv_to_thi) > 0 else 0}")

if skipped_courses:
    print(f"\n⚠️ DEBUG - SV BỊ BỎ QUA (môn không có trong danh sách thi):")
    print(f"   Số môn bị bỏ: {len(skipped_courses)}")
    print(f"   Số SV-lượt bị bỏ: {skipped_students}")
    print(f"   Môn bị bỏ (top 10): {skipped_courses[:10]}")

# ======================
# 3.1. SV -> danh sách môn thi
# ======================
sv_mon_thi = (
    df_sv
    .groupby("MaSV")
    .agg(
        Ten=("Ten", "first"),
        DanhSachMonThi=("MaHP", lambda x: sorted(x.unique())),
        SoMonThi=("MaHP", "nunique")
    )
    .reset_index()
)

sv_to_mon = (
    df_sv
    .groupby("MaSV")["MaHP"]
    .apply(lambda x: sorted(x.unique()))
    .to_dict()
)

map_to_sv = (
    df_sv_to_thi
    .groupby(["MaHP", "ToThi"])["MaSV"]
    .apply(list)
    .to_dict()
)

# ======================
# 3.5. Map ngược: (MaHP, ToThi) → Danh sách SV
# ======================
map_to_sv = (
    df_sv_to_thi
    .groupby(["MaHP", "ToThi"])["MaSV"]
    .apply(list)
    .to_dict()
)

print("\n===== [3.1] DANH SÁCH MaHP TỔ CHỨC THI =====")
print(ds_mahp_thi.sort_values().tolist())
print("→ Tổng số môn:", len(ds_mahp_thi))

print("\n===== [3.2] DANH SÁCH NGÀY THI =====")
for i, d in enumerate(ngay_thi, 1):
    print(f"{i}. {d}")

print("\n===== [3.2] DANH SÁCH CA THI =====")
for ca in ca_thi:
    print("Ca:", ca)

print("\n===== [3.3] THÔNG TIN TỔ THI THEO MÔN =====")
for mahp, info in phong_theo_mon.items():
    print(
        f"{mahp}: "
        f"SoToThi={info['ToThi']}, "
        f"PhongThi={info['PhongThi']}"
    )
print("\n===== [3.4] CTĐT - KHÓA - DANH SÁCH MÔN THI =====")
for _, row in ctdt_khoa_mon_thi.iterrows():
    print(
        f"CTDT={row['CTDT']}, "
        f"Khoa={row['Khoa']} | "
        f"SoMon={row['SoMon']} | "
        f"MonThi=[{row['DanhSachMonThi']}]"
    )
print("\n===== [3.5] SINH VIÊN → DANH SÁCH MÔN THI =====")
print(sv_mon_thi.head(20))
print("→ Tổng số SV:", len(sv_mon_thi))
print("\n===== [3.6] MÔN THI → SỐ SINH VIÊN =====")
for _, row in mon_sv.iterrows():
    print(
        f"{row['MaHP']}: "
        f"SoSV={row['SoSV']}"
    )
print("\n===== [3.7] RẢI SINH VIÊN VÀO TỔ THI =====")
print(df_sv_to_thi.head(30))
print("→ Tổng dòng (SV × môn):", len(df_sv_to_thi))
print("\n===== [3.8] (MaHP, ToThi) → DANH SÁCH SV =====")
for k, v in list(map_to_sv.items())[:10]:
    print(k, "→", len(v), "SV")

# ======================
# BƯỚC 4 – XẾP LỊCH 2 GIAI ĐOẠN (TWO-PHASE SCHEDULING)
# ======================

# Định nghĩa Ngày và Ca thi từ dữ liệu đã load
DAYS = NGAY 
CA = ca_thi

def run_solver_phase(
    phase_name, 
    ds_mon_to_schedule, 
    fixed_schedule=None, 
    time_limit=60,
    relax_same_day=False,  # Nếu True: chuyển ràng buộc "trùng ngày" từ HARD sang SOFT
    restricted_days=None,  # List[int]: Danh sách các ngày cho phép xếp lịch
    prioritize_early=True,  # Nếu True: Ưu tiên xếp vào các ngày đầu
    distribute_uniformly=False, # Nếu True: Cố gắng rải đều (Min-Max)
    max_to_per_slot=None  # Số tổ thi tối đa mỗi slot (None = dùng PHONG_KHA_DUNG)
):
    """
    Hàm chạy solver cho một tập các môn.
    - restricted_days: Chỉ xếp môn vào các ngày trong list này (cho Phase 2)
    - prioritize_early: Có ưu tiên xếp sớm hay không (False cho Phase 3 để rải đều)
    - distribute_uniformly: Thêm hàm mục tiêu để cân bằng tải giữa các ngày và các ca
    """
    print(f"\n🚀 Đang chạy {phase_name}...")
    print(f"   - Số môn cần xếp: {len(ds_mon_to_schedule)}")
    if fixed_schedule:
        print(f"   - Số môn đã cố định: {len(fixed_schedule)}")
    if restricted_days:
        print(f"   - Giới hạn xếp trong {len(restricted_days)} ngày đầu: {restricted_days}")
    
    # DEBUG: Tính capacity
    MAX_TO_PER_CA = max_to_per_slot if max_to_per_slot else len(PHONG_KHA_DUNG)
    total_to_thi = sum(phong_theo_mon[m]["ToThi"] for m in ds_mon_to_schedule if m in phong_theo_mon)
    max_capacity = len(DAYS) * len(CA) * MAX_TO_PER_CA
    print(f"   📊 DEBUG - Tổng tổ thi: {total_to_thi}, Capacity tối đa: {max_capacity} ({len(DAYS)} ngày x {len(CA)} ca x {MAX_TO_PER_CA} phòng)")
    
    # DEBUG: Kiểm tra môn có ToThi quá lớn
    mon_qua_lon = [(m, phong_theo_mon[m]["ToThi"]) for m in ds_mon_to_schedule if m in phong_theo_mon and phong_theo_mon[m]["ToThi"] > MAX_TO_PER_CA]
    if mon_qua_lon:
        print(f"   ⚠️ CẢNH BÁO: Có {len(mon_qua_lon)} môn có ToThi > {MAX_TO_PER_CA} phòng -> KHÔNG THỂ XẾP!")
        for m, to in mon_qua_lon[:5]:
            print(f"      - {m}: {to} tổ")
    
    # DEBUG: Kiểm tra môn không có trong phong_theo_mon
    mon_thieu = [m for m in ds_mon_to_schedule if m not in phong_theo_mon]
    if mon_thieu:
        print(f"   ⚠️ CẢNH BÁO: Có {len(mon_thieu)} môn KHÔNG có trong phong_theo_mon!")
        for m in mon_thieu[:5]:
            print(f"      - {m}")
    
    if total_to_thi > max_capacity:
        print(f"   ⚠️ CẢNH BÁO: Tổng tổ thi ({total_to_thi}) > Capacity ({max_capacity}) -> Chắc chắn INFEASIBLE!")
    
    # DEBUG: Phân tích capacity từng slot sau khi fixed
    if fixed_schedule:
        slot_usage = {}  # (d, c) -> tổng tổ thi đã fixed
        for mahp, (fix_d, fix_c) in fixed_schedule.items():
            if mahp in phong_theo_mon:
                key = (fix_d, fix_c)
                slot_usage[key] = slot_usage.get(key, 0) + phong_theo_mon[mahp]["ToThi"]
        
        # Tìm slot đã đầy hoặc gần đầy
        full_slots = [(k, v) for k, v in slot_usage.items() if v >= MAX_TO_PER_CA]
        if full_slots:
            print(f"   ⚠️ CẢNH BÁO: Có {len(full_slots)} slot ĐÃ ĐẦY (>= {MAX_TO_PER_CA} tổ):")
            for (d, c), v in full_slots[:5]:
                print(f"      - Ngày {d}, Ca {c}: {v} tổ")
        
        # Tính capacity còn lại
        total_used = sum(slot_usage.values())
        remaining_capacity = max_capacity - total_used
        new_courses_to_thi = sum(phong_theo_mon[m]["ToThi"] for m in ds_mon_to_schedule if m in phong_theo_mon and m not in fixed_schedule)
        print(f"   📊 DEBUG - Tổ thi đã fixed: {total_used}, Còn lại: {remaining_capacity}, Cần xếp thêm: {new_courses_to_thi}")

    model = cp_model.CpModel()
    
    # Biến quyết định: z[mahp, d, c]
    z = {}
    for mahp in ds_mon_to_schedule:
        for d in DAYS:
            for c in CA:
                z[(mahp, d, c)] = model.NewBoolVar(f"z_{mahp}_{d}_{c}")

    # 0. Ràng buộc Restricted Days
    if restricted_days is not None:
        valid_days_set = set(restricted_days)
        for d in DAYS:
            if d not in valid_days_set:
                for mahp in ds_mon_to_schedule:
                    # Nếu môn này đã được fixed (từ Phase trước), thì KHÔNG chặn
                    if fixed_schedule and mahp in fixed_schedule:
                        continue
                    
                    # Chặn không cho xếp vào ngày d
                    for c in CA:
                        model.Add(z[(mahp, d, c)] == 0)

    # 1. Ràng buộc: Mỗi môn thi đúng 1 ca
    for mahp in ds_mon_to_schedule:
        model.Add(
            sum(z[(mahp, d, c)] for d in DAYS for c in CA) == 1
        )

    # 2. Ràng buộc cố định (cho Phase 2/3)
    if fixed_schedule:
        for mahp, (fix_d, fix_c) in fixed_schedule.items():
            if mahp in ds_mon_to_schedule:
                # Bắt buộc môn này phải thi đúng ngày/ca đã định
                model.Add(z[(mahp, fix_d, fix_c)] == 1)

    # 3. Ràng buộc sức chứa (Số tổ <= Số phòng)
    # FIX: Phải tính capacity đã chiếm bởi fixed_schedule
    MAX_TO_PER_CA = len(PHONG_KHA_DUNG)
    
    # Tính capacity đã dùng bởi fixed_schedule ở mỗi slot
    fixed_usage = {}  # (d, c) -> tổng tổ thi đã fixed
    if fixed_schedule:
        for mahp, (fix_d, fix_c) in fixed_schedule.items():
            if mahp in phong_theo_mon:
                key = (fix_d, fix_c)
                fixed_usage[key] = fixed_usage.get(key, 0) + phong_theo_mon[mahp]["ToThi"]
    
    for d in DAYS:
        for c in CA:
            # Capacity còn lại sau khi trừ phần đã fixed
            used = fixed_usage.get((d, c), 0)
            remaining = MAX_TO_PER_CA - used
            
            # Chỉ constraint cho môn CHƯA fixed (môn mới)
            model.Add(
                sum(
                    z[(mahp, d, c)] * phong_theo_mon[mahp]["ToThi"]
                    for mahp in ds_mon_to_schedule
                    if mahp not in (fixed_schedule or {})  # Môn mới
                ) <= max(0, remaining)  # Đảm bảo không âm
            )

    # 3.5 Ràng buộc môn chia: D2 phải cách D1 ít nhất 2 ngày
    # split_courses = {MaHP_gốc: [(MaHP_D1, ToThi_D1), (MaHP_D2, ToThi_D2)]}
    MIN_GAP_SPLIT = 2  # Số ngày tối thiểu giữa D1 và D2
    ds_mon_set = set(ds_mon_to_schedule)  # Định nghĩa trước để dùng cho constraint
    
    for mahp_goc, split_list in split_courses.items():
        if len(split_list) >= 2:
            mahp_d1 = split_list[0][0]  # e.g., ACT01A_D1
            mahp_d2 = split_list[1][0]  # e.g., ACT01A_D2
            
            # Chỉ thêm constraint nếu cả 2 môn đều trong danh sách schedule
            if mahp_d1 in ds_mon_set and mahp_d2 in ds_mon_set:
                # Tạo biến ngày cho D1 và D2
                day_d1 = model.NewIntVar(1, len(DAYS), f"day_{mahp_d1}")
                day_d2 = model.NewIntVar(1, len(DAYS), f"day_{mahp_d2}")
                
                # Link day_d1 với z: day_d1 = sum(d * z[mahp_d1, d, c]) (vì chỉ 1 z=1)
                model.Add(day_d1 == sum(d * z[(mahp_d1, d, c)] for d in DAYS for c in CA))
                model.Add(day_d2 == sum(d * z[(mahp_d2, d, c)] for d in DAYS for c in CA))
                
                # Ràng buộc: day_d2 >= day_d1 + MIN_GAP_SPLIT
                model.Add(day_d2 >= day_d1 + MIN_GAP_SPLIT)

    # 4. Ràng buộc sinh viên không trùng ca
    # Phase 1/2 (relax_same_day=False): HARD CONSTRAINT
    # Phase 3 (relax_same_day=True): SOFT với penalty CỰC CAO
    ds_mon_set = set(ds_mon_to_schedule)
    penalty_sv_trung_ca = []
    
    for masv, mon_list in sv_to_mon.items():
        mon_list_filtered = [m for m in mon_list if m in ds_mon_set]
        if len(mon_list_filtered) <= 1:
            continue
        for d in DAYS:
            for c in CA:
                sum_sv = sum(z[(mahp, d, c)] for mahp in mon_list_filtered)
                
                if relax_same_day:
                    # SOFT CONSTRAINT for Phase 3 - với hệ số cực cao
                    vi_pham_sv = model.NewIntVar(0, len(mon_list_filtered), f"vpsv_{masv}_{d}_{c}")
                    model.Add(vi_pham_sv >= sum_sv - 1)
                    penalty_sv_trung_ca.append(vi_pham_sv)
                else:
                    # HARD CONSTRAINT for Phase 1/2
                    model.Add(sum_sv <= 1)

    # 4b. Penalty cho sinh viên thi NHIỀU MÔN CÙNG NGÀY (khác ca) - SOFT CONSTRAINT
    # Đây là ràng buộc mới để hạn chế tối đa SV phải thi nhiều môn trong 1 ngày
    penalty_sv_trung_ngay = []
    
    for masv, mon_list in sv_to_mon.items():
        mon_list_filtered = [m for m in mon_list if m in ds_mon_set]
        if len(mon_list_filtered) <= 1:
            continue
        
        for d in DAYS:
            # Đếm số môn SV này thi trong ngày d (bất kể ca nào)
            sum_sv_ngay = sum(z[(mahp, d, c)] for mahp in mon_list_filtered for c in CA)
            
            # Nếu SV thi > 1 môn trong ngày d -> phạt
            # vi_pham = max(0, sum - 1) = số môn vượt quá 1
            vi_pham_ngay = model.NewIntVar(0, len(mon_list_filtered), f"vpsvngay_{masv}_{d}")
            model.Add(vi_pham_ngay >= sum_sv_ngay - 1)
            penalty_sv_trung_ngay.append(vi_pham_ngay)

    # 5. Ràng buộc CTĐT-Khóa không thi cùng ngày
    # Nếu relax_same_day=True -> Soft Constraint (penalty)
    # Nếu relax_same_day=False -> Hard Constraint (cấm tuyệt đối)
    ctdt_khoa_to_mon = (
        df_data_thi_mon
        .groupby(["CTDT", "Khoa"])["MaHP"]
        .apply(list)
        .to_dict()
    )
    
    penalty_trung_ngay = []  # Chỉ dùng khi relax_same_day=True
    
    for (ctdt, khoa), mon_list in ctdt_khoa_to_mon.items():
        mon_list_filtered = [m for m in mon_list if m in ds_mon_set]
        if len(mon_list_filtered) <= 1:
            continue
        
        for d in DAYS:
            sum_mon_trong_ngay = sum(
                z[(mahp, d, c)]
                for mahp in mon_list_filtered
                for c in CA
            )
            
            if relax_same_day:
                # SOFT CONSTRAINT: Cho phép vi phạm, nhưng phạt rất nặng
                # vi_pham = max(0, sum - 1)
                vi_pham = model.NewIntVar(0, len(mon_list_filtered), f"vp_{ctdt}_{khoa}_{d}")
                model.Add(vi_pham >= sum_mon_trong_ngay - 1)
                penalty_trung_ngay.append(vi_pham)
            else:
                # HARD CONSTRAINT: Cấm tuyệt đối
                model.Add(sum_mon_trong_ngay <= 1)

    # 6. Ràng buộc CTĐT-Khóa không thi liền ngày (Mềm)
    penalty_lien_ngay = []
    
    for (ctdt, khoa), mon_list in ctdt_khoa_to_mon.items():
        mon_list_filtered = [m for m in mon_list if m in ds_mon_set]
        if len(mon_list_filtered) <= 1:
            continue
            
        for i in range(len(DAYS) - 1):
            d1, d2 = DAYS[i], DAYS[i+1]
            
            # BoolVar: có môn nào thi ngày d1 không
            has_d1 = model.NewBoolVar(f"has_{ctdt}_{khoa}_{d1}")
            sum_d1 = sum(z[(mahp, d1, c)] for mahp in mon_list_filtered for c in CA)
            model.Add(sum_d1 >= 1).OnlyEnforceIf(has_d1)
            model.Add(sum_d1 == 0).OnlyEnforceIf(has_d1.Not())
            
            # BoolVar: có môn nào thi ngày d2 không
            has_d2 = model.NewBoolVar(f"has_{ctdt}_{khoa}_{d2}")
            sum_d2 = sum(z[(mahp, d2, c)] for mahp in mon_list_filtered for c in CA)
            model.Add(sum_d2 >= 1).OnlyEnforceIf(has_d2)
            model.Add(sum_d2 == 0).OnlyEnforceIf(has_d2.Not())
            
            # Phạt nếu cả 2 ngày đều có thi
            both = model.NewBoolVar(f"both_{ctdt}_{khoa}_{d1}_{d2}")
            model.AddBoolAnd([has_d1, has_d2]).OnlyEnforceIf(both)
            model.AddBoolOr([has_d1.Not(), has_d2.Not()]).OnlyEnforceIf(both.Not())
            
            penalty_lien_ngay.append(both)

    # ======================
    # HÀM MỤC TIÊU
    # ======================
    total_objective = []
    
    # 0a. Phạt vi phạm sinh viên trùng ca (CHỈ KHI relax_same_day=True)
    # Hệ số CỰC LỚN vì đây là vi phạm nghiêm trọng nhất
    HE_SO_SV_TRUNG_CA = 100000000
    for pen in penalty_sv_trung_ca:
        total_objective.append(HE_SO_SV_TRUNG_CA * pen)
    
    # 0b. Phạt vi phạm CTDT-Khoa trùng ngày (CHỈ KHI relax_same_day=True)
    HE_SO_TRUNG_NGAY = 10000000
    for pen in penalty_trung_ngay:
        total_objective.append(HE_SO_TRUNG_NGAY * pen)
    
    # 0c. Phạt sinh viên thi NHIỀU MÔN CÙNG NGÀY (khác ca) - LUÔN ÁP DỤNG
    # Hệ số cao để hạn chế tối đa SV phải thi nhiều môn trong 1 ngày
    HE_SO_SV_TRUNG_NGAY = 5000000  # Cao nhưng thấp hơn trùng ca
    for pen in penalty_sv_trung_ngay:
        total_objective.append(HE_SO_SV_TRUNG_NGAY * pen)
    
    # 1. Tránh thi liền ngày (hệ số cao)
    HE_SO_LIEN_NGAY = 1000000
    for pen in penalty_lien_ngay:
        total_objective.append(HE_SO_LIEN_NGAY * pen)
        
    # 2. Ưu tiên ngày sớm (hệ số 1) - CHỈ KHI prioritize_early=True
    # Môn chung thì rải đều (đã có ràng buộc không trùng ngày giúp rải rồi)
    # Môn riêng thì ưu tiên gom về đầu nếu cần
    if prioritize_early:
        for mahp in ds_mon_to_schedule:
            so_to = phong_theo_mon[mahp]["ToThi"]
            for d in DAYS:
                for c in CA:
                    total_objective.append(z[(mahp, d, c)] * d * so_to * 1)
    
    # 3. Ưu tiên ca sớm (hệ số 0.1)
    # CHỈ KHI KHÔNG RẢI ĐỀU (nếu rải đều thì ta không muốn dồn vào ca đầu)
    if not distribute_uniformly:
        for mahp in ds_mon_to_schedule:
            for d in DAYS:
                for c in CA:
                    total_objective.append(z[(mahp, d, c)] * c * 0.1)
    
    # 4. CÂN BẰNG TẢI (RẢI ĐỀU) - CHỈ KHI distribute_uniformly=True
    if distribute_uniformly:
        print("   ⚖️ Đang áp dụng cân bằng tải (Distribute Uniformly)...")
        
        # 4.1 Cân bằng số lượng môn thi mỗi ngày (Minimize Max Exams Per Day)
        daily_counts = []
        for d in DAYS:
            # Đếm số môn thi trong ngày d
            count = sum(z[(mahp, d, c)] for mahp in ds_mon_to_schedule for c in CA)
            daily_counts.append(count)
        
        # Biến Max exams/day
        max_exams_per_day = model.NewIntVar(0, len(ds_mon_to_schedule), "max_exams_per_day")
        model.AddMaxEquality(max_exams_per_day, daily_counts)
        
        # Hàm mục tiêu: Minimize Max
        total_objective.append(max_exams_per_day * 5000)
        
        # 4.2 Cân bằng số lượng môn thi mỗi loại ca (Minimize Max Exams Per Shift ID)
        # Giúp tránh việc dồn hết vào Ca 1 của tất cả các ngày
        shift_counts = []
        for c in CA:
            count = sum(z[(mahp, d, c)] for mahp in ds_mon_to_schedule for d in DAYS)
            shift_counts.append(count)
            
        max_exams_per_shift = model.NewIntVar(0, len(ds_mon_to_schedule), "max_exams_per_shift")
        model.AddMaxEquality(max_exams_per_shift, shift_counts)
        
        total_objective.append(max_exams_per_shift * 2000)

    model.Minimize(sum(total_objective))

    # SOLVE
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = time_limit
    solver.parameters.num_search_workers = 8
    # solver.parameters.log_search_progress = True # Bật log nếu cần debug sâu
    
    status = solver.Solve(model)
    print(f"   👉 Trạng thái: {solver.StatusName(status)}")
    
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        print(f"   👉 Penalty (Objective): {solver.ObjectiveValue()}")
        result_schedule = {}
        for mahp in ds_mon_to_schedule:
            for d in DAYS:
                for c in CA:
                    if solver.Value(z[(mahp, d, c)]) == 1:
                        result_schedule[mahp] = (d, c)
                        break
        return result_schedule
    else:
        print("   ❌ Không tìm được nghiệm! (INFEASIBLE)")
        return None

# ======================
# CHIA MÔN LỚN THÀNH 2 NGÀY (ToThi > 25)
# ======================
NGUONG_CHIA_TO = 25  # Nếu ToThi > 25, chia làm 2 ngày
split_courses = {}  # {MaHP_gốc: [(MaHP_D1, ToThi_D1), (MaHP_D2, ToThi_D2)]}

# IMPORTANT: Detect I-Test courses BEFORE splitting to exclude them
# I-Test uses multi-slot scheduling, so shouldn't be split into D1/D2
itest_mahps_before_split = set()
if ITEST_ENABLED and "HinhThucThi" in df_lhp.columns:
    df_lhp_itest_check = df_lhp[df_lhp["HinhThucThi"] == 1]
    itest_mahps_before_split = set(df_lhp_itest_check["MaHP"].dropna().astype(str).str.strip().tolist())
    print(f"   🖥️ I-Test courses (excluded from split): {len(itest_mahps_before_split)}")

print("\n📊 KIỂM TRA MÔN CÓ TỔ THI LỚN (> 25):")
for mahp, info in list(phong_theo_mon.items()):  # Dùng list() để tránh lỗi khi thay đổi dict
    to_thi = info["ToThi"]
    
    # Skip I-Test courses - they use multi-slot scheduling, not D1/D2 split
    if mahp in itest_mahps_before_split:
        if to_thi > NGUONG_CHIA_TO:
            print(f"   ⏭️ {mahp}: {to_thi} tổ (I-Test - không chia)")
        continue
    
    if to_thi > NGUONG_CHIA_TO:
        # Chia làm 2
        to_d1 = to_thi // 2
        to_d2 = to_thi - to_d1  # Đảm bảo tổng đúng
        
        mahp_d1 = f"{mahp}_D1"
        mahp_d2 = f"{mahp}_D2"
        
        split_courses[mahp] = [(mahp_d1, to_d1), (mahp_d2, to_d2)]
        
        # Thêm entries mới vào phong_theo_mon
        phong_theo_mon[mahp_d1] = {"ToThi": to_d1, "PhongThi": info.get("PhongThi", "PH")}
        phong_theo_mon[mahp_d2] = {"ToThi": to_d2, "PhongThi": info.get("PhongThi", "PH")}
        
        print(f"   - {mahp}: {to_thi} tổ -> Chia thành {mahp_d1}({to_d1}) + {mahp_d2}({to_d2})")

if split_courses:
    print(f"   Tổng số môn chia: {len(split_courses)}")
else:
    print("   Không có môn nào cần chia.")

# Hàm helper để thay thế môn gốc bằng môn chia trong danh sách
def replace_split_courses(mon_list, split_courses):
    """Thay thế môn gốc bằng các môn chia (D1, D2), loại bỏ môn gốc"""
    result = []
    for m in mon_list:
        if m in split_courses:
            # Thay bằng các môn chia
            for mahp_split, _ in split_courses[m]:
                result.append(mahp_split)
        else:
            result.append(m)
    return result

# CRITICAL FIX: Update sv_to_mon with split codes
# Without this, student constraints won't apply to split courses!
# Note: ctdt_khoa_to_mon is defined inside run_solver_phase, not here
if split_courses:
    print("   Updating sv_to_mon with split course codes...")
    new_sv_to_mon = {}
    for masv, mon_list in sv_to_mon.items():
        new_sv_to_mon[masv] = replace_split_courses(mon_list, split_courses)
    sv_to_mon = new_sv_to_mon

# ======================
# CHUẨN BỊ 3 PHASE
# ======================

# 1. Đọc config Phase 2 từ file cau_hinh.xlsx
phase2_priority = [] # List[(CTDT, Khoa, SoNgay)]
try:
    df_p2 = pd.read_excel(path_cfg, sheet_name="UuTienPhase2")
    # Clean column names
    df_p2.columns = [str(c).strip() for c in df_p2.columns]
    
    # Check required columns
    # Support Vietnamese headers: "CTDT_Khoa", "SoNgay"
    col_ctdt = next((c for c in df_p2.columns if "CTDT" in c or "Khoa" in c), None)
    col_days = next((c for c in df_p2.columns if "Ngay" in c), None)
    
    if col_ctdt and col_days:
        for _, row in df_p2.iterrows():
            if pd.notna(row[col_ctdt]):
                text = str(row[col_ctdt]).strip()
                try:
                    days = int(row[col_days])
                except:
                    days = 5 # Default
                
                # Parse "CNTC-K27" -> CTDT="CNTC", Khoa="K27"
                # Giả định format: [CTDT]-[KHOA]
                parts = text.split("-")
                if len(parts) >= 2:
                    k = parts[-1].strip()
                    c = "-".join(parts[:-1]).strip()
                    phase2_priority.append((c, k, days))
    print(f"👉 Tìm thấy {len(phase2_priority)} cấu hình ưu tiên Phase 2: {phase2_priority}")
except Exception as e:
    # Nếu không có sheet hoặc lỗi, coi như không có Phase 2
    print(f"⚠️ Info: Không áp dụng Phase 2 (Lý do: {e})")

# 2. Phân loại môn

# 2a. I-Test courses (HinhThucThi = 1)
ds_mon_itest = []
if ITEST_ENABLED and "HinhThucThi" in df_lhp.columns:
    df_lhp_itest = df_lhp[df_lhp["HinhThucThi"] == 1]
    ds_mon_itest = df_lhp_itest["MaHP"].dropna().astype(str).str.strip().tolist()
    # Only keep I-Test courses that are in our exam list
    ds_mon_itest = [m for m in ds_mon_itest if m in phong_theo_mon]
    print(f"   📊 I-Test courses: {len(ds_mon_itest)} môn")
    if ds_mon_itest:
        print(f"      {ds_mon_itest[:10]}{'...' if len(ds_mon_itest) > 10 else ''}")

# 2b. Phase 1: Common courses (excluding I-Test)
ds_mon_phase1 = [m for m in list_mon_chung["MaHP"].tolist() if m not in ds_mon_itest]
ds_mon_phase2_all = []
max_days_phase2 = 5 # fallback

if phase2_priority:
    # Lấy max ngày để giới hạn chung cho Phase 2 (Simplified)
    max_days_phase2 = max(p[2] for p in phase2_priority)
    
    for c, k, _ in phase2_priority:
        # Tìm danh sách môn của CTDT-Khoa này
        row = ctdt_khoa_mon_thi[
            (ctdt_khoa_mon_thi["CTDT"] == c) & 
            (ctdt_khoa_mon_thi["Khoa"] == k)
        ]
        if not row.empty:
            mon_list_str = row.iloc[0]["DanhSachMonThi"]
            if mon_list_str:
                mon_list = [m.strip() for m in mon_list_str.split(",")]
                for m in mon_list:
                    # Chỉ lấy môn KHÔNG phải môn chung VÀ không phải I-Test
                    if m not in ds_mon_phase1 and m not in ds_mon_itest:
                        ds_mon_phase2_all.append(m)

ds_mon_phase2 = sorted(list(set(ds_mon_phase2_all)))
ds_toan_bo_mon = [m for m in df_mon["MaHP"].tolist() if m not in ds_mon_itest]

# Thay thế môn gốc bằng môn chia trong các danh sách
# NOTE: ds_mon_itest is NOT processed here - I-Test uses multi-slot scheduling, not D1/D2
if split_courses:
    ds_mon_phase1 = replace_split_courses(ds_mon_phase1, split_courses)
    ds_mon_phase2 = replace_split_courses(ds_mon_phase2, split_courses)
    ds_toan_bo_mon = replace_split_courses(ds_toan_bo_mon, split_courses)
    print(f"   Đã thay thế môn chia trong danh sách. Tổng môn mới: {len(ds_toan_bo_mon)}")

print(f"\n📊 KẾ HOẠCH XẾP LỊCH:")
if ds_mon_itest:
    print(f"   - Phase 0 (I-Test)   : {len(ds_mon_itest)} môn")
print(f"   - Phase 1 (Môn chung): {len(ds_mon_phase1)} môn")
print(f"   - Phase 2 (Ưu tiên)  : {len(ds_mon_phase2)} môn (Max {max_days_phase2} ngày)")
print(f"   - Phase 3 (Toàn bộ)  : {len(ds_toan_bo_mon)} môn")

# ======================
# RUN PHASE 0 - I-TEST (if enabled)
# ======================
schedule_itest = {}

# DEBUG: Print why Phase 0 might be skipped
print(f"\n🔍 DEBUG Phase 0 Condition Check:")
print(f"   ds_mon_itest: {len(ds_mon_itest) if ds_mon_itest else 0} môn -> {'OK' if ds_mon_itest else 'EMPTY!'}")
print(f"   NGAY_ITEST_IDX: {NGAY_ITEST_IDX if NGAY_ITEST_IDX else 'EMPTY!'}")
print(f"   Condition (ds_mon_itest and NGAY_ITEST_IDX): {bool(ds_mon_itest and NGAY_ITEST_IDX)}")

if ds_mon_itest and NGAY_ITEST_IDX:
    print(f"\n🖥️ PHASE 0 - I-TEST SCHEDULING (Direct Assignment)")
    print(f"   Môn I-Test: {len(ds_mon_itest)}")
    print(f"   Ngày cho phép: {NGAY_ITEST_IDX}")
    print(f"   Phòng I-Test: {PHONG_ITEST} ({len(PHONG_ITEST)} phòng)")
    
    # Direct scheduling for I-Test: distribute ToThi across slots
    # Each slot can hold len(PHONG_ITEST) ToThi
    rooms_per_slot = len(PHONG_ITEST) if PHONG_ITEST else 1
    
    # Calculate total ToThi needed
    itest_tothi = []
    for mahp in ds_mon_itest:
        if mahp in phong_theo_mon:
            to_count = int(phong_theo_mon[mahp]["ToThi"])
            itest_tothi.append((mahp, to_count))
    
    total_tothi = sum(t[1] for t in itest_tothi)
    slots_needed = (total_tothi + rooms_per_slot - 1) // rooms_per_slot  # Ceiling division
    slots_available = len(NGAY_ITEST_IDX) * len(CA)
    
    print(f"   📊 Total ToThi: {total_tothi}, Slots needed: {slots_needed}, Slots available: {slots_available}")
    
    if slots_needed > slots_available:
        print(f"   ⚠️ CẢNH BÁO: Không đủ slot cho I-Test! Cần {slots_needed} nhưng chỉ có {slots_available}")
    
    # Create list of available slots [(day, ca), ...]
    itest_slots = [(d, c) for d in NGAY_ITEST_IDX for c in CA]
    
    # Assign each course to slots based on how many ToThi it has
    # schedule_itest will be {mahp: [(d1, c1), (d2, c2), ...]} for multi-slot courses
    schedule_itest_multi = {}  # {mahp: [(d, c), ...]} - can have multiple slots per course
    slot_idx = 0
    current_slot_used = 0
    
    for mahp, to_count in itest_tothi:
        schedule_itest_multi[mahp] = []
        remaining_to = to_count
        
        while remaining_to > 0:
            if slot_idx >= len(itest_slots):
                print(f"   ❌ Hết slot cho môn {mahp}!")
                break
            
            current_slot = itest_slots[slot_idx]
            can_fit = min(remaining_to, rooms_per_slot - current_slot_used)
            
            if can_fit > 0:
                schedule_itest_multi[mahp].append((current_slot[0], current_slot[1], can_fit))  # (day, ca, num_to)
                remaining_to -= can_fit
                current_slot_used += can_fit
            
            # Move to next slot if current is full
            if current_slot_used >= rooms_per_slot:
                slot_idx += 1
                current_slot_used = 0
    
    # Convert to regular schedule format (for compatibility with later phases)
    # For multi-slot courses, we'll use the first slot as the "representative"
    for mahp, slots in schedule_itest_multi.items():
        if slots:
            # Use first slot as representative
            schedule_itest[mahp] = (slots[0][0], slots[0][1])
    
    print(f"   ✅ I-Test: {len(schedule_itest)} môn đã xếp lịch")
    for mahp, slots in schedule_itest_multi.items():
        print(f"      {mahp}: {[(f'D{d}C{c}x{n}') for d, c, n in slots]}")

# ======================
# RUN PHASE 1
# ======================
# Loại bỏ ngày đầu tiên (dành cho I-Test)
NGAY_PHASE_123 = [d for d in NGAY if d > 1]  # Từ ngày thứ 2 trở đi
print(f"\n📊 Phase 1-3 sẽ xếp vào các ngày: {NGAY_PHASE_123} (loại bỏ ngày đầu)")

schedule_phase1 = run_solver_phase(
    "PHASE 1 - Môn Chung", 
    ds_mon_phase1, 
    fixed_schedule=schedule_itest,  # Pass I-Test schedule as fixed
    time_limit=60,
    restricted_days=NGAY_PHASE_123,  # Loại bỏ ngày đầu
    prioritize_early=False,
    distribute_uniformly=True # Rải đều ngay từ môn chung
)

if schedule_phase1 is None:
    print("❌ Lỗi: Không thể xếp lịch Phase 1! Dừng.")
    exit(1)

# Xuất kết quả Phase 1 (Optional)
records_p1 = []
for mahp, (d, c) in schedule_phase1.items():
    records_p1.append({
        "MaHP": mahp,
        "Ngay": map_ngay[d],
        "Ca": c,
        "LoaiMon": "Chung",
        "Note": "Fixed Phase 1"
    })
pd.DataFrame(records_p1).to_excel(os.path.join(BASE_DIR, "ket_qua_phase1.xlsx"), index=False)


# ======================
# RUN PHASE 2 (Nếu có)
# ======================
schedule_phase2 = schedule_phase1.copy()
schedule_p2_result = {}

if ds_mon_phase2:
    # Loại bỏ ngày đầu, chỉ dùng các ngày 2 -> max_days_phase2+1
    restricted_days_p2 = [d for d in NGAY_PHASE_123 if d <= max_days_phase2 + 1]
    
    schedule_p2_result = run_solver_phase(
        "PHASE 2 - Môn Ưu Tiên",
        ds_mon_phase2,
        fixed_schedule=schedule_phase1,
        time_limit=60,
        restricted_days=restricted_days_p2,  # Loại bỏ ngày đầu
        prioritize_early=True,
        relax_same_day=True  # Cho phép vi phạm "cùng ngày" để đảm bảo có nghiệm
    )
    
    if schedule_p2_result:
        schedule_phase2.update(schedule_p2_result)
    else:
        print("⚠️ Cảnh báo: Phase 2 không tìm được nghiệm trong giới hạn ngày! Sẽ gộp vào Phase 3.")

# ======================
# RUN PHASE 3 - TOÀN BỘ CÒN LẠI
# ======================
# Input: Toàn bộ môn. Fixed: Phase 1 + Phase 2 (những gì đã xếp được)
final_schedule_input = schedule_phase2 if schedule_p2_result or not ds_mon_phase2 else schedule_phase1

schedule_final = run_solver_phase(
    "PHASE 3 - Toàn bộ (Rải đều)",
    ds_toan_bo_mon,
    fixed_schedule=final_schedule_input,
    time_limit=300,
    restricted_days=NGAY_PHASE_123,  # Loại bỏ ngày đầu
    relax_same_day=True,
    prioritize_early=False, # QUAN TRỌNG: Tắt ưu tiên sớm để rải đều
    distribute_uniformly=True # Bật chế độ cân bằng tải
)

if not schedule_final:
    print("❌ Lỗi: Không thể xếp lịch Phase 3 (INFEASIBLE)!")
    exit(1)

# ======================
# XỬ LÝ KẾT QUẢ CUỐNG CÙNG
# ======================

# CRITICAL: Merge schedule_itest into schedule_final
if schedule_itest:
    print(f"\n📊 Merging {len(schedule_itest)} I-Test courses into final schedule")
    schedule_final.update(schedule_itest)
    print(f"   Final schedule total: {len(schedule_final)} courses")

# 1. Thu thập kết quả theo slot để gán phòng
slot_assignments = {}  # {(ngay, ca): [(MaHP, ToThi), ...]}

# Tạo mapping đảo ngược: MaHP_D1/D2 -> MaHP gốc
split_to_original = {}
for mahp_goc, split_list in split_courses.items():
    for mahp_split, _ in split_list:
        split_to_original[mahp_split] = mahp_goc

# 1a. XỬ LÝ I-TEST RIÊNG (nếu có schedule_itest_multi)
if 'schedule_itest_multi' in dir() and schedule_itest_multi:
    print(f"\n📊 Processing I-Test ToThi assignments...")
    itest_to_offset = {}  # Track ToThi offset per course
    
    for mahp, slots in schedule_itest_multi.items():
        mahp_output = split_to_original.get(mahp, mahp)
        to_offset = itest_to_offset.get(mahp, 0)
        
        for d, c, num_to in slots:
            ngay = map_ngay[d]
            if (ngay, c) not in slot_assignments:
                slot_assignments[(ngay, c)] = []
            
            # Add each ToThi individually
            for i in range(num_to):
                to_offset += 1
                slot_assignments[(ngay, c)].append((mahp_output, mahp, to_offset))
        
        itest_to_offset[mahp] = to_offset
    
    print(f"   ✅ Added I-Test ToThi to slot_assignments")

# 1b. XỬ LÝ CÁC MÔN THƯỜNG (từ schedule_final, excluding I-Test)
ds_mon_itest_set = set(ds_mon_itest) if ds_mon_itest else set()

for mahp, (d, c) in schedule_final.items():
    # Skip I-Test courses (already handled above)
    if mahp in ds_mon_itest_set:
        continue
    
    ngay = map_ngay[d]
    if (ngay, c) not in slot_assignments:
        slot_assignments[(ngay, c)] = []
    
    # Convert MaHP_D1/D2 về MaHP gốc
    mahp_output = split_to_original.get(mahp, mahp)
    
    # Thêm từng tổ thi của môn đó
    so_to = int(phong_theo_mon[mahp]["ToThi"])
    
    # Tính offset cho các môn bị chia (D2 phải tiếp nối D1)
    start_offset = 0
    if mahp in split_to_original and mahp_output in split_courses:
        for m_split, t_split in split_courses[mahp_output]:
            if m_split == mahp:
                break
            start_offset += t_split
            
    for to in range(1, so_to + 1):
        actual_to = to + start_offset
        slot_assignments[(ngay, c)].append((mahp_output, mahp, actual_to))  # (MaHP_output, MaHP_internal, ToThi)

# 2. Gán phòng thi theo loại phòng (PH/PM/ITEST)
final_records = []

# Create set of I-Test course codes for lookup
ds_mon_itest_set = set(ds_mon_itest) if ds_mon_itest else set()

# Nhóm tổ thi theo loại phòng trong mỗi slot
for (ngay, ca), to_list in slot_assignments.items():
    # Tách theo loại phòng (dùng mahp_internal để tra cứu loại phòng)
    # I-Test courses get assigned to PHONG_ITEST
    to_list_itest = [(mahp_out, mahp_int, to) for mahp_out, mahp_int, to in to_list if mahp_int in ds_mon_itest_set]
    to_list_ph = [(mahp_out, mahp_int, to) for mahp_out, mahp_int, to in to_list if mahp_int not in ds_mon_itest_set and phong_theo_mon.get(mahp_int, {}).get("PhongThi", "PH") == "PH"]
    to_list_pm = [(mahp_out, mahp_int, to) for mahp_out, mahp_int, to in to_list if mahp_int not in ds_mon_itest_set and phong_theo_mon.get(mahp_int, {}).get("PhongThi", "PH") == "PM"]
    
    # Sắp xếp để cố định thứ tự gán
    to_list_itest.sort(key=lambda x: (x[0], x[2]))
    to_list_ph.sort(key=lambda x: (x[0], x[2]))
    to_list_pm.sort(key=lambda x: (x[0], x[2]))
    
    # Gán phòng I-Test cho môn I-Test
    # KIỂM TRA: Số tổ thi I-Test không được vượt quá số phòng I-Test
    if len(to_list_itest) > len(PHONG_ITEST) and PHONG_ITEST:
        print(f"   ⚠️ CẢNH BÁO: {len(to_list_itest)} tổ I-Test nhưng chỉ có {len(PHONG_ITEST)} phòng I-Test cho slot {ngay} ca {ca}")
    
    for idx, (mahp_out, mahp_int, to) in enumerate(to_list_itest):
        if PHONG_ITEST:
            if idx < len(PHONG_ITEST):
                phong = PHONG_ITEST[idx]  # Gán 1-1
            else:
                # Nếu hết phòng, gán vào phòng cuối cùng (với cảnh báo)
                phong = PHONG_ITEST[-1]
                print(f"      ⚠️ Tổ thi {mahp_out}-{to} phải dùng chung phòng {phong}")
        else:
            phong = PHONG_PM[idx % len(PHONG_PM)] if PHONG_PM else PHONG_KHA_DUNG[idx % len(PHONG_KHA_DUNG)]
        
        final_records.append({
            "MaHP": mahp_out,
            "ToThi": to,
            "Ngay": ngay,
            "Ca": ca,
            "PhongThi": phong
        })
    
    # Gán phòng PH cho môn PH
    for idx, (mahp_out, mahp_int, to) in enumerate(to_list_ph):
        if PHONG_PH:
            phong = PHONG_PH[idx % len(PHONG_PH)]
        else:
            phong = PHONG_KHA_DUNG[idx % len(PHONG_KHA_DUNG)]  # Fallback
        
        final_records.append({
            "MaHP": mahp_out,  # Dùng MaHP gốc (đã convert)
            "ToThi": to,
            "Ngay": ngay,
            "Ca": ca,
            "PhongThi": phong
        })
    
    # Gán phòng PM cho môn PM
    for idx, (mahp_out, mahp_int, to) in enumerate(to_list_pm):
        if PHONG_PM:
            phong = PHONG_PM[idx % len(PHONG_PM)]
        else:
            phong = PHONG_KHA_DUNG[idx % len(PHONG_KHA_DUNG)]  # Fallback
        
        final_records.append({
            "MaHP": mahp_out,  # Dùng MaHP gốc (đã convert)
            "ToThi": to,
            "Ngay": ngay,
            "Ca": ca,
            "PhongThi": phong
        })

df_kq = pd.DataFrame(final_records)

# Sắp xếp đẹp
df_kq["Ngay"] = pd.to_datetime(df_kq["Ngay"])
df_kq = df_kq.sort_values(["Ngay", "Ca", "PhongThi"])

# Thêm cột Thứ (ngày trong tuần)
THU_VIET = {
    0: "Thứ hai",
    1: "Thứ ba", 
    2: "Thứ tư",
    3: "Thứ năm",
    4: "Thứ sáu",
    5: "Thứ bảy",
    6: "Chủ nhật"
}
df_kq["Thu"] = df_kq["Ngay"].dt.dayofweek.map(THU_VIET)

# Format ngày sau khi tính Thứ
df_kq["Ngay"] = df_kq["Ngay"].dt.strftime('%d/%m/%Y')

# Sắp xếp lại cột (đưa Thứ sau Ngay)
cols = df_kq.columns.tolist()
if "Thu" in cols and "Ngay" in cols:
    cols.remove("Thu")
    ngay_idx = cols.index("Ngay")
    cols.insert(ngay_idx + 1, "Thu")
    df_kq = df_kq[cols]

# Xuất file kết quả chính (MaHP, ToThi, Ngay, Ca, PhongThi)
output_path = os.path.join(BASE_DIR, "ket_qua_xep_lich_thi.xlsx")
df_kq.to_excel(output_path, index=False)
print(f"✅ Đã xuất file lịch thi: {output_path}")
print(f"   Tổng số tổ thi: {len(df_kq)}")
# ======================
# XUẤT FILE DANH SÁCH SINH VIÊN (MỚI)
# ======================
print("\n===== XUẤT DANH SÁCH SINH VIÊN THI =====")

# df_sv_to_thi: MaSV, Ten, MaHP, ToThi
# df_kq: MaHP, ToThi, Ngay, Ca, PhongThi

# Merge lịch thi vào danh sách sinh viên
df_final_sv = pd.merge(
    df_sv_to_thi,
    df_kq,
    on=["MaHP", "ToThi"],
    how="left"
)

# Thêm thông tin chi tiết môn học từ df_lhp (nếu có)
# Kiểm tra các cột có sẵn trong df_lhp
available_lhp_cols = ["MaHP"]
if "TenMH" in df_lhp.columns:
    available_lhp_cols.append("TenMH")
elif "Ten_MH" in df_lhp.columns:
    available_lhp_cols.append("Ten_MH")
    
if "SoTC" in df_lhp.columns:
    available_lhp_cols.append("SoTC")
    
if "Lop" in df_lhp.columns:
    available_lhp_cols.append("Lop")

if len(available_lhp_cols) > 1:
    df_lhp_info = df_lhp[available_lhp_cols].drop_duplicates("MaHP")
    df_final_sv = pd.merge(
        df_final_sv,
        df_lhp_info,
        on="MaHP",
        how="left"
    )
else:
    # Không có thông tin bổ sung từ df_lhp
    print("   ⚠️ File LHP không có cột TenMH/SoTC, bỏ qua merge.")

# Tạo cột Giờ thi từ Ca
# Giả sử: Ca 1 (07:00), Ca 2 (09:30), Ca 3 (13:00), Ca 4 (15:30)
CA_TO_GIO = {
    1: "07:00",
    2: "09:30",
    3: "13:00",
    4: "15:30"
}
df_final_sv["GioThi"] = df_final_sv["Ca"].map(CA_TO_GIO)

# Tách Họ và Tên
def tach_ho_ten(full_name):
    if pd.isna(full_name):
        return "", ""
    parts = str(full_name).strip().split()
    if len(parts) == 0:
        return "", ""
    elif len(parts) == 1:
        return "", parts[0]
    else:
        return " ".join(parts[:-1]), parts[-1]

df_final_sv["HoDem"] = df_final_sv["Ten"].apply(lambda x: tach_ho_ten(x)[0])
df_final_sv["TenSV"] = df_final_sv["Ten"].apply(lambda x: tach_ho_ten(x)[1])

# Rename cột theo mẫu yêu cầu
rename_dict = {
    "Ten_MH": "Tên môn",
    "SoTC": "Số TC",
    "MaSV": "Mã SV",
    # "Ten": "Họ tên", # Tên gốc là fullname
    "Ngay": "Ngày thi",
    "GioThi": "Giờ thi",
    "PhongThi": "Phòng thi",
    "ToThi": "Tổ thi"
}
if "Lop" in df_final_sv.columns:
    rename_dict["Lop"] = "Lớp"

df_final_sv = df_final_sv.rename(columns=rename_dict)

# Thêm các cột còn thiếu
df_final_sv["Đợt thi"] = "Đợt 1"
df_final_sv["Nhóm thi"] = "1"
df_final_sv["Ghi chú"] = ""
df_final_sv["Mã HP"] = df_final_sv["MaHP"] # Duplicate cột này nếu cần cột MaHP riêng

# Chọn thứ tự cột
output_cols = [
    "Mã HP", "Tên môn", "Số TC", "Đợt thi", "Nhóm thi", "Tổ thi", 
    "Ngày thi", "Giờ thi", "Phòng thi", "Mã SV", "HoDem", "TenSV", "Lớp", "Ghi chú"
]

# Đổi tên cột hiển thị cho đẹp
# HoDem -> Họ đệm, TenSV -> Tên
final_rename = {
    "HoDem": "Họ đệm",
    "TenSV": "Tên"
}
df_final_sv = df_final_sv.rename(columns=final_rename)

# Cập nhật lại list cột cần lấy
output_cols = [
    "Mã HP", "Tên môn", "Số TC", "Đợt thi", "Nhóm thi", "Tổ thi", 
    "Ngày thi", "Giờ thi", "Phòng thi", "Mã SV", "Họ đệm", "Tên", "Lớp", "Ghi chú"
]

# Đảm bảo các cột tồn tại (nếu ko có Lớp thì bỏ qua)
existing_cols = [c for c in output_cols if c in df_final_sv.columns]
df_final_sv = df_final_sv[existing_cols]

# CRITICAL: Loại bỏ duplicate cuối cùng (nếu có)
before_dedup = len(df_final_sv)
df_final_sv = df_final_sv.drop_duplicates()
if len(df_final_sv) < before_dedup:
    print(f"   ⚠️ Đã loại bỏ {before_dedup - len(df_final_sv)} dòng trùng lặp trong output")

output_sv_path = os.path.join(BASE_DIR, "BangTongHopLichThiSinhVien_KetQua.xlsx")
df_final_sv.to_excel(output_sv_path, index=False)

print(f"✅ Đã xuất file danh sách sinh viên: {output_sv_path}")
print(f"   Tổng số dòng: {len(df_final_sv)}")