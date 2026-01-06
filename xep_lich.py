import pandas as pd
from ortools.sat.python import cp_model

model = cp_model.CpModel()

from collections import defaultdict

# ======================
# 1. ĐỌC FILE
# ======================
path_lhp = "D:\python\XepLichThi\danhsachLHP.xlsx"
path_data = "D:\python\XepLichThi\Data.xlsx"
path_cfg = "D:\python\XepLichThi\cau_hinh.xlsx"
path_sv = "D:/python/XepLichThi/danhsachSV.xlsx"

df_lhp = pd.read_excel(path_lhp)
df_data = pd.read_excel(path_data)
df_cfg = pd.read_excel(path_cfg)

df_sv = pd.read_excel(path_sv)

# Chuẩn hóa
df_sv["MaSV"] = df_sv["MaSV"].astype(str).str.strip()
df_sv["Ten"] = df_sv["Ten"].astype(str).str.strip()
df_sv["MaHP"] = df_sv["MaHP"].astype(str).str.strip()

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

map_to_sv = (
    df_data_thi
    .groupby(["MaHP", "ToThi"])["MaSV"]
    .apply(list)
    .to_dict()
)

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

for mahp, df_mhp in df_sv.groupby("MaHP"):
    # Chỉ xử lý môn có trong danh sách thi
    if mahp not in phong_theo_mon:
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

# ----------------------
# 4.1. TẬP TỔ THI
# ----------------------

# Ví dụ ToThi trong df_lhp: 1,2,3...
to_thi_list = []

TO_THI = list(map_to_sv.keys())

# ----------------------
# 4.2. NGÀY - CA - PHÒNG
# ----------------------

NGAY = ngay_thi
CA = ca_thi

PHONG = (
    df_phongthi["PhongThi"]
    .dropna()
    .astype(str)
    .tolist()
)

# sức chứa phòng
SUC_CHUA_PHONG = dict(
    zip(df_phongthi["PhongThi"], df_phongthi["SucChua"])
)

# ----------------------
# 4.3. SỐ SV TRONG MỖI TỔ
# ----------------------

# dict: (MaHP, ToThi) -> số SV
SV_TRONG_TO = {
    k: len(v)
    for k, v in map_to_sv.items()
}
# ----------------------
# 4.4. BIẾN QUYẾT ĐỊNH
# ----------------------

x = {}

for to in TO_THI:
    for d in NGAY:
        for c in CA:
            for p in PHONG:
                x[to, d, c, p] = model.NewBoolVar(
                    f"x_{to[0]}_T{to[1]}_{d}_{c}_{p}"
                )

# ----------------------
# C1. MỖI TỔ = 1 SLOT
# ----------------------

for to in TO_THI:
    model.Add(
        sum(
            x[to, d, c, p]
            for d in NGAY
            for c in CA
            for p in PHONG
        ) == 1
    )

# ----------------------
# C2. PHÒNG - CA - NGÀY
# ----------------------

for d in NGAY:
    for c in CA:
        for p in PHONG:
            model.Add(
                sum(
                    x[to, d, c, p]
                    for to in TO_THI
                ) <= 1
            )

# ----------------------
# C3. TỔ CÙNG MÔN
# ----------------------

from collections import defaultdict

to_by_mahp = defaultdict(list)
for (mahp, to) in TO_THI:
    to_by_mahp[mahp].append((mahp, to))

for mahp, to_list in to_by_mahp.items():
    for d in NGAY:
        for c in CA:
            model.Add(
                sum(
                    x[to, d, c, p]
                    for to in to_list
                    for p in PHONG
                ) <= 1
            )

# ----------------------
# C4. SV KHÔNG TRÙNG LỊCH
# ----------------------

for masv, mon_list in sv_to_mon.items():
    to_list = []
    for mahp in mon_list:
        to_list.extend(to_by_mahp.get(mahp, []))

    for d in NGAY:
        for c in CA:
            model.Add(
                sum(
                    x[to, d, c, p]
                    for to in to_list
                    for p in PHONG
                ) <= 1
            )

# ----------------------
# C5. SỨC CHỨA PHÒNG
# ----------------------

for to in TO_THI:
    so_sv = SV_TRONG_TO.get(to, 0)
    for d in NGAY:
        for c in CA:
            for p in PHONG:
                if so_sv > SUC_CHUA_PHONG[p]:
                    model.Add(x[to, d, c, p] == 0)

# ----------------------
# C8. ƯU TIÊN K25 & K9
# ----------------------

SV_UU_TIEN = (
    df_sv
    .loc[df_sv["Khoa"].isin(["K25", "K9"]), "MaSV"]
    .drop_duplicates()
    .tolist()
)

NGAY_UU_TIEN = NGAY[:16]  # ví dụ 14 + 2 ngày buffer

MON_UU_TIEN = set()

for sv in SV_UU_TIEN:
    for mahp in sv_to_mon.get(sv, []):
        MON_UU_TIEN.add(mahp)

SO_NGAY_2_TUAN = 12
SO_NGAY_BUFFER = 3

NGAY_UU_TIEN = NGAY[:SO_NGAY_2_TUAN + SO_NGAY_BUFFER]

for mahp in MON_UU_TIEN:
    for to in to_by_mahp.get(mahp, []):
        model.Add(
            sum(
                x[to, d, c, p]
                for d in NGAY_UU_TIEN
                for c in CA
                for p in PHONG
            ) == 1
        )


# ----------------------
# 4.6. TỐI ƯU MỀM
# ----------------------

penalties = []

for masv, mon_list in sv_to_mon.items():
    to_list = []
    for mahp in mon_list:
        to_list += to_by_mahp.get(mahp, [])

    for d in NGAY:
        y = model.NewIntVar(0, len(to_list), f"y_{masv}_{d}")
        model.Add(
            y == sum(
                x[to, d, c, p]
                for to in to_list
                for c in CA
                for p in PHONG
            )
        )

        # phạt nếu >1 môn/ngày
        excess = model.NewIntVar(0, len(to_list), f"excess_{masv}_{d}")
        model.Add(excess >= y - 1)
        penalties.append(excess)

# ----------------------
# 4.7. HÀM MỤC TIÊU
# ----------------------
model.Minimize(sum(penalties))

# ----------------------
# 4.8. GIẢI BÀI TOÁN
# ----------------------

solver = cp_model.CpSolver()
solver.parameters.max_time_in_seconds = 60
solver.parameters.num_search_workers = 8

status = solver.Solve(model)

print("STATUS:", solver.StatusName(status))

# ----------------------
# 4.9. In kết quả
# ----------------------

if status in [cp_model.OPTIMAL, cp_model.FEASIBLE]:
    for to in TO_THI:
        for d in NGAY:
            for c in CA:
                for p in PHONG:
                    if solver.Value(x[to, d, c, p]) == 1:
                        print(
                            f"MaHP={to[0]} | Tổ={to[1]} | "
                            f"Ngày={d} | Ca={c} | Phòng={p}"
                        )
else:
    print("❌ KHÔNG CÓ LỜI GIẢI – CHECK RÀNG BUỘC")