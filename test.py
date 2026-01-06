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
print(f"\n📊 Phòng thi: {len(PHONG_PH)} phòng PH, {len(PHONG_PM)} phòng PM")

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
    prioritize_early=True  # Nếu True: Ưu tiên xếp vào các ngày đầu
):
    """
    Hàm chạy solver cho một tập các môn.
    - restricted_days: Chỉ xếp môn vào các ngày trong list này (cho Phase 2)
    - prioritize_early: Có ưu tiên xếp sớm hay không (False cho Phase 3 để rải đều)
    """
    print(f"\n🚀 Đang chạy {phase_name}...")
    print(f"   - Số môn cần xếp: {len(ds_mon_to_schedule)}")
    if fixed_schedule:
        print(f"   - Số môn đã cố định: {len(fixed_schedule)}")
    if restricted_days:
        print(f"   - Giới hạn xếp trong {len(restricted_days)} ngày đầu: {restricted_days}")
    
    # DEBUG: Tính capacity
    MAX_TO_PER_CA = len(PHONG_KHA_DUNG)
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
    # Nếu relax_same_day=True -> Soft Constraint (penalty cho vi phạm)
    # Nếu relax_same_day=False -> Hard Constraint (cấm tuyệt đối)
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
                    # SOFT CONSTRAINT
                    vi_pham_sv = model.NewIntVar(0, len(mon_list_filtered), f"vpsv_{masv}_{d}_{c}")
                    model.Add(vi_pham_sv >= sum_sv - 1)
                    penalty_sv_trung_ca.append(vi_pham_sv)
                else:
                    # HARD CONSTRAINT
                    model.Add(sum_sv <= 1)

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
    
    # 0b. Phạt vi phạm trùng ngày (CHỈ KHI relax_same_day=True)
    # Hệ số cực lớn để hạn chế tối đa
    HE_SO_TRUNG_NGAY = 10000000
    for pen in penalty_trung_ngay:
        total_objective.append(HE_SO_TRUNG_NGAY * pen)
    
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
    for mahp in ds_mon_to_schedule:
        for d in DAYS:
            for c in CA:
                total_objective.append(z[(mahp, d, c)] * c * 0.1)

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

print("\n📊 KIỂM TRA MÔN CÓ TỔ THI LỚN (> 25):")
for mahp, info in list(phong_theo_mon.items()):  # Dùng list() để tránh lỗi khi thay đổi dict
    to_thi = info["ToThi"]
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
ds_mon_phase1 = list_mon_chung["MaHP"].tolist()
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
                    # Chỉ lấy môn KHÔNG phải môn chung
                    if m not in ds_mon_phase1:
                        ds_mon_phase2_all.append(m)

ds_mon_phase2 = sorted(list(set(ds_mon_phase2_all)))
ds_toan_bo_mon = df_mon["MaHP"].tolist()

# Thay thế môn gốc bằng môn chia trong các danh sách
if split_courses:
    ds_mon_phase1 = replace_split_courses(ds_mon_phase1, split_courses)
    ds_mon_phase2 = replace_split_courses(ds_mon_phase2, split_courses)
    ds_toan_bo_mon = replace_split_courses(ds_toan_bo_mon, split_courses)
    print(f"   Đã thay thế môn chia trong danh sách. Tổng môn mới: {len(ds_toan_bo_mon)}")

print(f"\n📊 KẾ HOẠCH XẾP LỊCH:")
print(f"   - Phase 1 (Môn chung): {len(ds_mon_phase1)} môn")
print(f"   - Phase 2 (Ưu tiên)  : {len(ds_mon_phase2)} môn (Max {max_days_phase2} ngày)")
print(f"   - Phase 3 (Toàn bộ)  : {len(ds_toan_bo_mon)} môn")

# ======================
# RUN PHASE 1
# ======================
schedule_phase1 = run_solver_phase(
    "PHASE 1 - Môn Chung", 
    ds_mon_phase1, 
    fixed_schedule=None, 
    time_limit=60,
    prioritize_early=True
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
    restricted_days = DAYS[:max_days_phase2]
    
    schedule_p2_result = run_solver_phase(
        "PHASE 2 - Môn Ưu Tiên",
        ds_mon_phase2,
        fixed_schedule=schedule_phase1,
        time_limit=60,
        restricted_days=restricted_days,
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
    relax_same_day=True,
    prioritize_early=False # QUAN TRỌNG: Tắt ưu tiên sớm để rải đều
)

if not schedule_final:
    print("❌ Lỗi: Không thể xếp lịch Phase 3 (INFEASIBLE)!")
    exit(1)

# ======================
# XỬ LÝ KẾT QUẢ CUỐNG CÙNG
# ======================

# 1. Thu thập kết quả theo slot để gán phòng
slot_assignments = {}  # {(ngay, ca): [(MaHP, ToThi), ...]}

# Tạo mapping đảo ngược: MaHP_D1/D2 -> MaHP gốc
split_to_original = {}
for mahp_goc, split_list in split_courses.items():
    for mahp_split, _ in split_list:
        split_to_original[mahp_split] = mahp_goc

for mahp, (d, c) in schedule_final.items():
    ngay = map_ngay[d]
    if (ngay, c) not in slot_assignments:
        slot_assignments[(ngay, c)] = []
    
    # Convert MaHP_D1/D2 về MaHP gốc
    mahp_output = split_to_original.get(mahp, mahp)
    
    # Thêm từng tổ thi của môn đó
    so_to = int(phong_theo_mon[mahp]["ToThi"])
    for to in range(1, so_to + 1):
        slot_assignments[(ngay, c)].append((mahp_output, mahp, to))  # (MaHP_output, MaHP_internal, ToThi)

# 2. Gán phòng thi theo loại phòng (PH/PM)
final_records = []

# Nhóm tổ thi theo loại phòng trong mỗi slot
for (ngay, ca), to_list in slot_assignments.items():
    # Tách theo loại phòng (dùng mahp_internal để tra cứu loại phòng)
    to_list_ph = [(mahp_out, mahp_int, to) for mahp_out, mahp_int, to in to_list if phong_theo_mon.get(mahp_int, {}).get("PhongThi", "PH") == "PH"]
    to_list_pm = [(mahp_out, mahp_int, to) for mahp_out, mahp_int, to in to_list if phong_theo_mon.get(mahp_int, {}).get("PhongThi", "PH") == "PM"]
    
    # Sắp xếp để cố định thứ tự gán
    to_list_ph.sort(key=lambda x: (x[0], x[2]))
    to_list_pm.sort(key=lambda x: (x[0], x[2]))
    
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
df_kq["Ngay"] = df_kq["Ngay"].dt.strftime('%d/%m/%Y')

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

output_sv_path = os.path.join(BASE_DIR, "BangTongHopLichThiSinhVien_KetQua.xlsx")
df_final_sv.to_excel(output_sv_path, index=False)

print(f"✅ Đã xuất file danh sách sinh viên: {output_sv_path}")
print(f"   Tổng số dòng: {len(df_final_sv)}")