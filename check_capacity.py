import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Load files
df_lhp = pd.read_excel(os.path.join(BASE_DIR, "danhsachLHP.xlsx"))
df_cfg = pd.read_excel(os.path.join(BASE_DIR, "cau_hinh.xlsx"), sheet_name="PhongThi")
df_time = pd.read_excel(os.path.join(BASE_DIR, "cau_hinh.xlsx"), sheet_name="ThoiGianThi")
df_ca = pd.read_excel(os.path.join(BASE_DIR, "cau_hinh.xlsx"), sheet_name="CaThi")

# Calculate
so_phong = len(df_cfg)
so_ngay = len(df_time[df_time["SuDung"] == 1])
so_ca = len(df_ca)
tong_slot = so_phong * so_ngay * so_ca
tong_to_thi = df_lhp["ToThi"].sum()

print("=== PHÂN TÍCH SỨC CHỨA ===")
print(f"Số phòng thi: {so_phong}")
print(f"Số ngày thi: {so_ngay}")
print(f"Số ca/ngày: {so_ca}")
print(f"Tổng slot khả dụng: {tong_slot}")
print(f"Tổng tổ thi cần xếp: {tong_to_thi}")
print()

if tong_to_thi > tong_slot:
    print(f"❌ THIẾU SLOT: Cần {tong_to_thi} slot nhưng chỉ có {tong_slot}")
    print(f"   -> Cần thêm {tong_to_thi - tong_slot} slot (thêm ngày/ca/phòng)")
else:
    print(f"✅ ĐỦ SLOT: Cần {tong_to_thi}/{tong_slot} ({100*tong_to_thi/tong_slot:.1f}%)")
    print("   -> Vấn đề không phải do thiếu slot tổng thể.")
    print("   -> Có thể do xung đột cục bộ (sinh viên trùng ca).")
