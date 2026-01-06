import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "Data.xlsx")
CFG_PATH = os.path.join(BASE_DIR, "cau_hinh.xlsx")
LHP_PATH = os.path.join(BASE_DIR, "danhsachLHP.xlsx")

def validate():
    print("=== VALIDATION CHECK ===")
    
    # 1. Đọc số ngày thi
    if not os.path.exists(CFG_PATH):
        print(f"❌ Không tìm thấy file {CFG_PATH}")
        return
        
    df_thoigian = pd.read_excel(CFG_PATH, sheet_name="ThoiGianThi")
    ngay_thi = df_thoigian[df_thoigian["SuDung"] == 1]["NgayThi"].unique()
    so_ngay = len(ngay_thi)
    print(f"📅 Số ngày thi khả dụng: {so_ngay}")
    
    # 2. Đọc danh sách môn cần thi
    if not os.path.exists(DATA_PATH):
        print(f"❌ Không tìm thấy file {DATA_PATH}")
        return
        
    df_data = pd.read_excel(DATA_PATH)
    
    # Lấy học kỳ hiện tại (giả sử dữ liệu năm/kỳ đầu tiên trong file config là đúng)
    df_hk = pd.read_excel(CFG_PATH, sheet_name="HK")
    nam_th = int(df_hk.loc[0, "NamTH"])
    hk_th = int(df_hk.loc[0, "HKTH"])
    
    df_data_thi = df_data[
        (df_data["NamTH"] == nam_th) & 
        (df_data["HKTH"] == hk_th)
    ]
    
    # 3. Đọc danh sách LHP để lọc môn thực tế có mở lớp
    df_lhp = pd.read_excel(LHP_PATH)
    ds_mahp_thuc_te = set(df_lhp["MaHP"].unique())
    
    # 4. Kiểm tra từng CTĐT-Khóa
    # Group by CTDT, Khoa -> count unique MaHP
    # Chỉ tính những môn có trong ds_mahp_thuc_te
    df_data_thi = df_data_thi[df_data_thi["MaHP"].isin(ds_mahp_thuc_te)]
    
    mon_per_khoa = df_data_thi.groupby(["CTDT", "Khoa"])["MaHP"].nunique()
    
    violation_found = False
    print("\n🔍 Kiểm tra số môn thi của từng CTĐT-Khóa:")
    for (ctdt, khoa), so_mon in mon_per_khoa.items():
        if so_mon > so_ngay:
            print(f"   ❌ [VI PHẠM CỨNG] {ctdt}-{khoa}: {so_mon} môn > {so_ngay} ngày")
            violation_found = True
        elif so_mon == so_ngay:
             print(f"   ⚠️ [Rủi ro cao] {ctdt}-{khoa}: {so_mon} môn = {so_ngay} ngày (Khó xếp)")
    
    if not violation_found:
        print("\n✅ Không có CTĐT-Khóa nào có số môn > số ngày thi.")
        print("   -> Bài toán có thể giải được về mặt lý thuyết.")
    else:
        print("\n❌ CÓ VI PHẠM CỨNG! Không thể xếp lịch nếu giữ ràng buộc 'Không thi cùng ngày'.")
        print("   -> Cần tăng số ngày thi hoặc cho phép thi cùng ngày.")

if __name__ == "__main__":
    validate()
