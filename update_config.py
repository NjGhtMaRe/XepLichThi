import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(BASE_DIR, 'cau_hinh.xlsx')

data = {
    "CTDT_Khoa": ["CNTC-K27", "TMDT-K12"],
    "SoNgayThi": [5, 5],
    "GhiChu": ["Ví dụ Test", "Test môn riêng"]
}

df_new = pd.DataFrame(data)

print(f"Updating {config_path}...")

if os.path.exists(config_path):
    try:
        # Pandas 1.3+ support if_sheet_exists='replace'
        with pd.ExcelWriter(config_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            df_new.to_excel(writer, sheet_name='UuTienPhase2', index=False)
        print("Đã cập nhật sheet UuTienPhase2 thành công.")
    except TypeError:
        # Fallback for older pandas: Load all, replace, save
        print("Falling back to manual replacement...")
        dfs = pd.read_excel(config_path, sheet_name=None)
        dfs['UuTienPhase2'] = df_new
        with pd.ExcelWriter(config_path, engine='openpyxl') as writer:
            for sheet, df in dfs.items():
                df.to_excel(writer, sheet_name=sheet, index=False)
        print("Đã cập nhật sheet UuTienPhase2 (Manual).")
    except Exception as e:
        print(f"Lỗi: {e}")
else:
    print("File chưa tồn tại. Tạo mới.")
    with pd.ExcelWriter(config_path, engine='xlsxwriter') as writer:
        df_new.to_excel(writer, sheet_name='UuTienPhase2', index=False)
