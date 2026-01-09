"""
Exam Scheduler Module - Module xếp lịch thi sử dụng OR-Tools CP-SAT Solver
"""

import pandas as pd
from ortools.sat.python import cp_model
from collections import defaultdict
import os
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field


@dataclass
class SchedulerConfig:
    """Cấu hình cho solver"""
    max_to_per_ca: int = 68  # Sẽ được tự động cập nhật theo số phòng
    sv_khong_trung_ca: bool = True
    ctdt_khong_trung_ngay: bool = True  # Ràng buộc cứng: không thi cùng ngày
    ctdt_khong_lien_ngay: bool = True   # Ràng buộc mềm: penalty nếu thi liền ngày
    he_so_penalty_lien_ngay: int = 1000000 
    solver_timeout: int = 300 
    num_workers: int = 8
    distribute_uniformly: bool = True # Mặc định bật load balancing



@dataclass
class SchedulerResult:
    """Kết quả từ solver"""
    status: str
    records: List[dict] = field(default_factory=list)
    records_sv: List[dict] = field(default_factory=list)  # Danh sách SV thi
    stats: dict = field(default_factory=dict)
    violations: List[dict] = field(default_factory=list)  # Vi phạm liền ngày
    error: Optional[str] = None


class ExamScheduler:
    """Class xử lý xếp lịch thi"""
    
    def __init__(self, config: SchedulerConfig = None):
        self.config = config or SchedulerConfig()
        self.data_loaded = False
        
        # DataFrames
        self.df_lhp = None
        self.df_data = None
        self.df_sv = None
        self.df_cfg = None
        self.df_data_thi = None
        self.df_sv_to_thi = None
        
        # Processed data
        self.ds_mahp_thi = None
        self.ngay_thi = []
        self.ca_thi = []
        self.phong_theo_mon = {}
        self.phong_kha_dung = []
        self.suc_chua_phong = {}
        self.sv_to_mon = {}
        self.map_ngay = {}
        self.ds_mahp_set = set()
        self.ctdt_khoa_to_mon = {}
        self.priority_phase2_config = [] # List[(CTDT, Khoa, SoNgay)]
        self.split_courses = {} # {MaHP_gốc: [(MaHP_D1, ToThi_D1), ...]}
                
        # Mapping Ca -> Giờ thi
        self.CA_TO_GIO = {
            1: "07:00",
            2: "09:15",
            3: "13:00",
            4: "15:15"
        }
        
    def load_data(self, path_lhp: str, path_data: str, path_cfg: str, path_sv: str) -> dict:
        """Đọc và xử lý dữ liệu từ các file Excel"""
        try:
            # Đọc files
            self.df_lhp = pd.read_excel(path_lhp)
            self.df_data = pd.read_excel(path_data)
            self.df_cfg = pd.read_excel(path_cfg)
            self.df_sv = pd.read_excel(path_sv)
            
            # Chuẩn hóa dữ liệu SV
            self.df_sv["MaSV"] = self.df_sv["MaSV"].astype(str).str.strip()
            self.df_sv["Ten"] = self.df_sv["Ten"].astype(str).str.strip()
            self.df_sv["MaHP"] = self.df_sv["MaHP"].astype(str).str.strip()
            
            # Chuẩn hóa dữ liệu LHP để khớp với SV
            self.df_lhp["MaHP"] = self.df_lhp["MaHP"].astype(str).str.strip()
            
            # Đọc các sheet cấu hình
            df_hk = pd.read_excel(path_cfg, sheet_name="HK")
            df_hk.columns = df_hk.columns.str.strip()
            
            df_thoigianthi = pd.read_excel(path_cfg, sheet_name="ThoiGianThi")
            df_thoigianthi.columns = df_thoigianthi.columns.str.strip()
            
            df_ca_thi = pd.read_excel(path_cfg, sheet_name="CaThi")
            df_ca_thi.columns = df_ca_thi.columns.str.strip()
            
            # Đọc danh sách phòng thi
            df_phongthi = pd.read_excel(path_cfg, sheet_name="PhongThi")
            df_phongthi.columns = df_phongthi.columns.str.strip()
            
            self.phong_kha_dung = (
                df_phongthi["PhongThi"]
                .dropna()
                .astype(str)
                .str.strip()
                .tolist()
            )

            # Đọc Cấu hình Phase 2 (Optional)
            try:
                xl = pd.ExcelFile(path_cfg)
                if "UuTienPhase2" in xl.sheet_names:
                    df_p2 = pd.read_excel(path_cfg, sheet_name="UuTienPhase2")
                    df_p2.columns = [str(c).strip() for c in df_p2.columns]
                    
                    col_ctdt = next((c for c in df_p2.columns if "CTDT" in c or "Khoa" in c), None)
                    col_days = next((c for c in df_p2.columns if "Ngay" in c), None)
                    
                    if col_ctdt and col_days:
                        for _, row in df_p2.iterrows():
                            if pd.notna(row[col_ctdt]):
                                text = str(row[col_ctdt]).strip()
                                try:
                                    days = int(row[col_days])
                                except:
                                    days = 5
                                
                                parts = text.split("-")
                                if len(parts) >= 2:
                                    k = parts[-1].strip()
                                    c = "-".join(parts[:-1]).strip()
                                    self.priority_phase2_config.append((c, k, days))
            except Exception as e:
                print(f"Warning load Core Config P2: {e}")

            
            self.suc_chua_phong = dict(
                zip(
                    df_phongthi["PhongThi"].astype(str).str.strip(),
                    df_phongthi["SucChua"]
                )
            )
            
            # Cập nhật max_to_per_ca theo số phòng khả dụng
            self.config.max_to_per_ca = len(self.phong_kha_dung)
            
            # Lấy năm và học kỳ
            nam_th = int(df_hk.loc[0, "NamTH"])
            hk_th = int(df_hk.loc[0, "HKTH"])
            
            # Lọc data theo năm + học kỳ
            self.df_data_thi = self.df_data[
                (self.df_data["NamTH"] == nam_th) &
                (self.df_data["HKTH"] == hk_th)
            ]
            
            # Danh sách môn thi
            self.ds_mahp_thi = self.df_lhp["MaHP"].drop_duplicates()
            self.ds_mahp_set = set(self.ds_mahp_thi)
            
            df_data_thi_mon = self.df_data_thi[
                self.df_data_thi["MaHP"].isin(self.ds_mahp_thi)
            ].copy()
            
            # Ngày thi
            self.ngay_thi = (
                df_thoigianthi
                .query("SuDung == 1")["NgayThi"]
                .sort_values()
                .tolist()
            )
            NGAY = list(range(1, len(self.ngay_thi) + 1))
            self.map_ngay = dict(zip(NGAY, self.ngay_thi))
            
            # Ca thi
            self.ca_thi = df_ca_thi["Ca"].sort_values().tolist()
            
            # Thông tin phòng theo môn
            self.phong_theo_mon = (
                self.df_lhp
                .set_index("MaHP")[["ToThi", "PhongThi"]]
                .to_dict("index")
            )
            
            # Rải sinh viên vào tổ thi
            # Remove duplicates first
            original_count = len(self.df_sv)
            self.df_sv = self.df_sv.drop_duplicates(subset=["MaSV", "MaHP"], keep="first")
            if len(self.df_sv) < original_count:
                print(f"   [WARNING] Removed {original_count - len(self.df_sv)} duplicate (MaSV, MaHP) entries")
            
            ds_sv_to_thi = []
            for mahp, df_mhp in self.df_sv.groupby("MaHP"):
                if mahp not in self.phong_theo_mon:
                    continue
                    
                so_to = int(self.phong_theo_mon[mahp]["ToThi"])
                df_mhp_sorted = df_mhp.sort_values("Ten").reset_index(drop=True)
                
                N = len(df_mhp_sorted)
                if N == 0:
                    continue
                    
                base = N // so_to
                du = N % so_to
                start_idx = 0
                
                for to in range(1, so_to + 1):
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
            
            self.df_sv_to_thi = pd.DataFrame(ds_sv_to_thi)
            
            # SV -> danh sách môn thi
            self.sv_to_mon = (
                self.df_sv
                .groupby("MaSV")["MaHP"]
                .apply(lambda x: sorted(x.unique()))
                .to_dict()
            )
            
            # CTĐT/Khóa -> danh sách môn
            self.ctdt_khoa_to_mon = (
                df_data_thi_mon
                .groupby(["CTDT", "Khoa"])["MaHP"]
                .apply(list)
                .to_dict()
            )
            
            self.data_loaded = True
            
            return {
                "success": True,
                "stats": {
                    "so_mon_thi": len(self.ds_mahp_thi),
                    "so_ngay_thi": len(self.ngay_thi),
                    "so_ca_thi": len(self.ca_thi),
                    "so_phong_thi": len(self.phong_kha_dung),
                    "so_sinh_vien": len(self.sv_to_mon),
                    "so_to_thi": len(self.df_sv_to_thi["ToThi"].unique()) if len(self.df_sv_to_thi) > 0 else 0,
                    "so_ctdt_khoa": len(self.ctdt_khoa_to_mon)
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def _run_solver_phase(self, 
                          phase_name: str, 
                          ds_mon_to_schedule: list, 
                          fixed_schedule: dict = None, 
                          time_limit: int = 60,
                          restricted_days: list = None,
                          prioritize_early: bool = True,
                          relax_same_day: bool = False,
                          distribute_uniformly: bool = False):
        """Helper chạy solver cho một phase"""
        print(f" [Scheduler] Starting {phase_name}...")
        print(f"   Courses to schedule: {len(ds_mon_to_schedule)}")
        print(f"   relax_same_day: {relax_same_day}")
        print(f"   distribute_uniformly: {distribute_uniformly}")
        
        model = cp_model.CpModel()
        
        # Lấy DAYS và CA từ self
        DAYS = list(range(1, len(self.ngay_thi) + 1)) # DAYS index 1..N
        CA = self.ca_thi
        
        # Biến quyết định
        z = {}
        for mahp in ds_mon_to_schedule:
            for d in DAYS:
                for c in CA:
                    z[(mahp, d, c)] = model.NewBoolVar(f"z_{mahp}_{d}_{c}")
                    
        # 0. Ràng buộc Restricted Days (Phase 2)
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
        
        # 1. Mỗi môn thi đúng 1 ca
        for mahp in ds_mon_to_schedule:
            model.Add(
                sum(z[(mahp, d, c)] for d in DAYS for c in CA) == 1
            )
            
        # 2. Ràng buộc cố định (từ phase trước)
        if fixed_schedule:
            for mahp, (fix_d, fix_c) in fixed_schedule.items():
                if mahp in ds_mon_to_schedule:
                    model.Add(z[(mahp, fix_d, fix_c)] == 1)
        
        # 3. Ràng buộc sức chứa
        # FIX: Phải tính capacity đã chiếm bởi fixed_schedule
        MAX_TO_PER_CA = len(self.phong_kha_dung)
        
        # Tính capacity đã dùng bởi fixed_schedule ở mỗi slot
        fixed_usage = {}  # (d, c) -> tổng tổ thi đã fixed
        if fixed_schedule:
            for mahp, (fix_d, fix_c) in fixed_schedule.items():
                if mahp in self.phong_theo_mon:
                    key = (fix_d, fix_c)
                    fixed_usage[key] = fixed_usage.get(key, 0) + self.phong_theo_mon[mahp]["ToThi"]
        
        for d in DAYS:
            for c in CA:
                # Capacity còn lại sau khi trừ phần đã fixed
                used = fixed_usage.get((d, c), 0)
                remaining = MAX_TO_PER_CA - used
                
                # Chỉ constraint cho môn CHƯA fixed (môn mới)
                model.Add(
                    sum(
                        z[(mahp, d, c)] * self.phong_theo_mon[mahp]["ToThi"]
                        for mahp in ds_mon_to_schedule
                        if mahp not in (fixed_schedule or {})  # Môn mới
                    ) <= max(0, remaining)  # Đảm bảo không âm
                )

        # 3.5 Ràng buộc môn chia: D2 phải cách D1 ít nhất 2 ngày
        MIN_GAP_SPLIT = 2
        ds_mon_set = set(ds_mon_to_schedule)
        
        for mahp_goc, split_list in self.split_courses.items():
            if len(split_list) >= 2:
                mahp_d1 = split_list[0][0]
                mahp_d2 = split_list[1][0]
                
                if mahp_d1 in ds_mon_set and mahp_d2 in ds_mon_set:
                    day_d1 = model.NewIntVar(1, len(DAYS), f"day_{mahp_d1}")
                    day_d2 = model.NewIntVar(1, len(DAYS), f"day_{mahp_d2}")
                    
                    model.Add(day_d1 == sum(d * z[(mahp_d1, d, c)] for d in DAYS for c in CA))
                    model.Add(day_d2 == sum(d * z[(mahp_d2, d, c)] for d in DAYS for c in CA))
                    
                    model.Add(day_d2 >= day_d1 + MIN_GAP_SPLIT)
        
        # 4. Sinh viên không trùng ca
        # Phase 1/2 (relax_same_day=False): HARD CONSTRAINT
        # Phase 3 (relax_same_day=True): SOFT với penalty CỰC CAO
        ds_mon_set = set(ds_mon_to_schedule)
        penalty_sv_trung_ca = []
        if self.config.sv_khong_trung_ca:
            for masv, mon_list in self.sv_to_mon.items():
                mon_list_filtered = [m for m in mon_list if m in ds_mon_set]
                if len(mon_list_filtered) <= 1:
                    continue
                for d in DAYS:
                    for c in CA:
                        sum_sv = sum(z[(mahp, d, c)] for mahp in mon_list_filtered)
                        
                        if relax_same_day:
                            # SOFT CONSTRAINT for Phase 3
                            vi_pham = model.NewIntVar(0, len(mon_list_filtered), f"vpsv_{masv}_{d}_{c}")
                            model.Add(vi_pham >= sum_sv - 1)
                            penalty_sv_trung_ca.append(vi_pham)
                        else:
                            # HARD CONSTRAINT for Phase 1/2
                            model.Add(sum_sv <= 1)
        
        # 4b. Penalty cho SV thi NHIỀU MÔN CÙNG NGÀY (khác ca) - SOFT CONSTRAINT
        # Hạn chế tối đa SV phải thi nhiều môn trong 1 ngày
        penalty_sv_trung_ngay = []
        for masv, mon_list in self.sv_to_mon.items():
            mon_list_filtered = [m for m in mon_list if m in ds_mon_set]
            if len(mon_list_filtered) <= 1:
                continue
            
            for d in DAYS:
                sum_sv_ngay = sum(z[(mahp, d, c)] for mahp in mon_list_filtered for c in CA)
                vi_pham_ngay = model.NewIntVar(0, len(mon_list_filtered), f"vpsvngay_{masv}_{d}")
                model.Add(vi_pham_ngay >= sum_sv_ngay - 1)
                penalty_sv_trung_ngay.append(vi_pham_ngay)
        
        # 5. CTĐT-Khóa không thi cùng ngày
        penalty_trung_ngay = []
        if self.config.ctdt_khong_trung_ngay:
            for (ctdt, khoa), mon_list in self.ctdt_khoa_to_mon.items():
                mon_list_filtered = [m for m in mon_list if m in ds_mon_set]
                if len(mon_list_filtered) <= 1:
                    continue
                for d in DAYS:
                    sum_mon = sum(z[(mahp, d, c)] for mahp in mon_list_filtered for c in CA)
                    if relax_same_day:
                        vi_pham = model.NewIntVar(0, len(mon_list_filtered), f"vp_{ctdt}_{khoa}_{d}")
                        model.Add(vi_pham >= sum_mon - 1)
                        penalty_trung_ngay.append(vi_pham)
                    else:
                        model.Add(sum_mon <= 1)
                    
        # 6. CTĐT-Khóa không thi liền ngày (Mềm)
        penalty_lien_ngay = []
        if self.config.ctdt_khong_lien_ngay:
            for (ctdt, khoa), mon_list in self.ctdt_khoa_to_mon.items():
                mon_list_filtered = [m for m in mon_list if m in ds_mon_set]
                if len(mon_list_filtered) <= 1:
                    continue
                
                for i in range(len(DAYS) - 1):
                    d1, d2 = DAYS[i], DAYS[i+1]
                    
                    has_d1 = model.NewBoolVar(f"has_{ctdt}_{khoa}_{d1}")
                    sum_d1 = sum(z[(mahp, d1, c)] for mahp in mon_list_filtered for c in CA)
                    model.Add(sum_d1 >= 1).OnlyEnforceIf(has_d1)
                    model.Add(sum_d1 == 0).OnlyEnforceIf(has_d1.Not())
                    
                    has_d2 = model.NewBoolVar(f"has_{ctdt}_{khoa}_{d2}")
                    sum_d2 = sum(z[(mahp, d2, c)] for mahp in mon_list_filtered for c in CA)
                    model.Add(sum_d2 >= 1).OnlyEnforceIf(has_d2)
                    model.Add(sum_d2 == 0).OnlyEnforceIf(has_d2.Not())
                    
                    both = model.NewBoolVar(f"both_{ctdt}_{khoa}_{d1}_{d2}")
                    model.AddBoolAnd([has_d1, has_d2]).OnlyEnforceIf(both)
                    model.AddBoolOr([has_d1.Not(), has_d2.Not()]).OnlyEnforceIf(both.Not())
                    
                    penalty_lien_ngay.append(both)
        
        # HÀM MỤC TIÊU
        total_objective = []
        
        # 0a. Penalty SV trùng ca (nếu relax)
        HE_SO_SV_TRUNG_CA = 100000000
        for pen in penalty_sv_trung_ca:
            total_objective.append(HE_SO_SV_TRUNG_CA * pen)
        
        # 0b. Penalty CTĐT trùng ngày (nếu relax)
        HE_SO_TRUNG_NGAY = 10000000
        for pen in penalty_trung_ngay:
            total_objective.append(HE_SO_TRUNG_NGAY * pen)
        
        # 0c. Penalty SV thi nhiều môn cùng ngày (khác ca)
        HE_SO_SV_TRUNG_NGAY = 5000000
        for pen in penalty_sv_trung_ngay:
            total_objective.append(HE_SO_SV_TRUNG_NGAY * pen)
        
        # 1. Penalty liền ngày
        HE_SO_PENALTY_LIEN_NGAY = self.config.he_so_penalty_lien_ngay
        for pen in penalty_lien_ngay:
            total_objective.append(HE_SO_PENALTY_LIEN_NGAY * pen)
            
        # 2. Ưu tiên ngày sớm (CHỈ KHI prioritize_early=True)
        if prioritize_early and not distribute_uniformly:
            for mahp in ds_mon_to_schedule:
                so_to = self.phong_theo_mon[mahp]["ToThi"]
                for d in DAYS:
                    for c in CA:
                        # Cách tính điểm phạt: ngày càng lớn phạt càng cao => ưu tiên ngày nhỏ
                        total_objective.append(z[(mahp, d, c)] * d * so_to * 1)
        
        # 3. Ưu tiên ca sớm (CHỈ KHI KHÔNG LOAD BALANCING)
        if not distribute_uniformly:
            for mahp in ds_mon_to_schedule:
                for d in DAYS:
                    for c in CA:
                        total_objective.append(z[(mahp, d, c)] * c * 0.1)

        # 4. LOAD BALANCING (Distribute Uniformly)
        if distribute_uniformly:
            print("   [Load Balancing] Enabling distribute_uniformly...")
            
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
            shift_counts = []
            for c in CA:
                count = sum(z[(mahp, d, c)] for mahp in ds_mon_to_schedule for d in DAYS)
                shift_counts.append(count)
                
            max_exams_per_shift = model.NewIntVar(0, len(ds_mon_to_schedule), "max_exams_per_shift")
            model.AddMaxEquality(max_exams_per_shift, shift_counts)
            
            total_objective.append(max_exams_per_shift * 2000)
                    
        model.Minimize(sum(total_objective))
        
        # Solve
        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = time_limit
        solver.parameters.num_search_workers = self.config.num_workers
        
        status = solver.Solve(model)
        
        if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            result_schedule = {}
            for mahp in ds_mon_to_schedule:
                for d in DAYS:
                    for c in CA:
                        if solver.Value(z[(mahp, d, c)]) == 1:
                            result_schedule[mahp] = (d, c)
                            break
            return result_schedule
        else:
            return None

    def solve(self) -> SchedulerResult:
        """Chạy solver xếp lịch 3 giai đoạn"""
        if not self.data_loaded:
            return SchedulerResult(status="ERROR", error="Data not loaded")
            
        try:
             # 0. CHIA MÔN LỚN THÀNH 2 NGÀY (Logic from test.py)
            NGUONG_CHIA_TO = 25
            self.split_courses = {}  # Reset
            split_courses = {}  # Local var for easy access

            print("\n CHECK LARGE EXAM GROUPS (> 25):")
            for mahp, info in list(self.phong_theo_mon.items()):
                to_thi = info["ToThi"]
                if to_thi > NGUONG_CHIA_TO:
                    to_d1 = to_thi // 2
                    to_d2 = to_thi - to_d1
                    
                    mahp_d1 = f"{mahp}_D1"
                    mahp_d2 = f"{mahp}_D2"
                    
                    split_courses[mahp] = [(mahp_d1, to_d1), (mahp_d2, to_d2)]
                    
                    # Thêm entries mới vào phong_theo_mon
                    self.phong_theo_mon[mahp_d1] = {"ToThi": to_d1, "PhongThi": info.get("PhongThi", "PH")}
                    self.phong_theo_mon[mahp_d2] = {"ToThi": to_d2, "PhongThi": info.get("PhongThi", "PH")}
                    
                    print(f"   - {mahp}: {to_thi} groups -> Split to {mahp_d1}({to_d1}) + {mahp_d2}({to_d2})")
            
            self.split_courses = split_courses
            
            # Helper to replace in lists (defined before use)
            def replace_split_courses(mon_list, split_map):
                result = []
                for m in mon_list:
                    if m in split_map:
                        for mahp_split, _ in split_map[m]:
                            result.append(mahp_split)
                    else:
                        result.append(m)
                return result
            
            # CRITICAL FIX: Update ctdt_khoa_to_mon and sv_to_mon with split codes
            # Without this, constraints will never match the split course codes
            if split_courses:
                # Update ctdt_khoa_to_mon
                new_ctdt_khoa_to_mon = {}
                for key, mon_list in self.ctdt_khoa_to_mon.items():
                    new_ctdt_khoa_to_mon[key] = replace_split_courses(mon_list, split_courses)
                self.ctdt_khoa_to_mon = new_ctdt_khoa_to_mon
                
                # Update sv_to_mon
                new_sv_to_mon = {}
                for masv, mon_list in self.sv_to_mon.items():
                    new_sv_to_mon[masv] = replace_split_courses(mon_list, split_courses)
                self.sv_to_mon = new_sv_to_mon
                
                print(f"   Updated constraints with {len(split_courses)} split courses")

            # 1. Phân loại môn
            mon_count = defaultdict(int)
            for mon_list in self.ctdt_khoa_to_mon.values():
                for m in mon_list:
                    mon_count[m] += 1
            
            # Phase 1: Môn Chung
            ds_mon_chung = [m for m in self.ds_mahp_thi if mon_count[m] > 1]
            ds_mon_phase1 = ds_mon_chung
            
            # Phase 2: Môn Riêng Ưu Tiên
            ds_mon_phase2_all = []
            max_days_phase2 = 5
            
            if self.priority_phase2_config:
                max_days_phase2 = max(p[2] for p in self.priority_phase2_config)
                for c, k, _ in self.priority_phase2_config:
                    mon_list = self.ctdt_khoa_to_mon.get((c, k), [])
                    for m in mon_list:
                        if m not in ds_mon_phase1:
                            ds_mon_phase2_all.append(m)
            
            ds_mon_phase2 = sorted(list(set(ds_mon_phase2_all)))
            ds_toan_bo_mon = self.ds_mahp_thi.tolist()

            # Apply Slit replacement
            if split_courses:
                ds_mon_phase1 = replace_split_courses(ds_mon_phase1, split_courses)
                ds_mon_phase2 = replace_split_courses(ds_mon_phase2, split_courses)
                ds_toan_bo_mon = replace_split_courses(ds_toan_bo_mon, split_courses)

            print(f"Stats Plan: P1={len(ds_mon_phase1)}, P2={len(ds_mon_phase2)}, Total={len(ds_toan_bo_mon)}")
            
            # --- PHASE 1: Môn Chung ---
            schedule_phase1 = self._run_solver_phase(
                "PHASE 1 (Môn chung)",
                ds_mon_phase1,
                fixed_schedule=None,
                time_limit=self.config.solver_timeout,
                prioritize_early=False,
                relax_same_day=True,  # CRITICAL: Enable soft constraints to prevent INFEASIBLE
                distribute_uniformly=True # Load Balancing enabled
            )
            
            if schedule_phase1 is None:
                return SchedulerResult(status="INFEASIBLE", error="Cannot schedule Phase 1 (Common)")
                
            # --- PHASE 2: Ưu Tiên ---
            schedule_phase2 = schedule_phase1.copy()
            schedule_p2_result = {}
            
            if ds_mon_phase2:
                DAYS = list(range(1, len(self.ngay_thi) + 1))
                restricted_days = DAYS[:max_days_phase2]
                
                schedule_p2_result = self._run_solver_phase(
                    "PHASE 2 (Ưu tiên)",
                    ds_mon_phase2,
                    fixed_schedule=schedule_phase1,
                    time_limit=max(60, int(self.config.solver_timeout * 0.5)),
                    restricted_days=restricted_days,
                    prioritize_early=True,
                    relax_same_day=True,
                    distribute_uniformly=False # Phase 2 vẫn ưu tiên sớm trong 5 ngày đầu
                )
                
                if schedule_p2_result:
                    schedule_phase2.update(schedule_p2_result)
                else:
                    print(" [Warning] Phase 2 fail match. Merge to Phase 3.")
            
            # --- PHASE 3: Toàn bộ (Rải đều) ---
            final_schedule_input = schedule_phase2
            
            schedule_final = self._run_solver_phase(
                "PHASE 3 (Toàn bộ - Rải đều)",
                ds_toan_bo_mon,
                fixed_schedule=final_schedule_input,
                time_limit=int(self.config.solver_timeout * 1.5),
                prioritize_early=False,
                relax_same_day=True, # FIX: Enable soft constraints for final phase
                distribute_uniformly=True # Load Balancing enabled
            )
            
            if not schedule_final:
                 return SchedulerResult(status="INFEASIBLE", error="Cannot schedule Phase 3 (Full)")
            
            # --- XỬ LÝ KẾT QUẢ ---
            records = []
            slot_assignments = defaultdict(list)
            
            # Tạo mapping đảo ngược: MaHP_D1/D2 -> MaHP gốc
            split_to_original = {}
            for mahp_goc, split_list in split_courses.items():
                for mahp_split, _ in split_list:
                    split_to_original[mahp_split] = mahp_goc

            for mahp, (d, c) in schedule_final.items():
                ngay = self.map_ngay[d]
                
                # Convert MaHP_D1/D2 về MaHP gốc
                mahp_output = split_to_original.get(mahp, mahp)
                
                so_to = int(self.phong_theo_mon[mahp]["ToThi"])
                
                # Tính offset cho các môn bị chia (D2 phải tiếp nối D1) - FIX BUG STUDENT DISTRIBUTION
                start_offset = 0
                if mahp in split_to_original and mahp_output in split_courses:
                    for m_split, t_split in split_courses[mahp_output]:
                        if m_split == mahp:
                            break
                        start_offset += t_split

                for to in range(1, so_to + 1):
                    actual_to = to + start_offset
                    slot_assignments[(ngay, c)].append((mahp_output, mahp, actual_to))
            
            # Gán phòng
            for (ngay, ca), to_list in slot_assignments.items():
                # Tách theo loại phòng (dùng mahp_internal để tra cứu loại phòng)
                to_list_ph = [(mout, mint, to) for mout, mint, to in to_list if self.phong_theo_mon.get(mint, {}).get("PhongThi", "PH") == "PH"]
                to_list_pm = [(mout, mint, to) for mout, mint, to in to_list if self.phong_theo_mon.get(mint, {}).get("PhongThi", "PH") == "PM"]

                # Sắp xếp
                to_list_ph.sort(key=lambda x: (x[0], x[2]))
                to_list_pm.sort(key=lambda x: (x[0], x[2]))

                # PHONG LISTS
                PHONG_PH = [p for p in self.phong_kha_dung if p.startswith("PH")]
                PHONG_PM = [p for p in self.phong_kha_dung if p.startswith("PM")]
                
                def assign_rooms(item_list, room_list, fallback_list):
                    for idx, (mout, mint, to) in enumerate(item_list):
                        if room_list:
                            phong = room_list[idx % len(room_list)]
                        else:
                            phong = fallback_list[idx % len(fallback_list)]
                        records.append({
                            "MaHP": mout,
                            "ToThi": to,
                            "Ngay": ngay,
                            "Ca": ca,
                            "PhongThi": phong
                        })

                assign_rooms(to_list_ph, PHONG_PH, self.phong_kha_dung)
                assign_rooms(to_list_pm, PHONG_PM, self.phong_kha_dung)
            
            # Tạo records_sv
            records_sv = [] 
            
            return SchedulerResult(
                status="OPTIMAL",
                records=records,
                records_sv=records_sv,
                stats={"msg": "Schedule Success (3 Phases)"}
            )
            
        except Exception as e:
            return SchedulerResult(status="ERROR", error=str(e))
    
    def export_to_excel(self, result: SchedulerResult, output_path: str) -> dict:
        """Xuất kết quả ra file Excel"""
        if not result.records:
            return {"success": False, "error": "Không có kết quả để xuất"}
            
        try:
            df_kq = pd.DataFrame(result.records)
            df_kq["Slot"] = 0
            
            for (ngay, ca), df_sub in df_kq.groupby(["Ngay", "Ca"]):
                df_sub = df_sub.sort_values(["MaHP", "ToThi"])
                for idx, i in enumerate(df_sub.index, start=1):
                    df_kq.loc[i, "Slot"] = idx
            
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
                # Format date column as dd/mm/yyyy
                df_kq_export = df_kq.copy()
                df_kq_export["Ngay"] = pd.to_datetime(df_kq_export["Ngay"]).dt.strftime('%d/%m/%Y')
                
                df_kq_export.sort_values(["Ngay", "Ca", "Slot", "PhongThi"]).to_excel(
                    writer,
                    sheet_name="LichThi_ToThi",
                    index=False
                )
                
                # Kiểm tra vi phạm liền ngày
                if self.df_data_thi is not None:
                    df_kq_ctdt = df_kq.merge(
                        self.df_data_thi[["MaHP", "CTDT", "Khoa"]].drop_duplicates(),
                        on="MaHP",
                        how="left"
                    )
                    
                    violations = []
                    for (ctdt, khoa), group in df_kq_ctdt.groupby(["CTDT", "Khoa"]):
                        if pd.isna(ctdt) or pd.isna(khoa):
                            continue
                        ngay_list = sorted(group["Ngay"].unique())
                        for i in range(len(ngay_list) - 1):
                            d1, d2 = ngay_list[i], ngay_list[i+1]
                            if (d2 - d1).days == 1:
                                mon_d1 = group[group["Ngay"] == d1]["MaHP"].unique()
                                mon_d2 = group[group["Ngay"] == d2]["MaHP"].unique()
                                violations.append({
                                    "CTDT": ctdt,
                                    "Khoa": khoa,
                                    "Ngay1": d1,
                                    "Mon_Ngay1": ", ".join(mon_d1),
                                    "Ngay2": d2,
                                    "Mon_Ngay2": ", ".join(mon_d2)
                                })
                    
                    if violations:
                        df_violations = pd.DataFrame(violations)
                        df_violations.to_excel(
                            writer,
                            sheet_name="ViPham_LienNgay",
                            index=False
                        )
            
            return {
                "success": True,
                "path": output_path,
                "num_violations": len(violations) if 'violations' in dir() else 0
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def export_student_list(self, result: SchedulerResult, output_path: str) -> dict:
        """Xuất danh sách sinh viên thi theo mẫu"""
        if not result.records or self.df_sv_to_thi is None:
            return {"success": False, "error": "Không có dữ liệu để xuất"}
        
        try:
            df_kq = pd.DataFrame(result.records)
            
            # Merge df_sv_to_thi với kết quả xếp lịch
            df_sv_lich_thi = self.df_sv_to_thi.merge(
                df_kq[["MaHP", "ToThi", "Ngay", "Ca", "PhongThi"]],
                on=["MaHP", "ToThi"],
                how="left"
            )
            
            # Merge với thông tin môn học
            if "TenMH" in self.df_lhp.columns and "SoTC" in self.df_lhp.columns:
                df_sv_lich_thi = df_sv_lich_thi.merge(
                    self.df_lhp[["MaHP", "TenMH", "SoTC"]].drop_duplicates(),
                    on="MaHP",
                    how="left"
                )
            else:
                df_sv_lich_thi["TenMH"] = ""
                df_sv_lich_thi["SoTC"] = ""
            
            # Merge với thông tin sinh viên đầy đủ
            if "Lop" in self.df_sv.columns:
                df_sv_full = self.df_sv[["MaSV", "Lop"]].drop_duplicates()
                df_sv_lich_thi = df_sv_lich_thi.merge(
                    df_sv_full,
                    on="MaSV",
                    how="left"
                )
            else:
                df_sv_lich_thi["Lop"] = ""
            
            # Tách Họ đệm và Tên
            def tach_ho_ten(ten_full):
                if pd.isna(ten_full):
                    return "", ""
                parts = str(ten_full).strip().split()
                if len(parts) == 0:
                    return "", ""
                elif len(parts) == 1:
                    return "", parts[0]
                else:
                    return " ".join(parts[:-1]), parts[-1]
            
            df_sv_lich_thi["HoDem"] = df_sv_lich_thi["Ten"].apply(lambda x: tach_ho_ten(x)[0])
            df_sv_lich_thi["TenSV"] = df_sv_lich_thi["Ten"].apply(lambda x: tach_ho_ten(x)[1])
            
            # Tạo các cột theo mẫu
            df_sv_lich_thi["MaHP_Full"] = df_sv_lich_thi["MaHP"].apply(lambda x: f"251{x}" if pd.notna(x) else "")
            df_sv_lich_thi["GioThi"] = df_sv_lich_thi["Ca"].map(self.CA_TO_GIO)
            df_sv_lich_thi["NgayThi_Str"] = df_sv_lich_thi["Ngay"].apply(
                lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else ""
            )
            
            # Tạo DataFrame xuất theo mẫu
            df_export_sv = pd.DataFrame({
                "Mã MH": df_sv_lich_thi["MaHP"],
                "Mã HP": df_sv_lich_thi["MaHP_Full"],
                "Tên môn": df_sv_lich_thi["TenMH"],
                "Số TC": df_sv_lich_thi["SoTC"],
                "Đợt thi": 1,
                "Nhóm thi": 1,
                "Tổ thi": df_sv_lich_thi["ToThi"],
                "Ngày thi": df_sv_lich_thi["NgayThi_Str"],
                "Giờ thi": df_sv_lich_thi["GioThi"],
                "Phòng thi": df_sv_lich_thi["PhongThi"],
                "Mã SV": df_sv_lich_thi["MaSV"],
                "Họ đệm": df_sv_lich_thi["HoDem"],
                "Tên": df_sv_lich_thi["TenSV"],
                "Lớp": df_sv_lich_thi["Lop"],
                "Ghi chú": "",
                "Lần thi": 1
            })
            
            # Sắp xếp theo ngày, ca, phòng, tên
            df_export_sv = df_export_sv.sort_values(
                ["Ngày thi", "Giờ thi", "Phòng thi", "Tên", "Họ đệm"]
            )
            
            # Xuất file
            df_export_sv.to_excel(output_path, index=False, sheet_name="DanhSachSVThi")
            
            return {
                "success": True,
                "path": output_path,
                "total_rows": len(df_export_sv)
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
