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
    he_so_penalty_lien_ngay: int = 1000000 # Tăng penalty cực cao để hạn chế tối đa
    solver_timeout: int = 300 # Tăng timeout để solver có thời gian tìm kiếm
    num_workers: int = 8


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
                          relax_same_day: bool = False):
        """Helper chạy solver cho một phase"""
        print(f"🚀 [Scheduler] Đang chạy {phase_name}...")
        
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
        
        # 4. Sinh viên không trùng ca
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
                            vi_pham = model.NewIntVar(0, len(mon_list_filtered), f"vpsv_{masv}_{d}_{c}")
                            model.Add(vi_pham >= sum_sv - 1)
                            penalty_sv_trung_ca.append(vi_pham)
                        else:
                            model.Add(sum_sv <= 1)
        
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
        
        # 1. Penalty liền ngày
        HE_SO_PENALTY_LIEN_NGAY = self.config.he_so_penalty_lien_ngay
        for pen in penalty_lien_ngay:
            total_objective.append(HE_SO_PENALTY_LIEN_NGAY * pen)
            
        # 2. Ưu tiên ngày sớm (CHỈ KHI prioritize_early=True)
        if prioritize_early:
            for mahp in ds_mon_to_schedule:
                so_to = self.phong_theo_mon[mahp]["ToThi"]
                for d in DAYS:
                    for c in CA:
                        # Cách tính điểm phạt: ngày càng lớn phạt càng cao => ưu tiên ngày nhỏ
                        total_objective.append(z[(mahp, d, c)] * d * so_to * 1)
        
        # 3. Ưu tiên ca sớm
        for mahp in ds_mon_to_schedule:
            for d in DAYS:
                for c in CA:
                    total_objective.append(z[(mahp, d, c)] * c * 0.1)
                    
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
            return SchedulerResult(status="ERROR", error="Dữ liệu chưa được load")
            
        try:
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
            
            print(f"Stats Plan: P1(Chung)={len(ds_mon_phase1)}, P2(UuTien)={len(ds_mon_phase2)} (Max {max_days_phase2} days), Total={len(self.ds_mahp_thi)}")
            
            # --- PHASE 1: Môn Chung ---
            schedule_phase1 = self._run_solver_phase(
                "PHASE 1 (Môn chung)",
                ds_mon_phase1,
                fixed_schedule=None,
                time_limit=self.config.solver_timeout,
                prioritize_early=True
            )
            
            if schedule_phase1 is None:
                return SchedulerResult(status="INFEASIBLE", error="Không xếp được lịch cho môn chung (Phase 1)")
                
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
                    relax_same_day=True  # Cho phép vi phạm để đảm bảo có nghiệm
                )
                
                if schedule_p2_result:
                    schedule_phase2.update(schedule_p2_result)
                else:
                    print("⚠️ Warning: Phase 2 fail to solve fully within restricted days. Will merge to Phase 3.")
            
            # --- PHASE 3: Toàn bộ (Rải đều) ---
            final_schedule_input = schedule_phase2
            
            schedule_final = self._run_solver_phase(
                "PHASE 3 (Toàn bộ - Rải đều)",
                self.ds_mahp_thi.tolist(),
                fixed_schedule=final_schedule_input,
                time_limit=int(self.config.solver_timeout * 1.5),
                prioritize_early=False
            )
            
            if not schedule_final:
                 return SchedulerResult(status="INFEASIBLE", error="Không xếp được lịch Phase 3 (Toàn bộ)")
            
            # --- XỬ LÝ KẾT QUẢ ---
            records = []
            slot_assignments = defaultdict(list)
            
            for mahp, (d, c) in schedule_final.items():
                ngay = self.map_ngay[d]
                for to in range(1, self.phong_theo_mon[mahp]["ToThi"] + 1):
                    slot_assignments[(ngay, c)].append((mahp, to))
            
            # Gán phòng
            for (ngay, ca), to_list in slot_assignments.items():
                to_list.sort(key=lambda x: (x[0], x[1]))
                for idx, (mahp, to) in enumerate(to_list):
                    phong = self.phong_kha_dung[idx % len(self.phong_kha_dung)]
                    records.append({
                        "MaHP": mahp,
                        "ToThi": to,
                        "Ngay": ngay,
                        "Ca": ca,
                        "PhongThi": phong
                    })
            
            # Tạo records_sv
            records_sv = [] 
            
            return SchedulerResult(
                status="OPTIMAL",
                records=records,
                records_sv=records_sv,
                stats={"msg": "Xếp lịch thành công 3 Phase"}
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
                df_kq.sort_values(["Ngay", "Ca", "Slot", "PhongThi"]).to_excel(
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
