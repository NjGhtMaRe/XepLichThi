from scheduler import ExamScheduler
import os
import sys

# Add path to sys to ensure imports work
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)

# Paths
P_LHP = os.path.join(BASE_DIR, "danhsachLHP.xlsx")
P_DATA = os.path.join(BASE_DIR, "Data.xlsx")
P_CFG = os.path.join(BASE_DIR, "cau_hinh.xlsx")
P_SV = os.path.join(BASE_DIR, "danhsachSV.xlsx")

print("Initializing Scheduler...")
scheduler = ExamScheduler()
print("Loading data...")
res_load = scheduler.load_data(P_LHP, P_DATA, P_CFG, P_SV)

if res_load.get("success"):
    print("Load Success. Config Phase 2:", scheduler.priority_phase2_config)
    print("Running SOLVE (3 Phases)...")
    res_solve = scheduler.solve()
    print(f"Status: {res_solve.status}")
    print(f"Stats: {res_solve.stats}")
    if res_solve.error:
        print(f"Error: {res_solve.error}")
else:
    print(f"Load data failed: {res_load}")
