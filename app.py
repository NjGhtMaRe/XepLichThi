"""
Flask Web Application - Xếp Lịch Thi
"""

from flask import Flask, request, jsonify, render_template, send_file
import os
import json
from werkzeug.utils import secure_filename
from scheduler import ExamScheduler, SchedulerConfig, SchedulerResult
from datetime import datetime
import pandas as pd
import math

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max

# Thư mục lưu trữ
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
RESULT_FOLDER = os.path.join(BASE_DIR, 'results')
RESULT_FOLDER = os.path.join(BASE_DIR, 'results')

# Tạo thư mục nếu chưa có
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULT_FOLDER, exist_ok=True)

# File types được phép
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

# Mapping file types
FILE_TYPES = {
    'lhp': 'danhsachLHP.xlsx',
    'data': 'Data.xlsx', 
    'cfg': 'cau_hinh.xlsx',
    'sv': 'danhsachSV.xlsx'
}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS





@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/upload', methods=['POST'])
def upload_files():
    """Upload file Excel"""
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'Không có file nào được gửi'})
    
    file = request.files['file']
    file_type = request.form.get('type', '')
    
    if file.filename == '':
        return jsonify({'success': False, 'error': 'Tên file trống'})
    
    if not allowed_file(file.filename):
        return jsonify({'success': False, 'error': 'Định dạng file không hợp lệ'})
    
    # Lưu với tên chuẩn hoặc tên gốc
    if file_type in FILE_TYPES:
        filename = FILE_TYPES[file_type]
    else:
        filename = secure_filename(file.filename)
    
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)
    
    return jsonify({
        'success': True,
        'filename': filename,
        'size': os.path.getsize(filepath)
    })


@app.route('/api/files', methods=['GET'])
def get_files():
    """Lấy danh sách files đã upload"""
    files = {}
    for file_type, filename in FILE_TYPES.items():
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        if os.path.exists(filepath):
            files[file_type] = {
                'filename': filename,
                'size': os.path.getsize(filepath),
                'modified': datetime.fromtimestamp(os.path.getmtime(filepath)).isoformat()
            }
        else:
            files[file_type] = None
    
    return jsonify({'success': True, 'files': files})





@app.route('/api/solve', methods=['POST'])
def solve():
    """Chạy solver xếp lịch"""
    # Kiểm tra files
    missing_files = []
    for file_type, filename in FILE_TYPES.items():
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        if not os.path.exists(filepath):
            missing_files.append(filename)
    
    if missing_files:
        return jsonify({
            'success': False,
            'error': f'Thiếu các file: {", ".join(missing_files)}'
        })
    
    # Config mặc định (đã loại bỏ tùy chỉnh)
    config = SchedulerConfig(
        max_to_per_ca=68,
        sv_khong_trung_ca=True,
        ctdt_khong_trung_ngay=True,
        ctdt_khong_lien_ngay=True,
        he_so_penalty_lien_ngay=10,
        solver_timeout=300, # Tăng timeout mặc định
        num_workers=8,
        distribute_uniformly=True # Luôn bật load balancing
    )
    
    # Khởi tạo scheduler
    scheduler = ExamScheduler(config)
    
    # Load data
    load_result = scheduler.load_data(
        path_lhp=os.path.join(UPLOAD_FOLDER, FILE_TYPES['lhp']),
        path_data=os.path.join(UPLOAD_FOLDER, FILE_TYPES['data']),
        path_cfg=os.path.join(UPLOAD_FOLDER, FILE_TYPES['cfg']),
        path_sv=os.path.join(UPLOAD_FOLDER, FILE_TYPES['sv'])
    )
    
    if not load_result['success']:
        return jsonify({
            'success': False,
            'error': f'Lỗi đọc dữ liệu: {load_result.get("error", "Unknown")}'
        })
    
    # Solve
    result = scheduler.solve()
    
    if result.error:
        return jsonify({
            'success': False,
            'status': result.status,
            'error': result.error,
            'data_stats': load_result['stats']
        })
    
    # Export kết quả
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_filename = f'ket_qua_xep_lich_{timestamp}.xlsx'
    output_path = os.path.join(RESULT_FOLDER, output_filename)
    
    export_result = scheduler.export_to_excel(result, output_path)
    
    # Export danh sách sinh viên thi
    sv_filename = f'BangTongHopLichThiSinhVien_{timestamp}.xlsx'
    sv_path = os.path.join(RESULT_FOLDER, sv_filename)
    sv_export_result = scheduler.export_student_list(result, sv_path)
    
    return jsonify({
        'success': True,
        'status': result.status,
        'result_file': output_filename,
        'student_file': sv_filename if sv_export_result.get('success') else None,
        'num_records': len(result.records),
        'num_student_records': sv_export_result.get('total_rows', 0),
        'num_violations': export_result.get('num_violations', 0),
        'data_stats': load_result['stats'],
        'solver_stats': result.stats,
        'records': result.records[:500]  # Giới hạn 500 dòng để tránh quá tải
    })


@app.route('/api/download/<filename>')
def download_file(filename):
    """Tải xuống file kết quả"""
    filepath = os.path.join(RESULT_FOLDER, secure_filename(filename))
    if os.path.exists(filepath):
        return send_file(filepath, as_attachment=True)
    return jsonify({'success': False, 'error': 'File không tồn tại'})


# ==========================================
# HELPER: Sync Student File After Schedule Update
# ==========================================
def sync_student_file():
    """Cập nhật BangTongHopLichThiSinhVien_KetQua.xlsx khi lịch thi thay đổi"""
    try:
        schedule_path = os.path.join(BASE_DIR, 'ket_qua_xep_lich_thi.xlsx')
        sv_file_path = os.path.join(BASE_DIR, 'BangTongHopLichThiSinhVien_KetQua.xlsx')
        
        if not os.path.exists(sv_file_path):
            print("   ⚠️ BangTongHopLichThiSinhVien_KetQua.xlsx chưa tồn tại, bỏ qua sync.")
            return
        
        if not os.path.exists(schedule_path):
            print("   ⚠️ ket_qua_xep_lich_thi.xlsx chưa tồn tại, bỏ qua sync.")
            return
        
        # Load files
        df_sv = pd.read_excel(sv_file_path)
        df_schedule = pd.read_excel(schedule_path)
        
        # Normalize column names for matching
        # Student file uses "Mã HP", "Tổ thi" vs schedule uses "MaHP", "ToThi"
        # Map schedule columns
        schedule_map = {}
        for _, row in df_schedule.iterrows():
            mahp = str(row.get('MaHP', '')).strip()
            tothi = int(row.get('ToThi', 0)) if pd.notna(row.get('ToThi')) else 0
            key = (mahp, tothi)
            schedule_map[key] = {
                'Ngày thi': row.get('Ngay', ''),
                'Giờ thi': row.get('GioThi', '') if 'GioThi' in df_schedule.columns else '',
                'Phòng thi': row.get('PhongThi', ''),
                'Ca': row.get('Ca', '')
            }
        
        # Map Ca to Gio
        CA_TO_GIO = {1: "07:00", 2: "09:30", 3: "13:00", 4: "15:30"}
        
        # Update student file
        updated_count = 0
        for idx, row in df_sv.iterrows():
            mahp = str(row.get('Mã HP', '')).strip()
            tothi = int(row.get('Tổ thi', 0)) if pd.notna(row.get('Tổ thi')) else 0
            key = (mahp, tothi)
            
            if key in schedule_map:
                new_data = schedule_map[key]
                if 'Ngày thi' in df_sv.columns:
                    df_sv.at[idx, 'Ngày thi'] = new_data['Ngày thi']
                if 'Phòng thi' in df_sv.columns:
                    df_sv.at[idx, 'Phòng thi'] = new_data['Phòng thi']
                if 'Giờ thi' in df_sv.columns:
                    gio = new_data['Giờ thi']
                    if not gio and new_data['Ca']:
                        gio = CA_TO_GIO.get(int(new_data['Ca']), '')
                    df_sv.at[idx, 'Giờ thi'] = gio
                updated_count += 1
        
        # Save updated student file
        df_sv.to_excel(sv_file_path, index=False)
        print(f"   ✅ Synced {updated_count} rows to BangTongHopLichThiSinhVien_KetQua.xlsx")
        
    except Exception as e:
        print(f"   ⚠️ Error syncing student file: {e}")

# ==========================================
# SCHEDULE VISUALIZATION & EDITING APIs
# ==========================================

@app.route('/api/schedule/data', methods=['GET'])
def get_schedule_data():
    """Lấy dữ liệu lịch thi để hiển thị"""
    try:
        # File paths
        schedule_path = os.path.join(BASE_DIR, 'ket_qua_xep_lich_thi.xlsx')
        config_path = os.path.join(BASE_DIR, 'cau_hinh.xlsx') # Hoặc file cấu hình upload

        if not os.path.exists(schedule_path):
            return jsonify({'success': False, 'error': 'Chưa có dữ liệu lịch thi. Vui lòng xếp lịch trước.'})
        
        # Load Result
        df_sche = pd.read_excel(schedule_path)
        # Clean data (convert NaN to None/Empty)
        df_sche = df_sche.where(pd.notnull(df_sche), None)
        
        # Load Config (Rooms, Days, Shifts)
        if os.path.exists(config_path):
            df_rooms = pd.read_excel(config_path, sheet_name='PhongThi')
            rooms = df_rooms['PhongThi'].dropna().unique().tolist()
            
            # Days from Config or from Result? Better from Result to be safe
            # But we need full list of available slots
            df_time = pd.read_excel(config_path, sheet_name='ThoiGianThi')
            available_days = df_time[df_time['SuDung'] == 1]['NgayThi'].dt.strftime('%d/%m/%Y').tolist()
            
            df_ca = pd.read_excel(config_path, sheet_name='CaThi')
            shifts = df_ca['Ca'].dropna().unique().tolist()
        else:
            # Fallback if config missing
            rooms = df_sche['PhongThi'].unique().tolist()
            available_days = df_sche['Ngay'].unique().tolist()
            shifts = [1, 2, 3, 4]

        # Structure data
        records = df_sche.to_dict('records')
        
        return jsonify({
            'success': True,
            'rooms': sorted(rooms),
            'days': available_days,
            'shifts': sorted(shifts),
            'schedule': records
        })

    except Exception as e:
        print(f"Error loading schedule: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/schedule/update', methods=['POST'])
def update_schedule():
    """Cập nhật lịch thi (Move/Swap)"""
    try:
        data = request.json
        # data format: 
        # { 
        #   "action": "move" | "swap", 
        #   "source": { "MaHP": "...", "ToThi": 1, "Ngay": "...", "Ca": 1, "PhongThi": "..." },
        #   "target": { "Ngay": "...", "Ca": 1, "PhongThi": "..." } 
        # }
        
        schedule_path = os.path.join(BASE_DIR, 'ket_qua_xep_lich_thi.xlsx')
        if not os.path.exists(schedule_path):
            return jsonify({'success': False, 'error': 'File lịch thi không tồn tại'})
            
        df = pd.read_excel(schedule_path)
        
        action = data.get('action')
        source = data.get('source')
        target = data.get('target') # Target slot (Ngay, Ca, PhongThi)
        
        if not source or not target:
            return jsonify({'success': False, 'error': 'Thiếu thông tin source/target'})

        # Tìm source row index
        source_idx = df[
            (df['MaHP'] == source['MaHP']) & 
            (df['ToThi'] == source['ToThi'])
        ].index
        
        if len(source_idx) == 0:
             return jsonify({'success': False, 'error': 'Không tìm thấy môn học nguồn'})
        
        source_idx = source_idx[0]
        
        # Check if target slot is occupied
        target_idx = df[
            (df['Ngay'] == target['Ngay']) & 
            (df['Ca'] == int(target['Ca'])) & 
            (df['PhongThi'] == target['PhongThi'])
        ].index
        
        if len(target_idx) > 0:
            target_idx = target_idx[0]
            # Target occupied -> SWAP or ERROR?
            # User wants to "đổi lịch cho nhau" => Swap
            
            # Update target row to source values
            df.at[target_idx, 'Ngay'] = source['Ngay']
            df.at[target_idx, 'Ca'] = int(source['Ca'])
            df.at[target_idx, 'PhongThi'] = source['PhongThi']
            
            msg = "Đã hoán đổi lịch thi thành công"
        else:
            msg = "Đã chuyển lịch thi thành công"

        # Update source row to target values
        df.at[source_idx, 'Ngay'] = target['Ngay']
        df.at[source_idx, 'Ca'] = int(target['Ca'])
        df.at[source_idx, 'PhongThi'] = target['PhongThi']
        
        # Save back to Excel
        df.to_excel(schedule_path, index=False)
        
        # Sync changes to student file
        sync_student_file()
        
        return jsonify({'success': True, 'message': msg})

    except Exception as e:
        print(f"Error updating schedule: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/schedule/batch-update', methods=['POST'])
def batch_update_schedule():
    """Batch update: Move multiple exam groups to a target day/shift with conflict checking"""
    try:
        data = request.json
        # data format:
        # {
        #   "items": [{ "MaHP": "...", "ToThi": 1 }, ...],
        #   "target": { "Ngay": "...", "Ca": 1 },
        #   "force_move": false  # If true, bypass same-day warning
        # }
        
        schedule_path = os.path.join(BASE_DIR, 'ket_qua_xep_lich_thi.xlsx')
        config_path = os.path.join(UPLOAD_FOLDER, 'cau_hinh.xlsx')
        sv_path = os.path.join(UPLOAD_FOLDER, 'danhsachSV.xlsx')
        
        if not os.path.exists(schedule_path):
            return jsonify({'success': False, 'error': 'Schedule file not found'})
            
        df_schedule = pd.read_excel(schedule_path)
        
        items = data.get('items', [])
        target = data.get('target', {})
        force_move = data.get('force_move', False)
        
        if not items or not target:
            return jsonify({'success': False, 'error': 'Missing items or target'})
        
        target_day = target.get('Ngay')
        target_shift = int(target.get('Ca'))
        
        # === CONFLICT CHECK ===
        # Load student list to check conflicts
        if os.path.exists(sv_path):
            df_sv = pd.read_excel(sv_path)
            df_sv['MaSV'] = df_sv['MaSV'].astype(str).str.strip()
            df_sv['MaHP'] = df_sv['MaHP'].astype(str).str.strip()
            
            # Build map: MaSV -> list of MaHP they take
            sv_to_mahp = df_sv.groupby('MaSV')['MaHP'].apply(list).to_dict()
            
            # Get students in items being moved with their ToThi
            moving_info = {item['MaHP']: item['ToThi'] for item in items}
            moving_mahps = list(moving_info.keys())
            students_in_moving = df_sv[df_sv['MaHP'].isin(moving_mahps)][['MaSV', 'MaHP']].drop_duplicates()
            
            # Get exams already in target slot (same day, same shift)
            exams_same_shift_df = df_schedule[
                (df_schedule['Ngay'] == target_day) & 
                (df_schedule['Ca'] == target_shift) &
                (~df_schedule['MaHP'].isin(moving_mahps))
            ][['MaHP', 'ToThi']]
            exams_same_shift = set(exams_same_shift_df['MaHP'].unique())
            
            # Get exams on same day (any shift)
            exams_same_day_df = df_schedule[
                (df_schedule['Ngay'] == target_day) &
                (~df_schedule['MaHP'].isin(moving_mahps))
            ][['MaHP', 'ToThi', 'Ca']]
            exams_same_day = set(exams_same_day_df['MaHP'].unique())
            
            # Check for SAME-SHIFT conflicts (HARD BLOCK)
            conflict_details_shift = []
            for _, row in students_in_moving.iterrows():
                masv = row['MaSV']
                moving_mahp = row['MaHP']
                moving_to = moving_info[moving_mahp]
                
                # Check if this student has other exams in target shift
                student_other_mahps = [m for m in sv_to_mahp.get(masv, []) if m in exams_same_shift]
                for conflict_mahp in student_other_mahps:
                    conflict_to = exams_same_shift_df[exams_same_shift_df['MaHP'] == conflict_mahp]['ToThi'].values
                    conflict_to = int(conflict_to[0]) if len(conflict_to) > 0 else '?'
                    conflict_details_shift.append({
                        'MaSV': masv,
                        'moving_MaHP': moving_mahp,
                        'moving_ToThi': moving_to,
                        'conflict_MaHP': conflict_mahp,
                        'conflict_ToThi': conflict_to
                    })
            
            if conflict_details_shift:
                # Limit to 15 entries
                return jsonify({
                    'success': False,
                    'error_type': 'CONFLICT_SHIFT',
                    'error': f'Cannot move! {len(conflict_details_shift)} conflict(s) in same shift.',
                    'conflict_details': conflict_details_shift[:15]
                })
            
            # Check for SAME-DAY conflicts (SOFT WARNING)
            if not force_move:
                conflict_details_day = []
                for _, row in students_in_moving.iterrows():
                    masv = row['MaSV']
                    moving_mahp = row['MaHP']
                    moving_to = moving_info[moving_mahp]
                    
                    student_other_mahps = [m for m in sv_to_mahp.get(masv, []) if m in exams_same_day]
                    for conflict_mahp in student_other_mahps:
                        conflict_row = exams_same_day_df[exams_same_day_df['MaHP'] == conflict_mahp].iloc[0] if len(exams_same_day_df[exams_same_day_df['MaHP'] == conflict_mahp]) > 0 else None
                        if conflict_row is not None:
                            conflict_details_day.append({
                                'MaSV': masv,
                                'moving_MaHP': moving_mahp,
                                'moving_ToThi': moving_to,
                                'conflict_MaHP': conflict_mahp,
                                'conflict_ToThi': int(conflict_row['ToThi']),
                                'conflict_Ca': int(conflict_row['Ca'])
                            })
                
                if conflict_details_day:
                    return jsonify({
                        'success': False,
                        'error_type': 'WARNING_SAME_DAY',
                        'error': f'Warning: {len(conflict_details_day)} same-day conflict(s).',
                        'conflict_details': conflict_details_day[:15],
                        'can_force': True
                    })
        
        # === ROOM CHECK ===
        if os.path.exists(config_path):
            df_rooms = pd.read_excel(config_path, sheet_name='PhongThi')
            all_rooms = df_rooms['PhongThi'].dropna().astype(str).str.strip().tolist()
        else:
            all_rooms = df_schedule['PhongThi'].unique().tolist()
        
        used_rooms = df_schedule[
            (df_schedule['Ngay'] == target_day) & 
            (df_schedule['Ca'] == target_shift)
        ]['PhongThi'].tolist()
        
        available_rooms = [r for r in all_rooms if r not in used_rooms]
        
        if len(available_rooms) < len(items):
            return jsonify({
                'success': False, 
                'error': f'Not enough rooms. Need {len(items)}, only {len(available_rooms)} available.'
            })
        
        # === UPDATE SCHEDULE ===
        moved_count = 0
        for i, item in enumerate(items):
            idx = df_schedule[
                (df_schedule['MaHP'] == item['MaHP']) & 
                (df_schedule['ToThi'] == item['ToThi'])
            ].index
            
            if len(idx) > 0:
                idx = idx[0]
                df_schedule.at[idx, 'Ngay'] = target_day
                df_schedule.at[idx, 'Ca'] = target_shift
                df_schedule.at[idx, 'PhongThi'] = available_rooms[i]
                moved_count += 1
        
        df_schedule.to_excel(schedule_path, index=False)
        
        # Sync changes to student file
        sync_student_file()
        
        return jsonify({
            'success': True, 
            'message': f'Moved {moved_count} exam group(s) successfully.'
        })

    except Exception as e:
        print(f"Error batch updating schedule: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/results', methods=['GET'])
def list_results():
    """Danh sách các file kết quả"""
    results = []
    if os.path.exists(RESULT_FOLDER):
        for filename in os.listdir(RESULT_FOLDER):
            if filename.endswith('.xlsx'):
                filepath = os.path.join(RESULT_FOLDER, filename)
                results.append({
                    'filename': filename,
                    'size': os.path.getsize(filepath),
                    'created': datetime.fromtimestamp(os.path.getctime(filepath)).isoformat()
                })
    
    results.sort(key=lambda x: x['created'], reverse=True)
    return jsonify({'success': True, 'results': results})


@app.route('/api/export-students', methods=['POST'])
def export_students():
    """Xuất file BangTongHopLichThiSinhVien_KetQua.xlsx từ lịch đã chỉnh sửa"""
    try:
        # Paths
        schedule_path = os.path.join(BASE_DIR, 'ket_qua_xep_lich_thi.xlsx')
        sv_path = os.path.join(BASE_DIR, 'danhsachSV.xlsx')
        lhp_path = os.path.join(BASE_DIR, 'danhsachLHP.xlsx')
        config_path = os.path.join(BASE_DIR, 'cau_hinh.xlsx')
        output_path = os.path.join(BASE_DIR, 'BangTongHopLichThiSinhVien_KetQua.xlsx')
        
        # Check files exist
        if not os.path.exists(schedule_path):
            return jsonify({'success': False, 'error': 'Chưa có file lịch thi. Vui lòng xếp lịch trước.'})
        if not os.path.exists(sv_path):
            return jsonify({'success': False, 'error': 'Chưa có file danhsachSV.xlsx'})
        if not os.path.exists(lhp_path):
            return jsonify({'success': False, 'error': 'Chưa có file danhsachLHP.xlsx'})
        
        # Load data
        df_kq = pd.read_excel(schedule_path)
        df_sv = pd.read_excel(sv_path)
        df_lhp = pd.read_excel(lhp_path)
        
        # Normalize MaHP
        df_sv["MaSV"] = df_sv["MaSV"].astype(str).str.strip()
        df_sv["Ten"] = df_sv["Ten"].astype(str).str.strip()
        df_sv["MaHP"] = df_sv["MaHP"].astype(str).str.strip()
        df_kq["MaHP"] = df_kq["MaHP"].astype(str).str.strip()
        df_lhp["MaHP"] = df_lhp["MaHP"].astype(str).str.strip()
        
        # Remove duplicates
        df_sv = df_sv.drop_duplicates(subset=["MaSV", "MaHP"], keep="first")
        
        # Create phong_theo_mon
        phong_theo_mon = df_lhp.set_index("MaHP")[["ToThi"]].to_dict("index")
        
        # Distribute students to ToThi
        ds_sv_to_thi = []
        for mahp, df_mhp in df_sv.groupby("MaHP"):
            if mahp not in phong_theo_mon:
                continue
            so_to = int(phong_theo_mon[mahp]["ToThi"])
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
        
        df_sv_to_thi = pd.DataFrame(ds_sv_to_thi)
        
        # Merge with schedule
        df_final_sv = pd.merge(df_sv_to_thi, df_kq, on=["MaHP", "ToThi"], how="left")
        
        # Add course info
        available_lhp_cols = ["MaHP"]
        if "TenMH" in df_lhp.columns:
            available_lhp_cols.append("TenMH")
        elif "Ten_MH" in df_lhp.columns:
            available_lhp_cols.append("Ten_MH")
        if "SoTC" in df_lhp.columns:
            available_lhp_cols.append("SoTC")
        
        if len(available_lhp_cols) > 1:
            df_lhp_info = df_lhp[available_lhp_cols].drop_duplicates("MaHP")
            df_final_sv = pd.merge(df_final_sv, df_lhp_info, on="MaHP", how="left")
        
        # Add time from Ca
        CA_TO_GIO = {1: "07:00", 2: "09:30", 3: "13:00", 4: "15:30"}
        if "Ca" in df_final_sv.columns:
            df_final_sv["GioThi"] = df_final_sv["Ca"].map(CA_TO_GIO)
        
        # Split Ho Ten
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
        
        # Rename columns
        rename_dict = {
            "Ten_MH": "Tên môn", "TenMH": "Tên môn",
            "SoTC": "Số TC", "MaSV": "Mã SV",
            "Ngay": "Ngày thi", "GioThi": "Giờ thi",
            "PhongThi": "Phòng thi", "ToThi": "Tổ thi",
            "HoDem": "Họ đệm", "TenSV": "Tên"
        }
        df_final_sv = df_final_sv.rename(columns=rename_dict)
        
        # Add required columns
        df_final_sv["Đợt thi"] = "Đợt 1"
        df_final_sv["Nhóm thi"] = "1"
        df_final_sv["Ghi chú"] = ""
        df_final_sv["Mã HP"] = df_final_sv["MaHP"]
        
        # Select output columns
        output_cols = [
            "Mã HP", "Tên môn", "Số TC", "Đợt thi", "Nhóm thi", "Tổ thi",
            "Ngày thi", "Giờ thi", "Phòng thi", "Mã SV", "Họ đệm", "Tên", "Ghi chú"
        ]
        existing_cols = [c for c in output_cols if c in df_final_sv.columns]
        df_final_sv = df_final_sv[existing_cols]
        
        # Remove duplicates
        df_final_sv = df_final_sv.drop_duplicates()
        
        # Export
        df_final_sv.to_excel(output_path, index=False)
        
        return jsonify({
            'success': True,
            'message': f'Đã xuất file thành công: {len(df_final_sv)} dòng',
            'filename': 'BangTongHopLichThiSinhVien_KetQua.xlsx',
            'rows': len(df_final_sv)
        })
        
    except Exception as e:
        print(f"Error exporting students: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/preview/<file_type>', methods=['GET'])
def preview_file(file_type):
    """Xem trước nội dung file"""
    import pandas as pd
    
    if file_type not in FILE_TYPES:
        return jsonify({'success': False, 'error': 'Loại file không hợp lệ'})
    
    filename = FILE_TYPES[file_type]
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    
    if not os.path.exists(filepath):
        return jsonify({'success': False, 'error': 'File chưa được upload'})
    
    try:
        # Đọc file Excel
        if file_type == 'cfg':
            # Đọc tất cả sheets cho file cấu hình
            xl = pd.ExcelFile(filepath)
            sheets = {}
            for sheet in xl.sheet_names:
                df = pd.read_excel(filepath, sheet_name=sheet)
                sheets[sheet] = {
                    'columns': df.columns.tolist(),
                    'data': df.head(10).to_dict('records'),
                    'total_rows': len(df)
                }
            return jsonify({
                'success': True,
                'type': 'multi_sheet',
                'sheets': sheets
            })
        else:
            df = pd.read_excel(filepath)
            return jsonify({
                'success': True,
                'type': 'single_sheet',
                'columns': df.columns.tolist(),
                'data': df.head(20).to_dict('records'),
                'total_rows': len(df)
            })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


if __name__ == '__main__':
    print("=" * 50)
    print("🎓 XẾP LỊCH THI - WEB APPLICATION")
    print("=" * 50)
    print(f"📁 Upload folder: {UPLOAD_FOLDER}")
    print(f"📁 Result folder: {RESULT_FOLDER}")
    print("🌐 Mở trình duyệt tại: http://127.0.0.1:5000")
    print("=" * 50)
    app.run(debug=True, port=5000)
