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
CONFIG_FILE = os.path.join(BASE_DIR, 'scheduler_config.json')

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


def get_default_config():
    return {
        'max_to_per_ca': 68,  # Sẽ được tự động cập nhật theo số phòng
        'sv_khong_trung_ca': True,
        'ctdt_khong_trung_ngay': True,  # RÀNG BUỘC CỨNG: không thi cùng ngày
        'ctdt_khong_lien_ngay': True,   # RÀNG BUỘC MỀM: penalty thi liền ngày
        'he_so_penalty_lien_ngay': 10,
        'solver_timeout': 120,
        'num_workers': 8
    }


def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return get_default_config()


def save_config(config):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


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


@app.route('/api/config', methods=['GET'])
def get_config():
    """Lấy cấu hình hiện tại"""
    config = load_config()
    return jsonify({'success': True, 'config': config})


@app.route('/api/config', methods=['POST'])
def update_config():
    """Cập nhật cấu hình"""
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'error': 'Không có dữ liệu'})
    
    config = load_config()
    
    # Cập nhật các field
    if 'max_to_per_ca' in data:
        config['max_to_per_ca'] = int(data['max_to_per_ca'])
    if 'sv_khong_trung_ca' in data:
        config['sv_khong_trung_ca'] = bool(data['sv_khong_trung_ca'])
    if 'ctdt_khong_trung_ngay' in data:
        config['ctdt_khong_trung_ngay'] = bool(data['ctdt_khong_trung_ngay'])
    if 'ctdt_khong_lien_ngay' in data:
        config['ctdt_khong_lien_ngay'] = bool(data['ctdt_khong_lien_ngay'])
    if 'he_so_penalty_lien_ngay' in data:
        config['he_so_penalty_lien_ngay'] = int(data['he_so_penalty_lien_ngay'])
    if 'solver_timeout' in data:
        config['solver_timeout'] = int(data['solver_timeout'])
    if 'num_workers' in data:
        config['num_workers'] = int(data['num_workers'])
    
    save_config(config)
    return jsonify({'success': True, 'config': config})


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
    
    # Load config
    config_data = load_config()
    config = SchedulerConfig(
        max_to_per_ca=config_data.get('max_to_per_ca', 68),
        sv_khong_trung_ca=config_data.get('sv_khong_trung_ca', True),
        ctdt_khong_trung_ngay=config_data.get('ctdt_khong_trung_ngay', True),
        ctdt_khong_lien_ngay=config_data.get('ctdt_khong_lien_ngay', True),
        he_so_penalty_lien_ngay=config_data.get('he_so_penalty_lien_ngay', 10),
        solver_timeout=config_data.get('solver_timeout', 120),
        num_workers=config_data.get('num_workers', 8)
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
        
        return jsonify({'success': True, 'message': msg})

    except Exception as e:
        print(f"Error updating schedule: {e}")
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
