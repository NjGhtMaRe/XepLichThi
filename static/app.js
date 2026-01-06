/**
 * XẾP LỊCH THI - Frontend JavaScript
 */

// State
const state = {
    files: {
        lhp: null,
        data: null,
        cfg: null,
        sv: null
    },
    config: {},
    resultFile: null,
    studentFile: null
};

// DOM Elements
const elements = {
    btnSolve: document.getElementById('btn-solve'),
    btnDownload: document.getElementById('btn-download'),
    btnReset: document.getElementById('btn-reset'),
    progressSection: document.getElementById('progress-section'),
    resultSection: document.getElementById('result-section'),
    errorSection: document.getElementById('error-section'),
    resultStats: document.getElementById('result-stats'),
    errorMessage: document.getElementById('error-message'),
    resultsList: document.getElementById('results-list')
};

// =====================================================
// INITIALIZATION
// =====================================================

document.addEventListener('DOMContentLoaded', () => {
    initUploadZones();
    initConfig();
    loadFileStatus();
    loadConfig();
    loadResults();
    loadResults();
    initEventListeners();
    loadScheduleData(); // Load visual data if exists
});

// =====================================================
// FILE UPLOAD
// =====================================================

function initUploadZones() {
    const uploadItems = document.querySelectorAll('.upload-item');

    uploadItems.forEach(item => {
        const type = item.dataset.type;
        const dropzone = item.querySelector('.upload-dropzone');
        const input = item.querySelector('input[type="file"]');

        // Click to upload
        dropzone.addEventListener('click', () => input.click());

        // File input change
        input.addEventListener('change', (e) => {
            if (e.target.files.length > 0) {
                uploadFile(type, e.target.files[0], dropzone);
            }
        });

        // Drag and drop
        dropzone.addEventListener('dragover', (e) => {
            e.preventDefault();
            dropzone.classList.add('dragover');
        });

        dropzone.addEventListener('dragleave', () => {
            dropzone.classList.remove('dragover');
        });

        dropzone.addEventListener('drop', (e) => {
            e.preventDefault();
            dropzone.classList.remove('dragover');

            if (e.dataTransfer.files.length > 0) {
                uploadFile(type, e.dataTransfer.files[0], dropzone);
            }
        });
    });
}

async function uploadFile(type, file, dropzone) {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('type', type);

    const statusEl = dropzone.querySelector('.upload-status');
    statusEl.textContent = 'Đang tải lên...';
    statusEl.style.color = 'var(--accent-secondary)';

    try {
        const response = await fetch('/api/upload', {
            method: 'POST',
            body: formData
        });

        const data = await response.json();

        if (data.success) {
            state.files[type] = data.filename;
            dropzone.classList.add('uploaded');
            statusEl.textContent = `✓ ${formatFileSize(data.size)}`;
            statusEl.style.color = 'var(--success)';
            checkAllFilesUploaded();
        } else {
            statusEl.textContent = `✗ ${data.error}`;
            statusEl.style.color = 'var(--error)';
        }
    } catch (error) {
        statusEl.textContent = '✗ Lỗi kết nối';
        statusEl.style.color = 'var(--error)';
    }
}

async function loadFileStatus() {
    try {
        const response = await fetch('/api/files');
        const data = await response.json();

        if (data.success) {
            Object.entries(data.files).forEach(([type, info]) => {
                if (info) {
                    state.files[type] = info.filename;
                    const dropzone = document.querySelector(`[data-type="${type}"] .upload-dropzone`);
                    if (dropzone) {
                        dropzone.classList.add('uploaded');
                        const statusEl = dropzone.querySelector('.upload-status');
                        statusEl.textContent = `✓ ${formatFileSize(info.size)}`;
                        statusEl.style.color = 'var(--success)';
                    }
                }
            });
            checkAllFilesUploaded();
        }
    } catch (error) {
        console.error('Error loading file status:', error);
    }
}

function checkAllFilesUploaded() {
    const allUploaded = Object.values(state.files).every(f => f !== null);
    elements.btnSolve.disabled = !allUploaded;
}

// =====================================================
// CONFIGURATION
// =====================================================

function initConfig() {
    // Toggle handlers
    document.getElementById('cfg-sv-khong-trung').addEventListener('change', saveConfig);
    document.getElementById('cfg-ctdt-trung-ngay').addEventListener('change', saveConfig);
    document.getElementById('cfg-ctdt-lien-ngay').addEventListener('change', saveConfig);

    // Number input handlers
    document.getElementById('cfg-max-to').addEventListener('change', saveConfig);
    document.getElementById('cfg-penalty').addEventListener('change', saveConfig);
    document.getElementById('cfg-timeout').addEventListener('change', saveConfig);
    document.getElementById('cfg-workers').addEventListener('change', saveConfig);
}

async function loadConfig() {
    try {
        const response = await fetch('/api/config');
        const data = await response.json();

        if (data.success) {
            state.config = data.config;

            document.getElementById('cfg-sv-khong-trung').checked = data.config.sv_khong_trung_ca;
            document.getElementById('cfg-ctdt-trung-ngay').checked = data.config.ctdt_khong_trung_ngay ?? true;
            document.getElementById('cfg-ctdt-lien-ngay').checked = data.config.ctdt_khong_lien_ngay;
            document.getElementById('cfg-max-to').value = data.config.max_to_per_ca;
            document.getElementById('cfg-penalty').value = data.config.he_so_penalty_lien_ngay ?? 10;
            document.getElementById('cfg-timeout').value = data.config.solver_timeout;
            document.getElementById('cfg-workers').value = data.config.num_workers;
        }
    } catch (error) {
        console.error('Error loading config:', error);
    }
}

async function saveConfig() {
    const config = {
        sv_khong_trung_ca: document.getElementById('cfg-sv-khong-trung').checked,
        ctdt_khong_trung_ngay: document.getElementById('cfg-ctdt-trung-ngay').checked,
        ctdt_khong_lien_ngay: document.getElementById('cfg-ctdt-lien-ngay').checked,
        max_to_per_ca: parseInt(document.getElementById('cfg-max-to').value),
        he_so_penalty_lien_ngay: parseInt(document.getElementById('cfg-penalty').value),
        solver_timeout: parseInt(document.getElementById('cfg-timeout').value),
        num_workers: parseInt(document.getElementById('cfg-workers').value)
    };

    try {
        await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config)
        });
        state.config = config;
    } catch (error) {
        console.error('Error saving config:', error);
    }
}

// =====================================================
// SOLVER
// =====================================================

function initEventListeners() {
    elements.btnSolve.addEventListener('click', runSolver);
    elements.btnDownload.addEventListener('click', downloadResult);
    elements.btnReset.addEventListener('click', resetSolver);

    // Student file download
    const btnDownloadSV = document.getElementById('btn-download-sv');
    if (btnDownloadSV) {
        btnDownloadSV.addEventListener('click', downloadStudentFile);
    }
}

async function runSolver() {
    // Show progress
    elements.progressSection.style.display = 'block';
    elements.resultSection.style.display = 'none';
    elements.errorSection.style.display = 'none';
    elements.btnSolve.disabled = true;

    try {
        const response = await fetch('/api/solve', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });

        const data = await response.json();

        elements.progressSection.style.display = 'none';

        if (data.success) {
            state.resultFile = data.result_file;
            state.studentFile = data.student_file;
            showResult(data);
            loadResults();
        } else {
            showError(data.error || 'Không thể xếp lịch');
        }
    } catch (error) {
        elements.progressSection.style.display = 'none';
        showError('Lỗi kết nối server');
    }

    elements.btnSolve.disabled = false;
}

function showResult(data) {
    elements.resultSection.style.display = 'block';

    // Build stats HTML
    const statsHtml = `
        <div class="stat-item">
            <div class="stat-value">${data.data_stats?.so_mon_thi || 0}</div>
            <div class="stat-label">Môn thi</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">${data.data_stats?.so_ngay_thi || 0}</div>
            <div class="stat-label">Ngày thi</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">${data.data_stats?.so_phong_thi || 0}</div>
            <div class="stat-label">Phòng thi</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">${data.data_stats?.so_sinh_vien || 0}</div>
            <div class="stat-label">Sinh viên</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">${data.num_records || 0}</div>
            <div class="stat-label">Tổ thi</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">${data.num_student_records || 0}</div>
            <div class="stat-label">SV được xếp</div>
        </div>
        <div class="stat-item">
            <div class="stat-value">${(data.solver_stats?.solve_time || 0).toFixed(2)}s</div>
            <div class="stat-label">Thời gian</div>
        </div>
        ${data.num_violations > 0 ? `
        <div class="stat-item stat-warning">
            <div class="stat-value">${data.num_violations}</div>
            <div class="stat-label">Vi phạm liền ngày</div>
        </div>
        ` : ''}
    `;

    elements.resultStats.innerHTML = statsHtml;

    // Show/hide student download button
    const btnDownloadSV = document.getElementById('btn-download-sv');
    if (btnDownloadSV && data.student_file) {
        btnDownloadSV.style.display = 'inline-flex';
    }

    // Show result table
    if (data.records && data.records.length > 0) {
        const tableContainer = document.getElementById('result-table-container');
        const tableBody = document.getElementById('result-table-body');
        const tableInfo = document.getElementById('table-info');

        tableContainer.style.display = 'block';
        tableInfo.textContent = `Hiển thị ${Math.min(data.records.length, 500)} / ${data.num_records} dòng`;

        // Build table rows with PhongThi
        const rowsHtml = data.records.map((record, index) => `
            <tr>
                <td>${index + 1}</td>
                <td>${record.MaHP}</td>
                <td>${record.ToThi}</td>
                <td>${formatExamDate(record.Ngay)}</td>
                <td>Ca ${record.Ca}</td>
                <td>${record.PhongThi || '-'}</td>
            </tr>
        `).join('');

        tableBody.innerHTML = rowsHtml;
    }

    // Load visual data automatically
    loadScheduleData();
}

function formatExamDate(dateValue) {
    // Handle different date formats
    if (!dateValue) return '-';

    // If it's a timestamp number (Excel date)
    if (typeof dateValue === 'number') {
        const date = new Date((dateValue - 25569) * 86400 * 1000);
        return date.toLocaleDateString('vi-VN');
    }

    // If it's already a string
    if (typeof dateValue === 'string') {
        // Try to parse as date
        const date = new Date(dateValue);
        if (!isNaN(date.getTime())) {
            return date.toLocaleDateString('vi-VN');
        }
        return dateValue;
    }

    return String(dateValue);
}

function showError(message) {
    elements.errorSection.style.display = 'block';
    elements.errorMessage.textContent = message;
}

function resetSolver() {
    elements.resultSection.style.display = 'none';
    elements.errorSection.style.display = 'none';
}

function downloadResult() {
    if (state.resultFile) {
        window.location.href = `/api/result/${state.resultFile}`;
    }
}

function downloadStudentFile() {
    if (state.studentFile) {
        window.location.href = `/api/result/${state.studentFile}`;
    }
}

// =====================================================
// RESULTS HISTORY
// =====================================================

async function loadResults() {
    try {
        const response = await fetch('/api/results');
        const data = await response.json();

        if (data.success && data.results.length > 0) {
            const html = data.results.map(result => `
                <div class="result-item">
                    <div class="result-item-info">
                        <span class="result-item-name">${result.filename}</span>
                        <span class="result-item-date">${formatDate(result.created)}</span>
                    </div>
                    <button class="btn btn-secondary result-item-btn" onclick="downloadFile('${result.filename}')">
                        📥 Tải
                    </button>
                </div>
            `).join('');

            elements.resultsList.innerHTML = html;
        } else {
            elements.resultsList.innerHTML = '<p class="no-results">Chưa có kết quả nào</p>';
        }
    } catch (error) {
        console.error('Error loading results:', error);
    }
}

function downloadFile(filename) {
    window.location.href = `/api/result/${filename}`;
}

// =====================================================
// UTILITIES
// =====================================================

function formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function formatDate(isoString) {
    const date = new Date(isoString);
    return date.toLocaleString('vi-VN', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
}


// ===================================
// VISUAL SCHEDULE EDITOR
// ===================================

let visualState = {
    selectedSlot: null, // { ngay, ca, phong, item: { ... } }
};

async function loadScheduleData() {
    try {
        const res = await fetch('/api/schedule/data');
        const data = await res.json();

        if (data.success) {
            initVisualSchedule(data);
        } else {
            console.log("Chưa có dữ liệu lịch: " + data.error);
        }
    } catch (e) {
        console.error(e);
    }
}

function initVisualSchedule(data) {
    if (!data.schedule) return;

    // Save data to state
    state.scheduleData = data;

    const section = document.getElementById('visual-section');
    section.style.display = 'block';

    // Init Date Select if empty or update it
    const dateSelect = document.getElementById('visual-date-select');
    const currentDate = dateSelect.value;

    dateSelect.innerHTML = '<option value="">-- Chọn ngày --</option>';

    // Sort dates properly? Handled by backend (sorted) or do it here
    const sortedDays = data.days; // Assuming sorted from backend

    sortedDays.forEach(day => {
        const opt = document.createElement('option');
        opt.value = day;
        opt.textContent = day;
        if (day === currentDate) opt.selected = true;
        dateSelect.appendChild(opt);
    });

    // Event Listeners (ensure only added once)
    if (!dateSelect.hasAttribute('data-init')) {
        dateSelect.addEventListener('change', renderScheduleGrid);
        document.getElementById('btn-refresh-visual').addEventListener('click', () => loadScheduleData());
        dateSelect.setAttribute('data-init', 'true');
    }

    // If a date was selected, re-render
    if (currentDate) {
        renderScheduleGrid();
    }
}

function renderScheduleGrid() {
    const date = document.getElementById('visual-date-select').value;
    const container = document.getElementById('schedule-grid');

    if (!date) {
        container.innerHTML = '<p class="text-center text-muted">Vui lòng chọn ngày để xem lịch.</p>';
        return;
    }

    const { rooms, shifts, schedule } = state.scheduleData;

    // Filter items for selected date
    const itemsOnDate = schedule.filter(item => item.Ngay === date);

    // Layout: 4 Columns (Shift 1, 2, 3, 4)
    let html = '<div class="shifts-container" style="display:flex; gap:15px; min-width: 1200px;">';

    shifts.forEach(shift => {
        html += `<div class="shift-column" style="flex:1; border:1px solid #ddd; background:#fff; border-radius:4px;">
                    <div class="shift-header" style="background:#f8f9fa; padding:10px; font-weight:bold; text-align:center; border-bottom:1px solid #ddd; position:sticky; top:0; z-index:10;">
                        CA ${shift}
                    </div>
                    <div class="shift-body" style="padding:5px; max-height:600px; overflow-y:auto;">`;

        rooms.forEach((room) => {
            // Find item in this slot
            const item = itemsOnDate.find(i => i.Ca == shift && i.PhongThi == room);
            const isOccupied = !!item;
            const isSelected = visualState.selectedSlot &&
                visualState.selectedSlot.ngay === date &&
                visualState.selectedSlot.ca === shift &&
                visualState.selectedSlot.phong === room;

            const bgClass = isSelected ? 'selected' : (isOccupied ? 'occupied' : 'empty');
            const bgColor = isSelected ? '#fff3cd' : (isOccupied ? '#e3f2fd' : '#f5f5f5');
            const borderColor = isSelected ? '#ffc107' : (isOccupied ? '#2196f3' : '#ddd');

            // Escape params for onclick
            const safeDate = date.replace(/'/g, "\\'");
            const safeRoom = room.replace(/'/g, "\\'");

            html += `<div class="room-slot ${bgClass}" 
                          onclick="handleSlotClick('${safeDate}', ${shift}, '${safeRoom}')"
                          style="background:${bgColor}; border:1px solid ${borderColor}; padding:8px; margin-bottom:5px; cursor:pointer; border-radius:3px; position:relative; min-height:50px; transition: all 0.2s;">
                        <div class="room-name" style="font-weight:bold; font-size:0.85em; color:#666; margin-bottom:4px;">${room}</div>
                        ${isOccupied ? `
                            <div class="slot-content">
                                <div style="color:#0d47a1; font-weight:bold;">${item.MaHP}</div>
                                <div style="font-size:0.8em;">Tổ: ${item.ToThi}</div>
                            </div>
                        ` : ''}
                     </div>`;
        });

        html += `   </div>
                 </div>`;
    });

    html += '</div>';
    container.innerHTML = html;
}

window.handleSlotClick = async function (ngay, ca, phong) {
    const { schedule } = state.scheduleData;
    const item = schedule.find(i => i.Ngay === ngay && i.Ca == ca && i.PhongThi === phong);

    // Action Logic
    if (!visualState.selectedSlot) {
        // Selection Mode - Only select if occupied
        if (item) {
            visualState.selectedSlot = { ngay, ca, phong, item };
            renderScheduleGrid(); // Re-render to show selection
        }
    } else {
        // Target Mode
        const source = visualState.selectedSlot;

        // Click same slot -> Deselect
        if (source.ngay === ngay && source.ca === ca && source.phong === phong) {
            visualState.selectedSlot = null;
            renderScheduleGrid();
            return;
        }

        // Perform Action (Move or Swap)
        const isSwap = !!item;

        const confirmMsg = isSwap ?
            `Bạn có muốn ĐỔI LỊCH giữa [${source.item.MaHP}-T${source.item.ToThi}] và [${item.MaHP}-T${item.ToThi}]?` :
            `Bạn có muốn CHUYỂN [${source.item.MaHP}-T${source.item.ToThi}] sang phòng ${phong} (Ca ${ca})?`;

        if (confirm(confirmMsg)) {
            // Call API
            const payload = {
                action: isSwap ? 'swap' : 'move',
                source: visualState.selectedSlot.item,
                target: { Ngay: ngay, Ca: ca, PhongThi: phong }
            };

            try {
                const res = await fetch('/api/schedule/update', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });

                const resData = await res.json();

                if (resData.success) {
                    alert(resData.message);
                    visualState.selectedSlot = null;
                    await loadScheduleData(); // Reload data
                    // renderScheduleGrid will be called by initVisualSchedule if date matches
                } else {
                    alert('Lỗi: ' + resData.error);
                }
            } catch (e) {
                alert('Lỗi kết nối: ' + e);
            }
        } else {
            // If clicked on another slot but user cancelled or just wanted to change selection?
            // Simplify: always deselect if action cancelled
            visualState.selectedSlot = null;
            renderScheduleGrid();
        }
    }
};
