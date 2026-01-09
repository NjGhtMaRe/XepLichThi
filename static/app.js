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
    selectedSlots: [], // Array of { ngay, ca, phong, item }
};

// Helper: Check if a slot is selected
function isSlotSelected(ngay, ca, phong) {
    return visualState.selectedSlots.some(s =>
        s.ngay === ngay && s.ca === ca && s.phong === phong
    );
}

// Helper: Toggle slot selection
function toggleSlotSelection(ngay, ca, phong, item) {
    const idx = visualState.selectedSlots.findIndex(s =>
        s.ngay === ngay && s.ca === ca && s.phong === phong
    );
    if (idx >= 0) {
        visualState.selectedSlots.splice(idx, 1);
    } else if (item) {
        visualState.selectedSlots.push({ ngay, ca, phong, item });
    }
    updateSelectionCount();
}

// Helper: Clear all selections
function clearAllSelections() {
    visualState.selectedSlots = [];
    updateSelectionCount();
    renderScheduleGrid();
}

// Helper: Update selection count display
function updateSelectionCount() {
    const countEl = document.getElementById('selection-count');
    if (countEl) {
        countEl.textContent = `${visualState.selectedSlots.length} selected`;
    }
    // Enable/disable batch move button
    const btnBatch = document.getElementById('btn-batch-move');
    if (btnBatch) {
        btnBatch.disabled = visualState.selectedSlots.length === 0;
    }
}

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

    dateSelect.innerHTML = '<option value="">-- Select Day --</option>';

    // Populate both view date and target date dropdowns
    const sortedDays = data.days;
    const targetDaySelect = document.getElementById('target-day-select');
    targetDaySelect.innerHTML = '<option value="">-- Select Day --</option>';

    sortedDays.forEach(day => {
        // View day select
        const opt = document.createElement('option');
        opt.value = day;
        opt.textContent = day;
        if (day === currentDate) opt.selected = true;
        dateSelect.appendChild(opt);

        // Target day select
        const opt2 = document.createElement('option');
        opt2.value = day;
        opt2.textContent = day;
        targetDaySelect.appendChild(opt2);
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
        container.innerHTML = '<p class="text-center text-muted">Please select a date to view schedule.</p>';
        return;
    }

    // PRESERVE SCROLL POSITIONS before re-rendering
    const wrapper = document.querySelector('.schedule-grid-wrapper');
    const wrapperScrollLeft = wrapper ? wrapper.scrollLeft : 0;

    // Save scroll positions of each shift body
    const shiftScrollTops = [];
    document.querySelectorAll('.shift-body').forEach((el, idx) => {
        shiftScrollTops[idx] = el.scrollTop;
    });

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
            const isSelected = isSlotSelected(date, shift, room);

            const bgColor = isSelected ? '#c8e6c9' : (isOccupied ? '#e3f2fd' : '#f5f5f5');
            const borderColor = isSelected ? '#4caf50' : (isOccupied ? '#2196f3' : '#ddd');

            // Escape params for onclick
            const safeDate = date.replace(/'/g, "\\'");
            const safeRoom = room.replace(/'/g, "\\'");

            html += `<div class="room-slot" 
                          onclick="handleSlotClick('${safeDate}', ${shift}, '${safeRoom}')"
                          style="background:${bgColor}; border:2px solid ${borderColor}; padding:8px; margin-bottom:5px; cursor:pointer; border-radius:3px; position:relative; min-height:50px; transition: all 0.2s;">
                        <div class="room-name" style="font-weight:bold; font-size:0.85em; color:#666; margin-bottom:4px;">${room}</div>
                        ${isOccupied ? `
                            <div class="slot-content">
                                <div style="color:#0d47a1; font-weight:bold;">${item.MaHP}</div>
                                <div style="font-size:0.8em;">To: ${item.ToThi}</div>
                            </div>
                            ${isSelected ? '<span style="position:absolute;top:5px;right:5px;color:#4caf50;font-size:1.2em;">&#10003;</span>' : ''}
                        ` : ''}
                     </div>`;
        });

        html += `   </div>
                 </div>`;
    });

    html += '</div>';
    container.innerHTML = html;

    // RESTORE SCROLL POSITIONS after rendering
    if (wrapper) {
        wrapper.scrollLeft = wrapperScrollLeft;
    }
    document.querySelectorAll('.shift-body').forEach((el, idx) => {
        if (shiftScrollTops[idx] !== undefined) {
            el.scrollTop = shiftScrollTops[idx];
        }
    });
}

window.handleSlotClick = function (ngay, ca, phong) {
    const { schedule } = state.scheduleData;
    const item = schedule.find(i => i.Ngay === ngay && i.Ca == ca && i.PhongThi === phong);

    // Only allow selecting occupied slots
    if (item) {
        toggleSlotSelection(ngay, ca, phong, item);
        renderScheduleGrid();
    }
};

// Batch move selected items to target day/shift
window.batchMoveSelected = async function (forceMove = false) {
    if (visualState.selectedSlots.length === 0) {
        alert('Please select at least one exam group first.');
        return;
    }

    const targetDay = document.getElementById('target-day-select').value;
    const targetShift = document.getElementById('target-shift-select').value;

    if (!targetDay || !targetShift) {
        alert('Please select target day and shift.');
        return;
    }

    const items = visualState.selectedSlots.map(s => ({
        MaHP: s.item.MaHP,
        ToThi: s.item.ToThi
    }));

    if (!forceMove) {
        const confirmMsg = `Move ${items.length} exam group(s) to ${targetDay} - Ca ${targetShift}?`;
        if (!confirm(confirmMsg)) return;
    }

    try {
        const res = await fetch('/api/schedule/batch-update', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                items: items,
                target: { Ngay: targetDay, Ca: parseInt(targetShift) },
                force_move: forceMove
            })
        });

        const resData = await res.json();

        if (resData.success) {
            alert(resData.message);
            clearAllSelections();
            await loadScheduleData();
        } else {
            // Handle different error types
            if (resData.error_type === 'CONFLICT_SHIFT') {
                // HARD BLOCK - cannot proceed
                let detailsStr = '';
                if (resData.conflict_details) {
                    detailsStr = resData.conflict_details.map(c =>
                        `SV: ${c.MaSV} | Moving: ${c.moving_MaHP} T${c.moving_ToThi} -> Conflict: ${c.conflict_MaHP} T${c.conflict_ToThi}`
                    ).join('\n');
                }
                alert(`BLOCKED: ${resData.error}\n\nConflict Details:\n${detailsStr}\n\nYou cannot move these exams to the same shift.`);
            } else if (resData.error_type === 'WARNING_SAME_DAY' && resData.can_force) {
                // SOFT WARNING - can proceed with confirmation
                let detailsStr = '';
                if (resData.conflict_details) {
                    detailsStr = resData.conflict_details.map(c =>
                        `SV: ${c.MaSV} | Moving: ${c.moving_MaHP} T${c.moving_ToThi} -> Same-day: ${c.conflict_MaHP} T${c.conflict_ToThi} Ca${c.conflict_Ca}`
                    ).join('\n');
                }
                const proceed = confirm(`WARNING: ${resData.error}\n\nConflict Details:\n${detailsStr}\n\nDo you want to proceed anyway?`);
                if (proceed) {
                    await batchMoveSelected(true);
                }
            } else {
                alert('Error: ' + resData.error);
            }
        }
    } catch (e) {
        alert('Connection error: ' + e);
    }
};

// Expose clearAllSelections globally
window.clearAllSelections = clearAllSelections;

// =====================================================
// EXPORT STUDENTS
// =====================================================
async function exportStudents() {
    try {
        const btn = document.querySelector('[onclick="exportStudents()"]');
        if (btn) {
            btn.disabled = true;
            btn.innerHTML = '⏳ Đang xuất...';
        }

        const res = await fetch('/api/export-students', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });

        const data = await res.json();

        if (data.success) {
            alert(`✅ ${data.message}\n\nFile: ${data.filename}`);
            // Trigger download
            window.location.href = `/download/${data.filename}`;
        } else {
            alert('❌ Lỗi: ' + data.error);
        }
    } catch (e) {
        alert('❌ Connection error: ' + e);
    } finally {
        const btn = document.querySelector('[onclick="exportStudents()"]');
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '📥 Xuất DS Sinh Viên';
        }
    }
}

window.exportStudents = exportStudents;
