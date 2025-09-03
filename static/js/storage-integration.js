// Storage Integration Functions with ALL FIXES APPLIED

// Global storage manager instance
let storageManager = null;

// FIX 4: Track if data came from storage to prevent re-saving
window.dataFromStorage = false;

// Initialize storage manager when DOM is ready
document.addEventListener('DOMContentLoaded', async function() {
    try {
        console.log('Initializing storage manager...');
        
        // Create and initialize storage manager
        storageManager = new LocationStorageManager();
        await storageManager.init();  // CRITICAL: Must await this!
        
        // Make it globally available
        window.storageManager = storageManager;
        
        console.log('Storage manager initialized successfully, database ready:', storageManager.db);
        
        // Check for existing data
        try {
            const sources = await storageManager.getAvailableDataSources();
            console.log('Found existing data sources:', sources);
            
            if (sources.originals.length > 0 || sources.subsets.length > 0) {
                // Show the quick access panel if data exists
                showQuickAccessPanel();
                await refreshDataSourcesList();
            }
        } catch (e) {
            console.log('No existing data found (this is normal for first use)');
        }
        
    } catch (error) {
        console.error('Failed to initialize storage manager:', error);
        // Continue without storage - app can still work
    }
});

async function refreshDataSourcesList() {
    try {
        // Check if storage manager is ready
        if (!storageManager || !storageManager.db) {
            console.warn('Storage manager not ready yet');
            return;
        }
        
        const sources = await storageManager.getAvailableDataSources();
        
        // Update recent subsets in quick access
        updateRecentSubsets(sources.subsets);
        
        // Update original files list - FIX 3: Better layout
        updateOriginalsList(sources.originals);
        
        // Update subsets list
        updateSubsetsList(sources.subsets);
        
        // Update merge options
        updateMergeOptions(sources.originals);
        
        // Update storage info
        updateStorageInfo();
        
    } catch (error) {
        console.error('Error refreshing data sources:', error);
    }
}

function extractDateRangeFromMetadata(file) {
    let dateRange = null;
    
    // Try multiple places where dates might be stored
    if (file.metadata) {
        // Try these in order of preference
        dateRange = file.metadata.dateRange || 
                   file.metadata.parse_dates_used || 
                   file.metadata.parseDates ||
                   null;
    }
    
    // If still no dates, try to parse from filename
    if (!dateRange && file.filename) {
        const match = file.filename.match(/(\d{4}-\d{2}-\d{2}).*?(\d{4}-\d{2}-\d{2})/);
        if (match) {
            dateRange = { 
                start: match[1], 
                end: match[2] 
            };
        }
    }
    
    // Normalize the format (could have 'from/to' or 'start/end')
    if (dateRange) {
        return {
            start: dateRange.start || dateRange.from,
            end: dateRange.end || dateRange.to
        };
    }
    
    return dateRange;
}

function updateRecentSubsets(subsets) {
    const recentDiv = document.getElementById('recentSubsets');
    if (!recentDiv) return;
    
    // Sort by last used and take top 3
    const recent = subsets
        .sort((a, b) => new Date(b.lastUsed) - new Date(a.lastUsed))
        .slice(0, 3);
    
    recentDiv.innerHTML = '';
    
    if (recent.length === 0) {
        recentDiv.innerHTML = '<p class="no-data">No saved date ranges yet</p>';
        return;
    }
    
    recent.forEach(subset => {
        const item = document.createElement('div');
        item.className = 'recent-item';
        
        // FIX 2: Display proper date range instead of "Unknown - Unknown"
        const dateRange = subset.dateRange || {};
        const startDate = dateRange.start ? new Date(dateRange.start).toLocaleDateString() : subset.startDate || 'Unknown';
        const endDate = dateRange.end ? new Date(dateRange.end).toLocaleDateString() : subset.endDate || 'Unknown';
        
        item.innerHTML = `
            <div class="recent-info">
                <strong>${subset.name}</strong>
                <small>${startDate} to ${endDate}</small>
                <small>${subset.stats.totalPoints} locations</small>
            </div>
            <button onclick="quickLoadSubset('${subset.id}')" class="quick-load-btn">
                Load
            </button>
        `;
        recentDiv.appendChild(item);
    });
}

// Updated updateOriginalsList to properly show master and parsed files
function updateOriginalsList(originals) {
    const listDiv = document.getElementById('originalFilesList');
    if (!listDiv) return;
    
    listDiv.innerHTML = '';

    // Handle both formats - if passed sources object or just originals array
    let filesList = originals;
    if (originals && originals.originals) {
        filesList = originals.originals;
    }

    if (!filesList || !Array.isArray(filesList)) {
        listDiv.innerHTML = '<p class="no-data">No files loaded</p>';
        return;
    }

    // First, find the MASTER file (largest, oldest, or specifically marked)
    let masterFile = null;
    let parsedFiles = [];
    
    filesList.forEach(file => {
        // Check if it's marked as master or if it's a raw Google file
        const hasMetadataTag = file.metadata && file.metadata.isParsed;
        const isParsedFile = file.filename.includes('parsed_') || hasMetadataTag;
        
        if (!isParsedFile) {
            // This is likely the master - typically the largest file without 'parsed' in name
            if (!masterFile || file.size > masterFile.size) {
                masterFile = file;
            }
        } else {
            parsedFiles.push(file);
        }
    });
    
    // If no master identified yet, use the largest file
    if (!masterFile && sources.originals.length > 0) {
        masterFile = sources.originals.reduce((prev, current) => 
            (prev.size > current.size) ? prev : current
        );
        // Remove from parsed files if it was there
        parsedFiles = parsedFiles.filter(f => f.id !== masterFile.id);
    }
    
    // Display Master File Section
    if (masterFile) {
        const masterSection = document.createElement('div');
        masterSection.style.cssText = `
            background: #f8f9fa;
            border: 2px solid #dc3545;
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 20px;
        `;
                
        masterSection.innerHTML = `
            <div style="display: flex; align-items: center; margin-bottom: 10px;">
                <h4 style="margin: 0; color: #dc3545;">Master File Loaded: ${masterFile.filename}</h4>
                <button onclick="parseFromMaster('${masterFile.id}')" style="margin-left: auto; padding: 5px 10px; background: #28a745; color: white; border: none; border-radius: 4px; cursor: pointer;">Parse New Range</button>
                <button onclick="removeMasterFile('${masterFile.id}')" style="margin-left: 10px; padding: 5px 10px; background: #dc3545; color: white; border: none; border-radius: 4px; cursor: pointer;">Remove</button>
                <button onclick="replaceMasterFile()" style="margin-left: 10px; padding: 5px 10px; background: #6c757d; color: white; border: none; border-radius: 4px; cursor: pointer;">Replace</button>
            </div>
            <div style="color: #666; font-size: 14px;">
                Date Range: ${formatDateRange(masterFile.dateRange || masterFile.metadata?.dateRange)}
                <br>Size: ${formatFileSize(masterFile.size)}
            </div>
        `;
        
        listDiv.appendChild(masterSection);
        
        // Display Parsed Files Section
        if (parsedFiles.length > 0) {
            const parsedSection = document.createElement('div');
            parsedSection.innerHTML = '<h4 style="margin: 20px 0 10px 0;">Parsed Files</h4>';
            
            // Create table for parsed files
            const table = document.createElement('table');
            table.style.cssText = 'width: 100%; border-collapse: collapse;';
            table.innerHTML = `
                <thead>
                    <tr style="background: #f8f9fa;">
                        <th style="padding: 8px; text-align: left; border: 1px solid #dee2e6;">Select</th>
                        <th style="padding: 8px; text-align: left; border: 1px solid #dee2e6;">Name</th>
                        <th style="padding: 8px; text-align: left; border: 1px solid #dee2e6;">From</th>
                        <th style="padding: 8px; text-align: left; border: 1px solid #dee2e6;">To</th>
                        <th style="padding: 8px; text-align: left; border: 1px solid #dee2e6;">Size</th>
                        <th style="padding: 8px; text-align: left; border: 1px solid #dee2e6;">Activities</th>
                        <th style="padding: 8px; text-align: left; border: 1px solid #dee2e6;">Visits</th>
                    </tr>
                </thead>
                <tbody>
            `;
            
            parsedFiles.forEach(file => {
                const dateRange = extractDateRangeFromMetadata(file);
                const stats = file.metadata?.stats || {};
                
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td style="padding: 8px; border: 1px solid #dee2e6;">
                        <input type="checkbox" value="${file.id}" class="parsed-file-checkbox">
                    </td>
                    <td style="padding: 8px; border: 1px solid #dee2e6;">${file.filename.replace('parsed_', '').replace('.json', '')}</td>
                    <td style="padding: 8px; border: 1px solid #dee2e6;">${dateRange?.start ? new Date(dateRange.start).toLocaleDateString() : '-'}</td>
                    <td style="padding: 8px; border: 1px solid #dee2e6;">${dateRange?.end ? new Date(dateRange.end).toLocaleDateString() : '-'}</td>
                    <td style="padding: 8px; border: 1px solid #dee2e6;">${formatFileSize(file.size)}</td>
                    <td style="padding: 8px; border: 1px solid #dee2e6; text-align: center;">${stats.activities || '-'}</td>
                    <td style="padding: 8px; border: 1px solid #dee2e6; text-align: center;">${stats.visits || '-'}</td>
                `;
                table.querySelector('tbody').appendChild(row);
            });
            
            table.innerHTML += '</tbody>';
            parsedSection.appendChild(table);
            
            // Add action buttons
            const actions = document.createElement('div');
            actions.style.cssText = 'margin-top: 15px; display: flex; gap: 10px;';
            actions.innerHTML = `
                <button onclick="processSelectedParsedFile()" style="padding: 8px 16px; background: #28a745; color: white; border: none; border-radius: 4px; cursor: pointer;">Process Selected</button>
                <button onclick="deleteSelectedParsedFiles()" style="padding: 8px 16px; background: #dc3545; color: white; border: none; border-radius: 4px; cursor: pointer;">Delete Selected</button>
            `;
            parsedSection.appendChild(actions);
            
            listDiv.appendChild(parsedSection);
        }
    } else {
        // No files yet
        listDiv.innerHTML = `
            <div style="padding: 20px; background: #fff3cd; border: 1px solid #ffc107; border-radius: 5px; text-align: center;">
                <p style="margin: 0; color: #856404;">No location data files loaded yet.</p>
                <p style="margin: 10px 0 0 0; color: #856404;">Upload your Google location-history.json file to get started.</p>
            </div>
        `;
    }
}

function updateSubsetsList(subsets) {
    const listDiv = document.getElementById('subsetsList');
    if (!listDiv) return;
    
    listDiv.innerHTML = '';
    
    if (!subsets || subsets.length === 0) {
        listDiv.innerHTML = '<p class="no-data">No saved date ranges</p>';
        return;
    }
    
    // Add header for parsed ranges
    listDiv.innerHTML = '<h4 style="color: #666; margin-bottom: 15px;">📊 Parsed Date Ranges</h4>';
    
    // Group by original file
    const grouped = {};
    subsets.forEach(subset => {
        if (!grouped[subset.originalId]) {
            grouped[subset.originalId] = [];
        }
        grouped[subset.originalId].push(subset);
    });
    
    Object.entries(grouped).forEach(([originalId, subs]) => {
        const group = document.createElement('div');
        group.className = 'subset-group';
        
        subs.forEach(subset => {
            const item = document.createElement('div');
            item.className = 'data-item';
            
            const dateRange = subset.dateRange || {};
            const startDate = dateRange.start ? new Date(dateRange.start).toLocaleDateString() : 
                             (subset.startDate ? new Date(subset.startDate).toLocaleDateString() : 'Unknown');
            const endDate = dateRange.end ? new Date(dateRange.end).toLocaleDateString() : 
                           (subset.endDate ? new Date(subset.endDate).toLocaleDateString() : 'Unknown');
            
            const displayName = subset.name.includes('__') ? subset.name : `${startDate} to ${endDate}`;
            
            item.innerHTML = `
                <div class="item-info">
                    <h4>${displayName}</h4>
                    <p>Date Range: ${startDate} - ${endDate}</p>
                    <p>Created: ${new Date(subset.createdAt).toLocaleDateString()}</p>
                    <p>Last used: ${new Date(subset.lastUsed).toLocaleDateString()}</p>
                    <p>Points: ${subset.stats.totalPoints}</p>
                </div>
                <div class="item-actions">
                    <button onclick="loadSubsetData('${subset.id}')" class="btn-primary">
                        Load & Analyze
                    </button>
                    <button onclick="exportSubset('${subset.id}')" class="btn-secondary">
                        Export
                    </button>
                    <button onclick="deleteSubsetData('${subset.id}')" class="btn-danger">
                        Delete
                    </button>
                </div>
            `;
            group.appendChild(item);
        });
        
        listDiv.appendChild(group);
    });
}

// Helper function to format date range
function formatDateRange(dateRange) {
    if (!dateRange) return 'Unknown';
    const start = dateRange.start || dateRange.from;
    const end = dateRange.end || dateRange.to;
    if (!start || !end) return 'Unknown';
    return `${new Date(start).toLocaleDateString()} to ${new Date(end).toLocaleDateString()}`;
}

// Process selected parsed file (for analysis only, no new file creation)
async function processSelectedParsedFile() {
    const checkboxes = document.querySelectorAll('.parsed-file-checkbox:checked');
    if (checkboxes.length !== 1) {
        alert('Please select exactly one parsed file to process');
        return;
    }
    
    const fileId = checkboxes[0].value;
    
    try {
        // Mark that this is a parsed file being loaded for analysis
        window.dataFromStorage = true;
        window.isLoadingParsedFile = true;
        
        const original = await storageManager.loadOriginal(fileId);
        if (original) {
            // Close modal
            closeDataManager();
            
            // This is definitely a parsed file - send directly for analysis
            const blob = new Blob([JSON.stringify(original.data)], { type: 'application/json' });
            const file = new File([blob], original.metadata.filename, { type: 'application/json' });
            
            const formData = new FormData();
            formData.append('file', file);
            
            const response = await fetch('/upload_parsed', {
                method: 'POST',
                body: formData
            });
            
            const result = await response.json();
            if (result.task_id) {
                window.currentTaskId = result.task_id;
                
                // Move directly to analysis step
                document.getElementById('analyze-btn').disabled = false;
                
                // Auto-populate dates from the parsed file's metadata
                if (original.data._metadata && original.data._metadata.dateRange) {
                    const dateRange = original.data._metadata.dateRange;
                    document.querySelector('input[name="start_date"]').value = dateRange.from || '';
                    document.querySelector('input[name="end_date"]').value = dateRange.to || '';
                }
                
                if (typeof moveToStep === 'function') {
                    moveToStep(2);
                }
                
                showNotification(`Loaded parsed file for analysis: ${original.metadata.filename}`);
            }
        }
    } catch (error) {
        console.error('Error loading parsed file:', error);
        showNotification('Error loading file', 'error');
    } finally {
        setTimeout(() => {
            window.dataFromStorage = false;
            window.isLoadingParsedFile = false;
        }, 1000);
    }
}
// Add this function to storage-integration.js
async function parseFromMaster(masterId) {
    try {
        // Load the master file
        const master = await storageManager.loadOriginal(masterId);
        if (!master) return;
        
        // Close the modal
        closeDataManager();
        
        // Load it into the parse form
        const blob = new Blob([JSON.stringify(master.data)], { type: 'application/json' });
        const file = new File([blob], master.metadata.filename, { type: 'application/json' });
        
        // Set the file in the input
        const dataTransfer = new DataTransfer();
        dataTransfer.items.add(file);
        const fileInput = document.getElementById('raw-file');
        if (fileInput) {
            fileInput.files = dataTransfer.files;
            
            // Update file info display
            const fileInfo = document.getElementById('file-info');
            if (fileInfo) {
                fileInfo.textContent = `Selected: ${file.name} (${(file.size / 1024 / 1024).toFixed(1)} MB)`;
            }
            
            // Store filename for later
            parsedFileName = file.name;
            
            // Move to parsing step
            if (typeof moveToStep === 'function') {
                moveToStep(1);
            }
            
            showNotification('Master file loaded. Configure parsing settings and click Parse.');
        }
    } catch (error) {
        console.error('Error loading master for parsing:', error);
        showNotification('Error loading master file', 'error');
    }
}

// Add this function to storage-integration.js
async function parseFromMaster(masterId) {
    try {
        // Load the master file
        const master = await storageManager.loadOriginal(masterId);
        if (!master) return;
        
        // Close the modal
        closeDataManager();
        
        // Load it into the parse form
        const blob = new Blob([JSON.stringify(master.data)], { type: 'application/json' });
        const file = new File([blob], master.metadata.filename, { type: 'application/json' });
        
        // Set the file in the input
        const dataTransfer = new DataTransfer();
        dataTransfer.items.add(file);
        const fileInput = document.getElementById('raw-file');
        if (fileInput) {
            fileInput.files = dataTransfer.files;
            
            // Update file info display
            const fileInfo = document.getElementById('file-info');
            if (fileInfo) {
                fileInfo.textContent = `Selected: ${file.name} (${(file.size / 1024 / 1024).toFixed(1)} MB)`;
            }
            
            // Store filename for later
            parsedFileName = file.name;
            
            // Move to parsing step
            if (typeof moveToStep === 'function') {
                moveToStep(1);
            }
            
            showNotification('Master file loaded. Configure parsing settings and click Parse.');
        }
    } catch (error) {
        console.error('Error loading master for parsing:', error);
        showNotification('Error loading master file', 'error');
    }
}

async function processSelectedParsedFile() {
    const checkboxes = document.querySelectorAll('.parsed-file-checkbox:checked');
    if (checkboxes.length !== 1) {
        alert('Please select exactly one parsed file to process');
        return;
    }
    
    const fileId = checkboxes[0].value;
    
    try {
        window.dataFromStorage = true;
        window.isLoadingParsedFile = true;
        
        const original = await storageManager.loadOriginal(fileId);
        if (original) {
            closeDataManager();
            
            const blob = new Blob([JSON.stringify(original.data)], { type: 'application/json' });
            const file = new File([blob], original.metadata.filename, { type: 'application/json' });
            
            const formData = new FormData();
            formData.append('file', file);
            
            const response = await fetch('/upload_parsed', {
                method: 'POST',
                body: formData
            });
            
            const result = await response.json();
            if (result.task_id) {
                window.currentTaskId = result.task_id;
                document.getElementById('analyze-btn').disabled = false;
                
                if (original.data._metadata && original.data._metadata.dateRange) {
                    const dateRange = original.data._metadata.dateRange;
                    document.querySelector('input[name="start_date"]').value = dateRange.from || '';
                    document.querySelector('input[name="end_date"]').value = dateRange.to || '';
                }
                
                if (typeof moveToStep === 'function') {
                    moveToStep(2);
                }
                
                showNotification(`Loaded parsed file for analysis: ${original.metadata.filename}`);
            }
        }
    } catch (error) {
        console.error('Error loading parsed file:', error);
        showNotification('Error loading file', 'error');
    } finally {
        setTimeout(() => {
            window.dataFromStorage = false;
            window.isLoadingParsedFile = false;
        }, 1000);
    }
}

async function deleteSelectedParsedFiles() {
    const checkboxes = document.querySelectorAll('.parsed-file-checkbox:checked');
    if (checkboxes.length === 0) {
        alert('Please select files to delete');
        return;
    }
    
    if (!confirm(`Delete ${checkboxes.length} selected parsed file(s)?`)) {
        return;
    }
    
    for (const checkbox of checkboxes) {
        await storageManager.deleteOriginal(checkbox.value);
    }
    
    await refreshDataSourcesList();
    showNotification(`Deleted ${checkboxes.length} file(s)`);
}


// Delete selected parsed files
async function deleteSelectedParsedFiles() {
    const checkboxes = document.querySelectorAll('.parsed-file-checkbox:checked');
    if (checkboxes.length === 0) {
        alert('Please select files to delete');
        return;
    }
    
    if (!confirm(`Delete ${checkboxes.length} selected parsed file(s)?`)) {
        return;
    }
    
    for (const checkbox of checkboxes) {
        await storageManager.deleteOriginal(checkbox.value);
    }
    
    await refreshDataSourcesList();
    showNotification(`Deleted ${checkboxes.length} file(s)`);
}

function updateMergeOptions(originals) {
    // This function will be called but we can leave it empty for now
}

// Action functions
async function quickLoadSubset(subsetId) {
    try {
        const subset = await storageManager.loadSubset(subsetId);
        if (subset) {
            // Close any modals
            closeDataManager();
            
            // Send to your existing processing pipeline
            await processLoadedSubset(subset);
            
            showNotification(`Loaded: ${subset.metadata.name}`);
        }
    } catch (error) {
        console.error('Error loading subset:', error);
        showNotification('Error loading saved data', 'error');
    }
}

async function loadOriginalFile(originalId) {
    try {
        window.dataFromStorage = true;
        
        const original = await storageManager.loadOriginal(originalId);
        if (original) {
            // Close modal
            closeDataManager();
            
            // Check if data is already parsed (has _metadata with isParsed flag)
            const isParsed = original.data && 
                           original.data._metadata && 
                           original.data._metadata.isParsed === true;
            
            if (isParsed) {
                // This is a parsed file - send it directly to analyze step
                console.log('Loading pre-parsed file for analysis');
                
                // Convert to blob and file for upload
                const blob = new Blob([JSON.stringify(original.data)], { type: 'application/json' });
                const file = new File([blob], original.metadata.filename, { type: 'application/json' });
                
                // Upload as parsed file
                const formData = new FormData();
                formData.append('file', file);
                
                const response = await fetch('/upload_parsed', {
                    method: 'POST',
                    body: formData
                });
                
                const result = await response.json();
                if (result.task_id) {
                    window.currentTaskId = result.task_id;
                    currentTaskId = result.task_id;
                    
                    // If it's already parsed, we should be ready for analysis
                    if (result.is_parsed) {
                        document.getElementById('analyze-btn').disabled = false;
                        
                        // Extract and populate date range if available
                        if (original.data._metadata && original.data._metadata.dateRange) {
                            const dateRange = original.data._metadata.dateRange;
                            document.querySelector('input[name="start_date"]').value = dateRange.from || dateRange.start || '';
                            document.querySelector('input[name="end_date"]').value = dateRange.to || dateRange.end || '';
                        }
                        
                        if (typeof moveToStep === 'function') {
                            moveToStep(2);
                        }
                        
                        showNotification(`Loaded parsed file: ${original.metadata.filename}`);
                    } else {
                        // This shouldn't happen but handle it
                        showNotification('File needs parsing first');
                    }
                }
            } else {
                // This is a raw file that needs parsing
                console.log('Loading raw file for parsing');
                
                // Convert to blob and file
                const blob = new Blob([JSON.stringify(original.data)], { type: 'application/json' });
                const file = new File([blob], original.metadata.filename, { type: 'application/json' });
                
                // Simulate file selection in the raw file input
                const dataTransfer = new DataTransfer();
                dataTransfer.items.add(file);
                const fileInput = document.getElementById('raw-file');
                if (fileInput) {
                    fileInput.files = dataTransfer.files;
                    
                    // Update file info display
                    const fileInfo = document.getElementById('file-info');
                    if (fileInfo) {
                        fileInfo.textContent = `Selected: ${file.name} (${(file.size / 1024 / 1024).toFixed(1)} MB)`;
                    }
                    
                    // Store filename for later
                    parsedFileName = file.name;
                    
                    // Move to parsing step
                    if (typeof moveToStep === 'function') {
                        moveToStep(1);
                    }
                    
                    // Auto-populate dates if available
                    if (original.metadata && original.metadata.dateRange) {
                        const fromInput = document.querySelector('input[name="parse_from_date"]');
                        const toInput = document.querySelector('input[name="parse_to_date"]');
                        if (fromInput && original.metadata.dateRange.start) {
                            fromInput.value = original.metadata.dateRange.start.split('T')[0];
                        }
                        if (toInput && original.metadata.dateRange.end) {
                            toInput.value = original.metadata.dateRange.end.split('T')[0];
                        }
                    }
                    
                    showNotification(`Loaded raw file: ${original.metadata.filename}. Ready to parse.`);
                } else {
                    showNotification('Error: File input not found', 'error');
                }
            }
        }
    } catch (error) {
        console.error('Error loading original:', error);
        showNotification('Error loading file', 'error');
    } finally {
        setTimeout(() => {
            window.dataFromStorage = false;
        }, 1000);
    }
}

async function loadSubsetData(subsetId) {
    // This is for loading date range subsets - redirect to quickLoadSubset for now
    return quickLoadSubset(subsetId);
}

async function deleteSubsetData(subsetId) {
    if (confirm('Delete this saved date range?')) {
        await storageManager.deleteSubset(subsetId);
        await refreshDataSourcesList();
        showNotification('Date range deleted');
    }
}

async function deleteOriginalFile(originalId) {
    if (confirm('Delete this file and all its date ranges?')) {
        await storageManager.deleteOriginal(originalId);
        await refreshDataSourcesList();
        showNotification('File deleted');
        
        // Hide quick access panel if no data left
        const sources = await storageManager.getAvailableDataSources();
        if (sources.originals.length === 0 && sources.subsets.length === 0) {
            document.getElementById('quickAccessPanel').style.display = 'none';
        }
    }
}

async function exportSubset(subsetId) {
    const subset = await storageManager.loadSubset(subsetId);
    if (subset) {
        const dataStr = JSON.stringify(subset.data, null, 2);
        const dataBlob = new Blob([dataStr], {type: 'application/json'});
        const url = URL.createObjectURL(dataBlob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `${subset.metadata.name}.json`;
        link.click();
    }
}

async function processLoadedSubset(subset) {
    // Send the subset data to your existing processing endpoint
    try {
        const response = await fetch('/process_subset', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                data: subset.data,
                settings: subset.settings,
                metadata: subset.metadata
            })
        });
        
        if (response.ok) {
            const result = await response.json();
            // Handle the response - update your UI with the results
            console.log('Subset processed:', result);
        }
    } catch (error) {
        console.error('Error processing subset:', error);
    }
}

function showDateRangeSelector(data, originalId) {
    // For now, just process the whole file
    console.log('Processing original file:', originalId);
}

function createNewRange(originalId) {
    console.log('Create new range for:', originalId);
    // This will open your existing date range selector
}

// File upload handling
document.addEventListener('DOMContentLoaded', function() {
    const fileInput = document.getElementById('storageFileInput');
    if (fileInput) {
        fileInput.addEventListener('change', async (e) => {
            const file = e.target.files[0];
            if (!file) return;
            
            try {
                // Parse the file to get metadata
                const fileText = await file.text();
                const parsedData = JSON.parse(fileText);
                
                // Save as new original using the new signature
                const originalId = await storageManager.saveOriginalFile(
                    file.name,
                    parsedData,
                    {
                        uploadedAt: new Date().toISOString(),
                        source: 'manual_upload'
                    }
                );
                
                showNotification('File saved successfully');
                
                // Show quick access panel
                showQuickAccessPanel();
                
                // Refresh lists
                await refreshDataSourcesList();
                
                // Reset file input
                fileInput.value = '';
                
            } catch (error) {
                console.error('Error handling file:', error);
                showNotification('Error processing file', 'error');
            }
        });
    }
});

// Helper functions
function formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function showNotification(message, type = 'success') {
    // Use existing status message system if available
    if (typeof showStatus === 'function') {
        showStatus(message, type);
    } else {
        // Fallback notification
        const notification = document.createElement('div');
        notification.className = `notification ${type}`;
        notification.textContent = message;
        notification.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 15px 25px;
            background: ${type === 'error' ? '#f44336' : '#4CAF50'};
            color: white;
            border-radius: 5px;
            z-index: 10000;
        `;
        document.body.appendChild(notification);
        
        setTimeout(() => {
            notification.remove();
        }, 3000);
    }
}

async function updateStorageInfo() {
    if (navigator.storage && navigator.storage.estimate) {
        const estimate = await navigator.storage.estimate();
        const percentUsed = ((estimate.usage / estimate.quota) * 100).toFixed(1);
        const usedMB = (estimate.usage / (1024 * 1024)).toFixed(1);
        const quotaMB = (estimate.quota / (1024 * 1024)).toFixed(0);
        
        const storageInfo = document.getElementById('storageInfo');
        if (storageInfo) {
            storageInfo.textContent = `Storage: ${usedMB} MB / ${quotaMB} MB (${percentUsed}%)`;
            
            if (percentUsed > 80) {
                storageInfo.style.color = 'orange';
            } else if (percentUsed > 95) {
                storageInfo.style.color = 'red';
            }
        }
    }
}

// FIX 2: Modified to default to files tab
async function showAllSavedData() {
    // Check if storage manager is initialized
    if (!window.storageManager || !window.storageManager.db) {
        console.error('Storage manager not initialized yet');
        showNotification('Storage system is initializing. Please try again in a moment.', 'error');
        return;
    }
    
    // Show the modal
    const modal = document.getElementById('dataManagerModal');
    if (modal) {
        modal.style.display = 'block';
        
        // FIX 2: Default to files tab instead of upload
        switchTab('originals');
        
        // Refresh the lists
        await refreshDataSourcesList();
    }
}

function closeDataManager() {
    const modal = document.getElementById('dataManagerModal');
    if (modal) {
        modal.style.display = 'none';
    }
}

// FIX 2: Modified to properly handle tab switching
function switchTab(tabName) {
    // Hide all tabs
    document.querySelectorAll('.tab-pane').forEach(pane => {
        pane.style.display = 'none';
    });
    
    // Remove active class from all buttons
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    
    // Show selected tab
    const selectedTab = document.getElementById(tabName + 'Tab');
    if (selectedTab) {
        selectedTab.style.display = 'block';
    }
    
    // Add active class to corresponding button based on position
    const buttons = document.querySelectorAll('.tab-btn');
    if (tabName === 'upload' && buttons[0]) {
        buttons[0].classList.add('active');
    } else if (tabName === 'originals' && buttons[1]) {
        buttons[1].classList.add('active');
    } else if (tabName === 'subsets' && buttons[2]) {
        buttons[2].classList.add('active');
    }
}

function showQuickAccessPanel() {
    const panel = document.getElementById('quickAccessPanel');
    if (panel) {
        panel.style.display = 'block';
    }
}

function toggleDataPanel() {
    const content = document.getElementById('dataPanelContent');
    if (content) {
        if (content.style.display === 'none') {
            content.style.display = 'block';
        } else {
            content.style.display = 'none';
        }
    }
}

async function clearAllStorage() {
    if (confirm('This will delete all saved files and date ranges. Are you sure?')) {
        const sources = await storageManager.getAvailableDataSources();
        
        for (const original of sources.originals) {
            await storageManager.deleteOriginal(original.id);
        }
        
        await refreshDataSourcesList();
        showNotification('All data cleared');
        
        // Hide quick access panel
        document.getElementById('quickAccessPanel').style.display = 'none';
    }
}

// Make functions globally available
window.quickLoadSubset = quickLoadSubset;
window.loadOriginalFile = loadOriginalFile;
window.loadSubsetData = loadSubsetData;
window.deleteSubsetData = deleteSubsetData;
window.deleteOriginalFile = deleteOriginalFile;
window.exportSubset = exportSubset;
window.createNewRange = createNewRange;
window.showAllSavedData = showAllSavedData;
window.closeDataManager = closeDataManager;
window.switchTab = switchTab;
window.toggleDataPanel = toggleDataPanel;
window.clearAllStorage = clearAllStorage;

// Add these functions to storage-integration.js

async function removeMasterFile(masterId) {
    if (!confirm('Remove the master file from this session? (File remains in storage)')) {
        return;
    }
    // Just refresh the UI, don't actually delete
    await refreshDataSourcesList();
}

async function replaceMasterFile() {
    // Open the upload dialog
    document.getElementById('storageFileInput').click();
}

// Make functions globally available
window.processSelectedParsedFile = processSelectedParsedFile;
window.deleteSelectedParsedFiles = deleteSelectedParsedFiles;
window.removeMasterFile = removeMasterFile;
window.replaceMasterFile = replaceMasterFile;