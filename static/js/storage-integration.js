// Storage Integration Functions

async function refreshDataSourcesList() {
    try {
        const sources = await storageManager.getAvailableDataSources();
        
        // Update recent subsets in quick access
        updateRecentSubsets(sources.subsets);
        
        // Update original files list
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
        item.innerHTML = `
            <div class="recent-info">
                <strong>${subset.name}</strong>
                <small>${subset.stats.totalPoints} locations</small>
            </div>
            <button onclick="quickLoadSubset('${subset.id}')" class="quick-load-btn">
                Load
            </button>
        `;
        recentDiv.appendChild(item);
    });
}

function updateOriginalsList(originals) {
    const listDiv = document.getElementById('originalFilesList');
    if (!listDiv) return;
    
    listDiv.innerHTML = '';
    
    if (originals.length === 0) {
        listDiv.innerHTML = '<p class="no-data">No original files saved</p>';
        return;
    }
    
    originals.forEach(original => {
        const item = document.createElement('div');
        item.className = 'data-item';
        
        const dateRange = original.dateRange;
        const startDate = dateRange.start ? new Date(dateRange.start).toLocaleDateString() : 'Unknown';
        const endDate = dateRange.end ? new Date(dateRange.end).toLocaleDateString() : 'Unknown';
        
        item.innerHTML = `
            <div class="item-info">
                <h4>${original.filename}</h4>
                <p>Uploaded: ${new Date(original.uploadDate).toLocaleDateString()}</p>
                <p>Range: ${startDate} - ${endDate}</p>
                <p>Size: ${formatFileSize(original.size)}</p>
            </div>
            <div class="item-actions">
                <button onclick="loadOriginalFile('${original.id}')" class="btn-primary">
                    Process
                </button>
                <button onclick="createNewRange('${original.id}')" class="btn-secondary">
                    New Range
                </button>
                <button onclick="deleteOriginalFile('${original.id}')" class="btn-danger">
                    Delete
                </button>
            </div>
        `;
        listDiv.appendChild(item);
    });
}

function updateSubsetsList(subsets) {
    const listDiv = document.getElementById('subsetsList');
    if (!listDiv) return;
    
    listDiv.innerHTML = '';
    
    if (subsets.length === 0) {
        listDiv.innerHTML = '<p class="no-data">No saved date ranges</p>';
        return;
    }
    
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
            
            item.innerHTML = `
                <div class="item-info">
                    <h4>${subset.name}</h4>
                    <p>Created: ${new Date(subset.createdAt).toLocaleDateString()}</p>
                    <p>Last used: ${new Date(subset.lastUsed).toLocaleDateString()}</p>
                    <p>Points: ${subset.stats.totalPoints}</p>
                    <p>Settings: Distance: ${subset.settings.distanceThreshold}m, Time: ${subset.settings.timeThreshold}s</p>
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
        const original = await storageManager.loadOriginal(originalId);
        if (original) {
            // Close modal
            closeDataManager();
            
            // Show date range selector
            showDateRangeSelector(original, originalId);
        }
    } catch (error) {
        console.error('Error loading original:', error);
        showNotification('Error loading file', 'error');
    }
}

async function loadSubsetData(subsetId) {
    await quickLoadSubset(subsetId);
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
                
                // Save as new original
                const originalId = await storageManager.saveOriginalFile(file, parsedData);
                showNotification('File saved successfully');
                
                // Refresh lists
                await refreshDataSourcesList();
                
                // Close modal
                closeDataManager();
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
    const notification = document.createElement('div');
    notification.className = `notification ${type}`;
    notification.textContent = message;
    document.body.appendChild(notification);
    
    setTimeout(() => {
        notification.remove();
    }, 3000);
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

// Modal controls
function showAllSavedData() {
    document.getElementById('dataManagerModal').style.display = 'block';
    refreshDataSourcesList();
}

function closeDataManager() {
    document.getElementById('dataManagerModal').style.display = 'none';
}

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
    document.getElementById(tabName + 'Tab').style.display = 'block';
    
    // Add active class to clicked button
    event.target.classList.add('active');
}

function showQuickAccessPanel() {
    const panel = document.getElementById('quickAccessPanel');
    if (panel) {
        panel.style.display = 'block';
    }
}

function toggleDataPanel() {
    const content = document.getElementById('dataPanelContent');
    if (content.style.display === 'none') {
        content.style.display = 'block';
    } else {
        content.style.display = 'none';
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