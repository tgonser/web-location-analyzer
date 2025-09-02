class LocationStorageManager {
    constructor() {
        this.dbName = 'LocationAnalyzerDB';
        this.version = 1;
        this.db = null;
    }

    async init() {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open(this.dbName, this.version);
            
            request.onerror = () => reject(request.error);
            request.onsuccess = () => {
                this.db = request.result;
                console.log('IndexedDB opened successfully');
                resolve();
            };
            
            request.onupgradeneeded = (event) => {
                const db = event.target.result;
                
                // Store original files
                if (!db.objectStoreNames.contains('originals')) {
                    const originalsStore = db.createObjectStore('originals', { keyPath: 'id' });
                    originalsStore.createIndex('uploadDate', 'uploadDate', { unique: false });
                }
                
                // Store parsed subsets
                if (!db.objectStoreNames.contains('subsets')) {
                    const subsetsStore = db.createObjectStore('subsets', { keyPath: 'id' });
                    subsetsStore.createIndex('originalId', 'originalId', { unique: false });
                    subsetsStore.createIndex('dateRange', ['startDate', 'endDate'], { unique: false });
                }
                
                // Store metadata
                if (!db.objectStoreNames.contains('metadata')) {
                    db.createObjectStore('metadata', { keyPath: 'key' });
                }
            };
        });
    }

    async saveOriginalFile(filename, parsedData, metadata = {}) {
        const id = `orig_${Date.now()}`;
        
        // Convert parsed data to ArrayBuffer
        const dataStr = JSON.stringify(parsedData);
        const encoder = new TextEncoder();
        const dataArray = encoder.encode(dataStr);
        
        const record = {
            id,
            filename: filename,
            uploadDate: new Date().toISOString(),
            size: dataArray.byteLength,
            dateRange: metadata.dateRange || this.extractDateRange(parsedData),
            data: dataArray.buffer,
            checksum: await this.calculateChecksum(dataArray.buffer),
            metadata: metadata
        };
        
        const transaction = this.db.transaction(['originals'], 'readwrite');
        const store = transaction.objectStore('originals');
        await store.put(record);
        
        // Update metadata
        await this.updateMetadata('lastOriginalId', id);
        
        return id;
    }

    async saveSubset(originalId, filteredData, dateRange, settings) {
        const id = `subset_${Date.now()}`;
        
        const record = {
            id,
            originalId,
            name: `${dateRange.start} to ${dateRange.end}`,
            startDate: dateRange.start,
            endDate: dateRange.end,
            createdAt: new Date().toISOString(),
            lastUsed: new Date().toISOString(),
            settings: {
                distanceThreshold: settings.distanceThreshold,
                timeThreshold: settings.timeThreshold,
                probabilityThreshold: settings.probabilityThreshold
            },
            data: filteredData,
            stats: {
                totalPoints: filteredData.length,
                dateRange: dateRange
            }
        };
        
        const transaction = this.db.transaction(['subsets'], 'readwrite');
        const store = transaction.objectStore('subsets');
        await store.put(record);
        
        return id;
    }

    async getAvailableDataSources() {
        if (!this.db) {
            throw new Error('Database not initialized. Call init() first.');
        }
        
        const sources = {
            originals: [],
            subsets: []
        };
        
        // Get all originals
        const originalsTransaction = this.db.transaction(['originals'], 'readonly');
        const originalsStore = originalsTransaction.objectStore('originals');
        const originals = await this.getAllFromStore(originalsStore);
        
        sources.originals = originals.map(orig => ({
            id: orig.id,
            filename: orig.filename,
            uploadDate: orig.uploadDate,
            dateRange: orig.dateRange,
            size: orig.size,
            metadata: orig.metadata || {}
        }));
        
        // Get all subsets
        const subsetsTransaction = this.db.transaction(['subsets'], 'readonly');
        const subsetsStore = subsetsTransaction.objectStore('subsets');
        const subsets = await this.getAllFromStore(subsetsStore);
        
        sources.subsets = subsets.map(subset => ({
            id: subset.id,
            originalId: subset.originalId,
            name: subset.name,
            dateRange: { start: subset.startDate, end: subset.endDate },
            createdAt: subset.createdAt,
            lastUsed: subset.lastUsed,
            settings: subset.settings,
            stats: subset.stats
        }));
        
        return sources;
    }

    async loadOriginal(id) {
        const transaction = this.db.transaction(['originals'], 'readonly');
        const store = transaction.objectStore('originals');
        const record = await this.getFromStore(store, id);
        
        if (record) {
            // Convert ArrayBuffer back to JSON
            const decoder = new TextDecoder();
            const jsonStr = decoder.decode(record.data);
            const parsedData = JSON.parse(jsonStr);
            
            return {
                data: parsedData,
                metadata: {
                    filename: record.filename,
                    uploadDate: record.uploadDate,
                    dateRange: record.dateRange,
                    ...record.metadata
                }
            };
        }
        return null;
    }

    async loadSubset(id) {
        const transaction = this.db.transaction(['subsets'], 'readwrite');
        const store = transaction.objectStore('subsets');
        const record = await this.getFromStore(store, id);
        
        if (record) {
            // Update last used timestamp
            record.lastUsed = new Date().toISOString();
            await store.put(record);
            
            return {
                data: record.data,
                settings: record.settings,
                metadata: {
                    name: record.name,
                    dateRange: { start: record.startDate, end: record.endDate },
                    originalId: record.originalId,
                    createdAt: record.createdAt
                }
            };
        }
        return null;
    }

    async mergeWithExisting(originalId, newFile) {
        const existing = await this.loadOriginal(originalId);
        if (!existing) return null;
        
        const existingData = await existing.file.text();
        const newData = await newFile.text();
        
        const existingJson = JSON.parse(existingData);
        const newJson = JSON.parse(newData);
        
        // Merge logic - combine semantic location history
        const merged = this.mergeLocationData(existingJson, newJson);
        
        // Save merged as new original
        const mergedBlob = new Blob([JSON.stringify(merged)], { type: 'application/json' });
        const mergedFile = new File([mergedBlob], `merged_${Date.now()}.json`);
        
        return this.saveOriginalFile(mergedFile, merged);
    }

    mergeLocationData(existing, newData) {
        // Assuming the structure has semanticLocationHistory
        const merged = { ...existing };
        
        if (existing.semanticLocationHistory && newData.semanticLocationHistory) {
            // Combine and deduplicate based on timestamps
            const existingLocations = existing.semanticLocationHistory || [];
            const newLocations = newData.semanticLocationHistory || [];
            
            const locationMap = new Map();
            
            // Add existing locations
            existingLocations.forEach(loc => {
                const key = `${loc.startTimestamp}_${loc.endTimestamp}`;
                locationMap.set(key, loc);
            });
            
            // Add new locations (will overwrite if duplicate)
            newLocations.forEach(loc => {
                const key = `${loc.startTimestamp}_${loc.endTimestamp}`;
                locationMap.set(key, loc);
            });
            
            // Sort by timestamp
            merged.semanticLocationHistory = Array.from(locationMap.values())
                .sort((a, b) => new Date(a.startTimestamp) - new Date(b.startTimestamp));
        }
        
        return merged;
    }

    // Helper methods
    async calculateChecksum(arrayBuffer) {
        const hashBuffer = await crypto.subtle.digest('SHA-256', arrayBuffer);
        const hashArray = Array.from(new Uint8Array(hashBuffer));
        return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
    }

    extractDateRange(parsedData) {
        // Handle different data structures
        if (parsedData.semanticLocationHistory && parsedData.semanticLocationHistory.length > 0) {
            const locations = parsedData.semanticLocationHistory;
            const firstDate = locations[0].startTimestamp;
            const lastDate = locations[locations.length - 1].endTimestamp;
            return { start: firstDate, end: lastDate };
        } else if (Array.isArray(parsedData) && parsedData.length > 0) {
            // Handle array of timeline objects
            const firstItem = parsedData[0];
            const lastItem = parsedData[parsedData.length - 1];
            
            // Try to find date fields
            const getDate = (item) => {
                return item.startTimestamp || item.timestamp || item.date || item.startTime;
            };
            
            return {
                start: getDate(firstItem),
                end: getDate(lastItem) || getDate(firstItem)
            };
        }
        return { start: null, end: null };
    }

    async getAllFromStore(store) {
        return new Promise((resolve, reject) => {
            const request = store.getAll();
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    async getFromStore(store, key) {
        return new Promise((resolve, reject) => {
            const request = store.get(key);
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    async updateMetadata(key, value) {
        const transaction = this.db.transaction(['metadata'], 'readwrite');
        const store = transaction.objectStore('metadata');
        await store.put({ key, value, updated: new Date().toISOString() });
    }

    async deleteSubset(id) {
        const transaction = this.db.transaction(['subsets'], 'readwrite');
        const store = transaction.objectStore('subsets');
        await store.delete(id);
    }

    async deleteOriginal(id) {
        // First delete all associated subsets
        const subsetsTransaction = this.db.transaction(['subsets'], 'readwrite');
        const subsetsStore = subsetsTransaction.objectStore('subsets');
        const index = subsetsStore.index('originalId');
        const request = index.openCursor(IDBKeyRange.only(id));
        
        request.onsuccess = (event) => {
            const cursor = event.target.result;
            if (cursor) {
                subsetsStore.delete(cursor.primaryKey);
                cursor.continue();
            }
        };
        
        // Then delete the original
        const originalsTransaction = this.db.transaction(['originals'], 'readwrite');
        const originalsStore = originalsTransaction.objectStore('originals');
        await originalsStore.delete(id);
    }
}

// Don't initialize here - let storage-integration.js handle it
// Export the class for use in other files
if (typeof module !== 'undefined' && module.exports) {
    module.exports = LocationStorageManager;
}