const fs = require('fs').promises;
const fsSync = require('fs');
const path = require('path');

const http = require('http');
const https = require('https');
const { URL } = require('url');

class CollectionCache {
    constructor(options = {}) {
        this.storagePath = options.storagePath || './storage';
        this.debug = options.debug || false;

        this.cache = new Map();
        this.locks = new Map();
        this.dirty = new Set();
        this.pendingSaves = new Set();

        this.isShuttingDown = false;

        const autoSaveInterval = options.autoSaveInterval || 5000;

        if (autoSaveInterval > 0) {
            this.autoSaveTimer = setInterval(() => {
                this._autoSave().catch(console.error);
            }, autoSaveInterval);
            this.autoSaveTimer.unref();
        }
    }

    async _withLock(name, fn) {
        const previous = this.locks.get(name) || Promise.resolve();

        const next = previous
            .then(fn)
            .catch(err => {
                console.error(`[CollectionCache] ${name} error:`, err);
                throw err;
            });

        this.locks.set(name, next);
        return next;
    }

    async _loadUnlocked(name) {
        if (this.cache.has(name)) {
            return this.cache.get(name);
        }

        const start = Utils.startTimer();

        const filePath = path.join(this.storagePath, `${name}.json`);

        try {
            const raw = await fs.readFile(filePath, 'utf8');

            let documents = [];
            try {
                documents = JSON.parse(raw) || [];
            } catch (e) {
                console.error(`[CollectionCache] Corrupted file ${name}, starting fresh`);
            }

            const idIndex = new Map();
            documents.forEach((doc, i) => {
                if (doc?.id !== undefined) {
                    idIndex.set(doc.id, i);
                }
            });

            const data = { documents, idIndex, dirty: false };
            this.cache.set(name, data);

            if (this.debug) {
                console.log(`[CollectionCache] Loaded ${name} with ${data.documents.length} documents in ${Format.formatDuration(Utils.endTimer(start))}`);
            }

            return data;

        } catch (err) {
            if (err.code === 'ENOENT') {
                const data = {
                    documents: [],
                    idIndex: new Map(),
                    dirty: false
                };
                this.cache.set(name, data);
                return data;
            }
            throw err;
        }
    }

    async _saveUnlocked(name, data) {
        const filePath = path.join(this.storagePath, `${name}.json`);
        const tmpPath = `${filePath}.${Date.now()}.tmp`;

        this.pendingSaves.add(tmpPath);

        try {
            await fs.mkdir(this.storagePath, { recursive: true });

            const content =
                '[\n' +
                data.documents.map(doc => JSON.stringify(Utils.reorderDocumentFields(doc))).join(',\n') +
                '\n]';

            await fs.writeFile(tmpPath, content, 'utf8');

            JSON.parse(await fs.readFile(tmpPath, 'utf8'));

            await fs.rename(tmpPath, filePath);
        } finally {
            this.pendingSaves.delete(tmpPath);
            await fs.unlink(tmpPath).catch(() => { });
        }
    }

    async get(name) {
        return this._withLock(name, async () => {
            return this._loadUnlocked(name);
        });
    }

    async update(name, updateFn) {
        return this._withLock(name, async () => {
            const data = await this._loadUnlocked(name);
            const changed = await updateFn(data);

            if (changed !== false) {
                data.dirty = true;
                this.dirty.add(name);
            }

            return changed;
        });
    }

    async updateDocument(name, id, updateFn) {
        return this._withLock(name, async () => {
            const data = await this._loadUnlocked(name);

            const idx = data.idIndex.get(id);
            if (idx === undefined) {
                throw new Error(`[CollectionCache.updateDocument] Document ${id} not found in ${name}`);
            }

            const updated = { ...data.documents[idx] };

            updateFn(updated);

            if (updated.id !== id) {
                throw new Error('[CollectionCache.update] Updated document id cannot be changed');
            }

            data.documents[idx] = updated;
            data.dirty = true;
            this.dirty.add(name);

            return updated;
        });
    }

    async removeDocument(name, id) {
        return this._withLock(name, async () => {
            const data = await this._loadUnlocked(name);

            const idx = data.idIndex.get(id);
            if (idx === undefined) return false;

            data.documents.splice(idx, 1);
            data.idIndex.delete(id);

            for (let i = idx; i < data.documents.length; i++) {
                const doc = data.documents[i];
                if (doc?.id !== undefined) {
                    data.idIndex.set(doc.id, i);
                }
            }

            data.dirty = true;
            this.dirty.add(name);

            return true;
        });
    }

    async save(name) {
        const start = Utils.startTimer();

        return this._withLock(name, async () => {
            const data = this.cache.get(name);
            if (!data || !data.dirty) return false;

            await this._saveUnlocked(name, data);

            data.dirty = false;
            this.dirty.delete(name);

            console.log(`[CollectionCache] Saved ${name}.json with ${data.documents.length} documents in ${Format.formatDuration(Utils.endTimer(start))}`);
            return true;
        });
    }

    async _autoSave() {
        if (this.isShuttingDown) return;
        if (this.dirty.size === 0) return;

        for (const name of Array.from(this.dirty)) {
            try {
                await this.save(name);
            } catch (err) {
                console.error(`[CollectionCache] Autosave failed for ${name}`, err);
            }
        }
    }

    async flushAll() {
        this.isShuttingDown = true;
        const start = Utils.startTimer();

        const savePromises = Array.from(this.dirty).map(name =>
            this.save(name).catch(console.error)
        );

        await Promise.all(savePromises);

        console.log(`[CollectionCache] Flushed ${savePromises.length} collections in ${Format.formatDuration(Utils.endTimer(start))}`);
    }

    async shutdown(signal) {
        if (this.debug) {
            console.log(`[CollectionCache] ${signal} received`);
        }
        await this.flushAll();
        return true;
        //process.exit(0);
    };
}

class QueryEngine {

    static applyFilters(data, filter) {
        if (!filter || Object.keys(filter).length === 0) return data;

        return data.filter(item => this.matchesFilter(item, filter));
    }

    static compareValue(actual, expected) {
        return actual === expected;
    }

    static matchesFilter(item, filter) {
        if (!filter) return true;
        if (filter.$and) return filter.$and.every(f => this.matchesFilter(item, f));
        if (filter.$or) return filter.$or.some(f => this.matchesFilter(item, f));
        if (filter.$nor) return !filter.$nor.some(f => this.matchesFilter(item, f));
        if (filter.$not) return !this.matchesFilter(item, filter.$not);

        for (const key of Object.keys(filter)) {
            if (key.startsWith('$')) continue;

            const expected = filter[key];
            const value = this.getValue(item, key);

            if (
                typeof expected === 'object' &&
                expected !== null &&
                !Array.isArray(expected)
            ) {
                if (!this.matchesOperators(value, expected)) return false;
            } else {
                if (Array.isArray(value)) {
                    if (!value.includes(expected)) return false;
                } else if (value !== expected) {
                    return false;
                }
            }
        }

        return true;
    }

    static getValue(item, path) {
        if (!path.includes('.')) return item[path];

        const parts = path.split('.');
        let cur = item;

        for (let i = 0; i < parts.length; i++) {
            const p = parts[i];

            if (Array.isArray(cur)) {
                const idx = parseInt(p, 10);
                if (!isNaN(idx) && idx >= 0 && idx < cur.length) {
                    cur = cur[idx];
                    continue;
                }

                const remainingPath = parts.slice(i).join('.');
                let results = [];

                for (let el of cur) {
                    const v = this.getValue(el, remainingPath);
                    if (v !== undefined) {
                        if (Array.isArray(v)) {
                            results.push(...v);
                        } else {
                            results.push(v);
                        }
                    }
                }

                return results.length > 0 ? results : undefined;
            }

            if (cur == null || typeof cur !== 'object') return undefined;
            cur = cur[p];
        }

        return cur;
    }

    static matchesOperators(actual, ops) {
        for (const [op, expected] of Object.entries(ops)) {
            if (op === '$options') continue;

            if (actual === undefined) {
                switch (op) {
                    case '$exists':
                        if (expected === true) return false;
                        if (expected === false) return true;
                        break;
                    case '$ne':
                        return true;
                    default:
                        return false;
                }
                continue;
            }

            switch (op) {
                case '$eq':
                    if (Array.isArray(actual)) {
                        if (!actual.includes(expected)) return false;
                    } else if (actual !== expected) return false;
                    break;

                case '$ne':
                    if (Array.isArray(actual)) {
                        if (actual.includes(expected)) return false;
                    } else if (actual === expected) return false;
                    break;

                case '$gt':
                    if (Array.isArray(actual)) {
                        if (!actual.some(v => v > expected)) return false;
                    } else if (!(actual > expected)) return false;
                    break;

                case '$gte':
                    if (Array.isArray(actual)) {
                        if (!actual.some(v => v >= expected)) return false;
                    } else if (!(actual >= expected)) return false;
                    break;

                case '$lt':
                    if (Array.isArray(actual)) {
                        if (!actual.some(v => v < expected)) return false;
                    } else if (!(actual < expected)) return false;
                    break;

                case '$lte':
                    if (Array.isArray(actual)) {
                        if (!actual.some(v => v <= expected)) return false;
                    } else if (!(actual <= expected)) return false;
                    break;

                case '$in':
                    if (Array.isArray(actual)) {
                        if (!actual.some(v => expected.includes(v))) return false;
                    } else {
                        if (!expected.includes(actual)) return false;
                    }
                    break;

                case '$nin':
                    if (Array.isArray(actual)) {
                        if (actual.some(v => expected.includes(v))) return false;
                    } else {
                        if (expected.includes(actual)) return false;
                    }
                    break;

                case '$exists':
                    if (expected === true && actual === undefined) return false;
                    if (expected === false && actual !== undefined) return false;
                    break;

                case '$not':
                    return !this.matchesOperators(actual, expected);

                case '$regex':
                    try {
                        const pattern = expected instanceof RegExp ? expected : new RegExp(expected, ops.$options || '');
                        if (Array.isArray(actual)) {
                            if (!actual.some(v => pattern.test(String(v)))) return false;
                        } else {
                            if (!pattern.test(String(actual))) return false;
                        }
                    } catch (e) {
                        console.warn('Invalid regex pattern:', expected);
                        return false;
                    }
                    break;

                case '$mod':
                    if (!Array.isArray(expected) || expected.length !== 2) {
                        console.warn("Invalid $mod operator:", expected);
                        return false;
                    }
                    const [div, rem] = expected;
                    if (Array.isArray(actual)) {
                        if (!actual.some(v => typeof v === 'number' && v % div === rem)) return false;
                    } else {
                        if (typeof actual !== 'number') return false;
                        if (actual % div !== rem) return false;
                    }
                    break;

                default:
                    if (this.debug) console.warn("Unknown operator:", op);
                    continue;
            }
        }
        return true;
    }

    static count(documents, filters = {}) {
        if (!filters || Object.keys(filters).length === 0) {
            return documents.length;
        }
        return this.applyFilters(documents, filters).length;
    }

    static sortResults(data, sortSpec) {
        return data.sort((a, b) => {
            for (const [field, direction] of Object.entries(sortSpec)) {
                const aVal = this.getValue(a, field);
                const bVal = this.getValue(b, field);

                if (aVal < bVal) return direction === 1 ? -1 : 1;
                if (aVal > bVal) return direction === 1 ? 1 : -1;
            }
            return 0;
        });
    }

    static selectFields(data, projection) {
        if (!projection || Object.keys(projection).length === 0) {
            return data;
        }

        if (data == null) {
            return data;
        }

        const hasIncludeFields = Object.values(projection).some(v => v === 1 || v === true);
        const hasExcludeFields = Object.values(projection).some(v => v === -1 || v === false);

        const projectSingleDoc = (doc) => {
            if (!doc || typeof doc !== 'object') {
                return doc;
            }

            if (hasIncludeFields && !hasExcludeFields) {
                const result = {};
                for (const [field, include] of Object.entries(projection)) {
                    if (include === 1 || include === true) {
                        result[field] = this.getValue(doc, field);
                    }
                }
                return result;
            }

            if (hasExcludeFields && !hasIncludeFields) {
                const result = { ...doc };
                for (const [field, exclude] of Object.entries(projection)) {
                    if (exclude === -1 || exclude === false) {
                        const parts = field.split('.');
                        if (parts.length === 1) {
                            delete result[field];
                        } else {
                            this.removeFieldByPath(result, field);
                        }
                    }
                }
                return result;
            }

            console.warn('Mixed inclusion/exclusion in projection not supported. Returning full document.');
            return doc;
        };

        if (Array.isArray(data)) {
            return data.map(projectSingleDoc);
        } else if (typeof data === 'object') {
            return projectSingleDoc(data);
        } else {
            return data;
        }
    }

    static removeFieldByPath(obj, path) {
        const parts = path.split('.');
        let current = obj;

        for (let i = 0; i < parts.length - 1; i++) {
            const part = parts[i];
            if (current[part] === undefined || typeof current[part] !== 'object') {
                return;
            }
            current = current[part];
        }

        const lastPart = parts[parts.length - 1];
        delete current[lastPart];
    }

    static applyUpdateToDoc(doc, update) {
        if (!update) return;

        const applyNestedOperation = (obj, path, operation, value) => {
            const parts = path.split('.');
            let current = obj;

            for (let i = 0; i < parts.length - 1; i++) {
                const part = parts[i];
                if (current[part] === undefined || typeof current[part] !== 'object') {
                    current[part] = {};
                }
                current = current[part];
            }

            const lastPart = parts[parts.length - 1];

            switch (operation) {
                case 'set':
                    current[lastPart] = value;
                    break;
                case 'unset':
                    delete current[lastPart];
                    break;
                case 'inc':
                    current[lastPart] = (typeof current[lastPart] === 'number' ? current[lastPart] : 0) + value;
                    break;
                case 'push':
                    if (!Array.isArray(current[lastPart])) current[lastPart] = [];
                    current[lastPart].push(value);
                    break;
                case 'addToSet':
                    if (!Array.isArray(current[lastPart])) current[lastPart] = [];
                    if (!current[lastPart].includes(value)) {
                        current[lastPart].push(value);
                    }
                    break;
                case 'pull':
                    if (Array.isArray(current[lastPart])) {
                        current[lastPart] = current[lastPart].filter(item => item !== value);
                    }
                    break;
            }
        };

        const hasRootLevelOperators =
            '$set' in update ||
            '$unset' in update ||
            '$inc' in update ||
            '$push' in update ||
            '$pull' in update ||
            '$addToSet' in update;

        if (hasRootLevelOperators) {
            if (update.$set) {
                for (const [k, v] of Object.entries(update.$set)) {
                    if (k.includes('.')) {
                        applyNestedOperation(doc, k, 'set', v);
                    } else {
                        doc[k] = v;
                    }
                }
            }

            if (update.$unset) {
                for (const k of Object.keys(update.$unset)) {
                    if (k.includes('.')) {
                        applyNestedOperation(doc, k, 'unset', null);
                    } else {
                        delete doc[k];
                    }
                }
            }

            if (update.$inc) {
                for (const [k, v] of Object.entries(update.$inc)) {
                    if (k.includes('.')) {
                        applyNestedOperation(doc, k, 'inc', v);
                    } else {
                        doc[k] = (typeof doc[k] === 'number' ? doc[k] : 0) + v;
                    }
                }
            }

            if (update.$push) {
                for (const [k, v] of Object.entries(update.$push)) {
                    if (k.includes('.')) {
                        applyNestedOperation(doc, k, 'push', v);
                    } else {
                        if (!Array.isArray(doc[k])) doc[k] = [];
                        doc[k].push(v);
                    }
                }
            }

            if (update.$addToSet) {
                for (const [k, v] of Object.entries(update.$addToSet)) {
                    if (k.includes('.')) {
                        applyNestedOperation(doc, k, 'addToSet', v);
                    } else {
                        if (!Array.isArray(doc[k])) doc[k] = [];
                        if (v && typeof v === 'object' && v.$each) {
                            for (const item of v.$each) {
                                if (!doc[k].includes(item)) {
                                    doc[k].push(item);
                                }
                            }
                        } else {
                            if (!doc[k].includes(v)) {
                                doc[k].push(v);
                            }
                        }
                    }
                }
            }

            if (update.$pull) {
                for (const [k, v] of Object.entries(update.$pull)) {
                    if (k.includes('.')) {
                        applyNestedOperation(doc, k, 'pull', v);
                    } else {
                        if (Array.isArray(doc[k])) {
                            doc[k] = doc[k].filter(item => {
                                if (typeof v === 'object' && v.$in) {
                                    return !v.$in.includes(item);
                                }
                                return item !== v;
                            });
                        }
                    }
                }
            }
        } else {
            Object.assign(doc, update);
        }

        doc.updatedAt = new Date().toISOString();
    }
}

class HTTPAdapter {
    constructor(opts = {}) {
        this.poolSize = opts.poolSize || 10;
        this.requestQueue = [];
        this.activeRequests = 0;
        this.maxRetries = opts.maxRetries || 3;
        this.timeout = opts.timeout || 15000;
        this.databaseUrl = opts.databaseUrl || 'http://127.0.0.1:8050';
        this.token = opts.token || null;
        this.parsedBaseUrl = new URL(this.databaseUrl);
        this.isHttps = this.parsedBaseUrl.protocol === 'https:';
        this.hostname = ['localhost', '127.0.0.1'].includes(this.parsedBaseUrl.hostname)
            ? '127.0.0.1'
            : this.parsedBaseUrl.hostname;

        const isLocal = ['localhost', '127.0.0.1'].includes(this.parsedBaseUrl.hostname);

        const agentOptions = {
            keepAlive: !isLocal,           // désactivé en local → pas utile
            keepAliveMsecs: 1000,
            maxSockets: this.poolSize,
            maxFreeSockets: this.poolSize,
            timeout: this.timeout,
            scheduling: 'lifo'
        };

        this.httpAgent = new http.Agent(agentOptions);
        this.httpsAgent = new https.Agent(agentOptions);

        // Configuration des sockets
        const setupSocket = (socket) => {
            socket.setNoDelay(true);                    // toujours utile (même en local)
            if (!isLocal) {
                socket.setKeepAlive(true, 1000);        // seulement en distant
            }
        };

        this.httpAgent.on('socket', setupSocket);
        this.httpsAgent.on('socket', setupSocket);

        this.baseHeaders = {
            "Content-Type": "application/json"
        };

        if (this.token) {
            this.baseHeaders.Authorization = `Bearer ${this.token}`;
        }
    }

    async request(method, endpoint, data = {}) {
        return new Promise((resolve, reject) => {
            this._enqueue({ method, endpoint, data, resolve, reject, retries: 0 });
        });
    }

    _enqueue(req) {
        this.requestQueue.push(req);
        this._processQueue();
    }

    _processQueue() {
        if (this.activeRequests >= this.poolSize || this.requestQueue.length === 0) return;

        const req = this.requestQueue.shift();
        this.activeRequests++;

        this._execute(req)
            .then(req.resolve)
            .catch(err => {
                if (req.retries < this.maxRetries && this._retryable(err)) {
                    req.retries++;
                    this.requestQueue.unshift(req);
                } else {
                    req.reject(err);
                }
            })
            .finally(() => {
                this.activeRequests--;
                setImmediate(() => this._processQueue());
            });
    }

    async _execute(req) {
        let path = req.endpoint;
        let body = null;

        const headers = { ...this.baseHeaders };

        // 🔹 GET / HEAD → query string
        if (req.method === "GET" || req.method === "HEAD") {
            if (req.data && Object.keys(req.data).length > 0) {
                const query = new URLSearchParams();

                for (const [key, value] of Object.entries(req.data)) {
                    query.append(
                        key,
                        typeof value === "object"
                            ? JSON.stringify(value)
                            : String(value)
                    );
                }

                path += `?${query.toString()}`;
            }
        }
        // 🔹 Autres méthodes → body JSON
        else {
            body = JSON.stringify(req.data || {});
            headers["Content-Length"] = Buffer.byteLength(body);
        }

        const options = {
            method: req.method,
            hostname: this.hostname,
            port: this.parsedBaseUrl.port,
            path,
            headers,
            agent: this.isHttps ? this.httpsAgent : this.httpAgent
        };

        return new Promise((resolve, reject) => {
            const start = performance.now();
            const transport = this.isHttps ? https : http;

            const request = transport.request(options, res => {
                const chunks = [];

                res.on("data", c => chunks.push(c));
                res.on("end", () => {
                    const raw = Buffer.concat(chunks);
                    const size = raw.length;

                    let parsed = raw.toString();
                    if (res.headers["content-type"]?.includes("application/json")) {
                        try { parsed = JSON.parse(parsed); } catch { }
                    }

                    this._log(req, start, size, res.statusCode);

                    if (res.statusCode >= 200 && res.statusCode < 300) {
                        resolve(parsed);
                    } else {
                        const RED = '\x1b[31m';
                        const RESET = '\x1b[0m';

                        reject(`${RED}[ERROR] ${parsed.error?.code} ${parsed.error?.message || `HTTP ${res.statusCode}`}${RESET}`);
                        /*
                        reject(
                            new Error(
                                `HTTP ${res.statusCode}: ${parsed?.error.message || parsed
                                }`
                            )
                        );*/
                    }
                });
            });

            request.on("error", err => {
                this._log(req, start, 0, "ERROR", err.message);
                reject(err);
            });

            request.setTimeout(this.timeout, () => {
                request.destroy();
                reject(new Error("Request timeout"));
            });

            if (body) request.end(body);
            else request.end();
        });
    }

    _retryable(err) {
        if (!err || !err.message) return false;
        return (
            err.message.includes("timeout") ||
            err.message.includes("ECONNRESET") ||
            err.message.includes("ECONNREFUSED") ||
            err.message.includes("EAI_AGAIN")
        );
    }

    _log(req, start, size, status, error = null) {
        const ms = (Math.round((performance.now() - start) * 1000) / 1000).toFixed(2);
        const op = req.endpoint.split("/")[2]?.toUpperCase() || "REQUEST";

        if (status === "ERROR") {
            console.log(
                `[HTTP] ${op} ${req.endpoint} | Error: ${error} | Duration: ${ms}ms`
            );
        } else {
            console.log(
                `[HTTP] ${op} | ${req.method} -> ${req.endpoint} | Status: ${status} | Duration: ${ms}ms | Size: ${size}B`
            );
        }
    }

    close() {
        this.httpAgent.destroy();
        this.httpsAgent.destroy();
    }
}

class LocalAdapter {
    constructor(options = {}) {
        this.storagePath = options.storagePath || './storage';
        this.debug = options.debug || false;

        this.cache = new CollectionCache({
            storagePath: this.storagePath,
            autoSaveInterval: options.autoSaveInterval || 5000,
            debug: options.debug
        });

        this.collectionName = null;
    }

    log(...args) {
        if (this.debug) console.log('[LiekoDB]', ...args);
    }

    logRequest(operation, details, duration, responseSize) {
        if (!this.debug) return;

        const durationFormatted = Format.formatDuration(duration);

        let sizePart = '';
        if (typeof responseSize === 'number' && Number.isFinite(responseSize) && responseSize > 0) {
            sizePart = ` | Response Size: ${Format.formatBytes(responseSize)}`;
        }

        this.log(
            `${operation.toUpperCase()} | Collection: ${this.collectionName} | ` +
            `Duration: ${durationFormatted}` +
            sizePart +
            (details ? ` | ${details}` : '')
        );
    }

    logError(operation, err) {
        console.error(
            `[LiekoDB] ERROR - ${operation.toUpperCase()} | Collection: ${this.collectionName}` +
            (err ? ` | ${err}` : '')
        );
        console.error(err)
    }

    listCollections() {
        const collections = [];

        try {
            const files = fsSync.readdirSync(this.storagePath);

            for (const file of files) {
                if (file.endsWith('.json')) {
                    const collectionName = path.basename(file, '.json');
                    const filePath = path.join(this.storagePath, file);

                    let totalDocuments = 0;
                    let fileSize = 0;

                    try {
                        const stats = fsSync.statSync(filePath);
                        fileSize = stats.size;

                        const content = fsSync.readFileSync(filePath, 'utf-8');
                        if (content.trim() === '') {
                            totalDocuments = 0;
                        } else {
                            const data = JSON.parse(content);
                            if (Array.isArray(data)) {
                                totalDocuments = data.length;
                            } else {
                                totalDocuments = Object.keys(data).length;
                            }
                        }
                    } catch (parseErr) {
                        console.error(`Error while reading ${collectionName}:`, parseErr);
                        totalDocuments = 'error';
                        fileSize = 'error';
                    }

                    collections.push({
                        name: collectionName,
                        totalDocuments,
                        sizeBytes: fileSize,
                        sizeFormatted: Format.formatBytes(fileSize)
                    });
                }
            }
        } catch (err) {
            console.error(`Error while reading storage path (${this.storagePath})`, err);
        }

        return collections.sort((a, b) => a.name.localeCompare(b.name));
    }

    async status() {
        const collections = this.listCollections();

        const totalDocuments = collections.reduce((sum, col) => {
            return sum + (typeof col.totalDocuments === 'number' ? col.totalDocuments : 0);
        }, 0);

        const totalCollectionsSize = collections.reduce((sum, col) => {
            return sum + (typeof col.sizeBytes === 'number' ? col.sizeBytes : 0);
        }, 0);

        return {
            storagePath: this.storagePath,
            collections,
            totalCollections: collections.length,
            totalDocuments,
            totalCollectionsSize: totalCollectionsSize,
            totalCollectionsSizeFormatted: Format.formatBytes(totalCollectionsSize)
        };
    }

    async request(method, endpoint, payload = {}) {
        const parts = endpoint.split("/").filter(Boolean);
        // Payload can contains filters, options, data, update

        this.collectionName = parts[1];
        const param = parts[2];

        if (method === "GET" && !param) return this.find(payload);
        if (method === "GET" && param === "count") return this.count(payload);
        if (method === "GET" && param) return this.findById(param, payload);

        if (method === "POST") return this.insert(payload);

        if (method === "PATCH" && param) return this.updateById(param, payload);
        if (method === "PATCH") return this.update(payload);

        if (method === "DELETE" && param === "drop") return this.dropCollection();
        if (method === "DELETE" && param) return this.deleteById(param);
        if (method === "DELETE") return this.delete(payload);

        throw new Error(`Unsupported endpoint: ${method} ${endpoint}`);
    }

    async count({ filters = {} } = {}) {
        try {
            const start = Utils.startTimer();
            const collection = await this.cache.get(this.collectionName);
            const count = QueryEngine.count(collection.documents, filters);

            const duration = Utils.endTimer(start);
            const details = `Filters: ${Format.formatFilters(filters)} | Count: ${count}`;
            this.logRequest('count', details, duration, Utils.getDataSize(count));

            return {
                data: count
            };

        } catch (err) {
            this.logError('count', err);
            return {
                error: {
                    message: err.message || 'Failed to count documents',
                    code: 500
                }
            };
        }
    }

    async find({ filters = {}, options = {} } = {}) {
        const start = Utils.startTimer();
        try {
            const collection = await this.cache.get(this.collectionName);

            let limitNum = null;
            let skipNum = 0;

            if (options.limit) {
                limitNum = parseInt(options.limit, 10);
                if (isNaN(limitNum) || limitNum < 0) {
                    throw new Error('Limit must be a positive number');
                }

                if (options.skip !== undefined) {
                    skipNum = parseInt(options.skip, 10);
                    if (isNaN(skipNum) || skipNum < 0) {
                        throw new Error('Skip cannot be negative');
                    }
                } else if (options.page !== undefined) {
                    const pageNum = parseInt(options.page, 10);
                    if (isNaN(pageNum) || pageNum < 1) {
                        throw new Error('Page must be >= 1');
                    }
                    skipNum = (pageNum - 1) * limitNum;
                }
            }

            const filteredDocuments = QueryEngine.applyFilters(
                collection.documents,
                filters
            );

            let documents = [...filteredDocuments];

            if (options.sort) {
                if (typeof options.sort !== 'object') {
                    throw new Error('Sort must be an object');
                }
                documents = QueryEngine.sortResults(documents, options.sort);
            }

            if (limitNum !== null) {
                documents = documents.slice(skipNum, skipNum + limitNum);
            }

            if (options.fields) {
                if (typeof options.fields !== 'object') {
                    throw new Error('Fields must be an object');
                }
                documents = QueryEngine.selectFields(documents, options.fields);
            }

            const totalFilteredDocuments = filteredDocuments.length;
            const returnedCount = documents.length;
            const duration = Utils.endTimer(start);

            const response = {
                data: {
                    //foundCount: documents.length,
                    documents,
                    //totalDocuments: collection.documents.length
                }
            };

            if (limitNum !== null) {
                const page = Math.floor(skipNum / limitNum) + 1;
                const totalPages = Math.max(1, Math.ceil(totalFilteredDocuments / limitNum));

                response.data.pagination = {
                    page,
                    limit: limitNum,
                    skip: skipNum,

                    totalDocuments: totalFilteredDocuments,
                    totalPages,

                    hasNext: page < totalPages,
                    hasPrev: page > 1,

                    nextPage: page < totalPages ? page + 1 : null,
                    prevPage: page > 1 ? page - 1 : null,

                    startIndex: totalFilteredDocuments > 0 ? skipNum + 1 : 0,
                    endIndex: Math.min(skipNum + returnedCount, totalFilteredDocuments)
                };
            }

            let pageInfo = '';

            if (limitNum !== null) {
                const page = Math.floor(skipNum / limitNum) + 1;
                const totalPages = Math.max(1, Math.ceil(totalFilteredDocuments / limitNum));
                pageInfo = ` | Page: ${page}/${totalPages}`;
            }

            const details =
                `Filters: ${Format.formatFilters(filters)}` +
                `${Format.formatOptions(options)}` +
                `${pageInfo} | Found: ${totalFilteredDocuments} | Returned: ${returnedCount}`;

            this.logRequest(
                'find',
                details,
                duration,
                Utils.getDataSize(response)
            );

            return response;

        } catch (err) {
            this.logError('find', err);

            return {
                error: {
                    message: err.message || 'An unexpected error occurred during find',
                    code: 500
                }
            };
        }
    }

    async findById(id, { options = {} } = {}) {
        try {
            const start = Utils.startTimer();
            const collectionData = await this.cache.get(this.collectionName);
            let details = '';

            let document = null;
            if (collectionData.idIndex?.has(id)) {
                const idx = collectionData.idIndex.get(id);
                document = collectionData.documents[idx];
            }

            if (options.fields && document) {
                document = QueryEngine.selectFields(document, options.fields);
                details = `ID: ${id} | ${Format.formatFieldsLog(options.fields)} | Found: ${document ? 'Yes' : 'No'}`;
            } else {
                details = `ID: ${id} | Found: ${document ? 'Yes' : 'No'}`;
            }

            const duration = Utils.endTimer(start);
            this.logRequest('find_By_Id', details, duration, Utils.getDataSize(document));

            return {
                data: document ?? null
            };

        } catch (err) {
            this.logError('findById', err);
            return {
                error: {
                    message: err.message || 'Internal error during findById',
                    code: 500
                }
            };
        }
    }

    async insert({ documents }) {
        const start = Utils.startTimer();
        try {
            const documentsToInsert = Array.isArray(documents) ? documents : [documents];
            const now = new Date().toISOString();

            let totalDocuments = 0;
            const inserted = [];
            const updated = [];

            await this.cache.update(this.collectionName, async (data) => {
                totalDocuments = data.documents.length;
                const insertCount = documentsToInsert.length;
                const useSequentialIds = insertCount >= 2;

                let prefix = null;
                let sequence = 0;

                if (useSequentialIds) {
                    prefix = Date.now().toString(36);
                }

                for (let document of documentsToInsert) {
                    let docId = document.id;

                    if (!docId) {
                        docId = useSequentialIds
                            ? `${prefix}_${++sequence}`
                            : Utils.generateId();
                        document.id = String(docId);
                    } else {
                        document.id = String(docId);
                    }

                    if (data.idIndex.has(document.id)) {
                        // UPDATE existing document
                        const existingIndex = data.idIndex.get(document.id);
                        const existingDoc = data.documents[existingIndex];
                        const originalCreatedAt = existingDoc.createdAt;

                        for (const [key, value] of Object.entries(document)) {
                            if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
                                existingDoc[key] = {
                                    ...(existingDoc[key] || {}),
                                    ...value
                                };
                            } else {
                                existingDoc[key] = value;
                            }
                        }

                        existingDoc.createdAt = originalCreatedAt;
                        existingDoc.updatedAt = now;

                        updated.push(existingDoc);
                    } else {
                        // INSERT new document
                        document.createdAt = now;
                        document.updatedAt = now;

                        const newIndex = data.documents.length;
                        data.documents.push(document);
                        data.idIndex.set(document.id, newIndex);

                        inserted.push(document);
                    }
                }

                return { inserted, updated };
            });

            const responseData = {
                insertedCount: inserted.length,
                updatedCount: updated.length,
                totalDocuments: totalDocuments + inserted.length
            };

            if (inserted.length > 0) {
                if (inserted.length > 20) {
                    responseData.firstId = inserted[0].id;
                    responseData.lastId = inserted[inserted.length - 1].id;
                } else {
                    responseData.insertedIds = inserted.map(d => d.id);
                }
            }

            const duration = Utils.endTimer(start);
            const details = updated.length > 0
                ? `Inserted: ${inserted.length}, Updated: ${updated.length}`
                : `Inserted: ${inserted.length}`;
            this.logRequest('insert', details, duration, Utils.getDataSize(responseData));

            return { data: responseData };

        } catch (err) {
            this.logError('insert', err);
            return {
                error: {
                    message: err.message || 'Failed to insert documents',
                    code: 500
                }
            };
        }
    }

    async update({ filters = {}, update = {}, options = {} } = {}) {
        try {
            const start = Utils.startTimer();

            const normalizedUpdate = update.$set || update.$inc ||
                update.$push || update.$pull ||
                update.$unset || update.$addToSet
                ? update
                : { $set: update };

            const returnType = options.returnType || 'count';
            const maxReturn = options.maxReturn ?? 50;

            let updated = 0;
            const allUpdatedDocs = [];

            const result = await this.cache.update(this.collectionName, async (data) => {
                for (let i = 0; i < data.documents.length; i++) {
                    if (QueryEngine.matchesFilter(data.documents[i], filters)) {
                        const before = returnType !== 'count'
                            ? JSON.parse(JSON.stringify(data.documents[i]))
                            : null;

                        QueryEngine.applyUpdateToDoc(data.documents[i], normalizedUpdate);
                        data.documents[i].updatedAt = new Date().toISOString();

                        updated++;

                        if (returnType !== 'count') {
                            allUpdatedDocs.push({
                                before,
                                after: data.documents[i]
                            });
                        }
                    }
                }

                return {
                    totalDocuments: data.documents.length,
                    updated,
                    allUpdatedDocs
                };
            });

            let responseData = {
                updatedCount: result.updated,
                totalDocuments: result.totalDocuments
            };

            if (returnType === 'ids' && allUpdatedDocs.length > 0) {
                const ids = allUpdatedDocs.map(item => item.after.id);
                responseData.updatedIds = ids.slice(0, maxReturn);
                if (ids.length > maxReturn) {
                    responseData.truncated = true;
                    responseData.maxReturn = maxReturn;
                }
            } else if (returnType === 'documents' && allUpdatedDocs.length > 0) {
                let documents = allUpdatedDocs.map(item => item.after);

                if (options.fields) {
                    documents = QueryEngine.selectFields(documents, options.fields);
                }

                responseData.updatedDocuments = documents.slice(0, maxReturn);
                responseData.updatedDocuments = responseData.updatedDocuments;

                if (documents.length > maxReturn) {
                    responseData.truncated = true;
                    responseData.maxReturn = maxReturn;
                }
            }

            const duration = Utils.endTimer(start);
            const details = `Filters: ${Format.formatFilters(filters)} | Updated: ${updated} | ReturnType: ${returnType}`;
            this.logRequest('update', details, duration, Utils.getDataSize(responseData));

            return { data: responseData };

        } catch (err) {
            this.logError('update', err);
            return {
                error: {
                    message: err.message || 'Failed to update documents',
                    code: 500
                }
            };
        }
    }

    async updateById(id, { update = {}, options = {} } = {}) {
        const start = Utils.startTimer();
        try {
            const returnType = options.returnType;

            const updatedDoc = await this.cache.updateDocument(
                this.collectionName,
                id,
                (doc) => {
                    console.log(doc);
                    const normalizedUpdate = Object.keys(update).some(k => k.startsWith('$'))
                        ? update
                        : { $set: update };

                    QueryEngine.applyUpdateToDoc(doc, normalizedUpdate);
                    doc.updatedAt = new Date().toISOString();
                    return doc;
                }
            );

            const responseData = {
                updatedCount: 1,
                updatedId: id
            };

            if (returnType === 'document') {
                let returnedDoc = updatedDoc;
                if (options.fields) {
                    returnedDoc = QueryEngine.selectFields(updatedDoc, options.fields);
                }
                responseData.document = returnedDoc;
            }

            const duration = Utils.endTimer(start);
            const details = `ID: ${id} | ReturnType: ${returnType} | Updated: 1`;
            this.logRequest('update_By_Id', details, duration, Utils.getDataSize(responseData));

            return { data: responseData };

        } catch (err) {
            this.logError('update_By_Id', err);
            return {
                error: {
                    message: err.message || 'Failed to update document by ID',
                    code: 500
                }
            };
        }
    }

    async delete({ filters = {} }) {

        if (filters === undefined || Object.keys(filters).length === 0) {
            throw new Error(
                `Filters can't be empty.` +
                `To clear collection, please use .drop() or dropCollection() method.`
            );

        }
        try {
            const start = Utils.startTimer();
            let deletedCount = 0;

            await this.cache.update(this.collectionName, async (data) => {
                const before = data.documents.length;

                const documentsToKeep = data.documents.filter(
                    document => !QueryEngine.matchesFilter(document, filters)
                );

                deletedCount = before - documentsToKeep.length;

                if (deletedCount > 0) {
                    data.documents = documentsToKeep;

                    data.idIndex.clear();
                    documentsToKeep.forEach((doc, idx) => {
                        if (doc.id !== undefined) {
                            data.idIndex.set(doc.id, idx);
                        }
                    });
                }
            });

            const duration = Utils.endTimer(start);
            const details = deletedCount === 0
                ? `Filters: ${Format.formatFilters(filters)} | No documents matched`
                : `Filters: ${Format.formatFilters(filters)} | Deleted: ${deletedCount}`;

            this.logRequest('delete', details, duration, Utils.getDataSize({ deletedCount }));

            return {
                data: {
                    collectionName: this.collectionName,
                    deletedCount,
                    ...(deletedCount === 0 && { details: 'No documents matched filters' })
                }
            };

        } catch (err) {
            this.logError('delete', err);
            return {
                error: {
                    message: err.message || 'Failed to delete documents',
                    code: 500
                }
            };
        }
    }

    async deleteById(id) {
        const start = Utils.startTimer();
        try {
            const deleted = await this.cache.removeDocument(this.collectionName, id);

            const duration = Utils.endTimer(start);

            if (!deleted) {
                this.logRequest('delete_By_Id', `ID: ${id} | Not found`, duration);
                return {
                    data: {
                        deletedCount: 0,
                        details: 'Document not found'
                    }
                };
            }

            this.logRequest('delete_By_Id', `ID: ${id} | Deleted`, duration);

            return {
                data: {
                    deletedCount: 1,
                    deletedId: id
                }
            };

        } catch (err) {
            this.logError('delete_By_Id', err);
            return {
                error: {
                    message: err.message || 'Failed to delete document by ID',
                    code: 500
                }
            };
        }
    }

    async dropCollection() {
        try {
            const start = Utils.startTimer();

            const data = this.cache.cache.get(this.collectionName);
            if (data) {
                if (data.dirty) {
                    await this.cache.save(this.collectionName);
                }

                this.cache.cache.delete(this.collectionName);
                this.cache.dirty.delete(this.collectionName);
                this.cache.locks.delete(this.collectionName);
            }

            const filePath = path.join(this.storagePath, `${this.collectionName}.json`);
            try {
                await fs.unlink(filePath);
            } catch (err) {
                if (err.code !== 'ENOENT') {
                    throw err;
                }
            }

            const duration = Utils.endTimer(start);
            this.logRequest('dropCollection', 'Success', duration);

            return {
                data: {
                    collectionName: this.collectionName,
                    dropped: true
                }
            };

        } catch (err) {
            this.logError('dropCollection', err);
            return {
                error: {
                    message: err.message || 'Unexpected error during dropCollection',
                    code: 500
                }
            };
        }
    }

    async flushAll() {
        await this.cache.flushAll();
    }

    async shutdown(signal) {
        await this.cache.shutdown(signal);
    }

    async close() {
        await this.cache.flushAll();
        return true;
    }
}

class Collection {
    constructor(adapter, collectionName) {
        this.adapter = adapter;
        this.collectionName = collectionName;
    }

    async count(filters = {}) {
        Validator.validateFilters(filters);

        return this.adapter.request('GET', `/collections/${encodeURIComponent(this.collectionName)}/count`, {
            filters
        });
    }

    async find(filters = {}, options = {}) {
        Validator.validateFilters(filters);
        Validator.validateOptions(options);

        return this.adapter.request('GET', `/collections/${encodeURIComponent(this.collectionName)}`, {
            filters,
            options
        });
    }

    async findById(id, options = {}) {
        Validator.validateOptions(options);

        return this.adapter.request('GET', `/collections/${encodeURIComponent(this.collectionName)}/${id}`, {
            options
        });
    }

    async findOne(filters = {}, options = {}) {
        Validator.validateFilters(filters);
        Validator.validateOptions(options);

        try {
            const { error, data } = await this.adapter.request('GET', `/collections/${encodeURIComponent(this.collectionName)}`, {
                filters,
                options: { limit: 1, sort: { updatedAt: -1 }, ...options }
            });

            if (error) return { error };

            if (data.documents.length > 0) {
                return {
                    data: data.documents[0]
                };
            }

            return {
                data: null
            };

        } catch (err) {
            console.error(err);
            return {
                error: {
                    message: err.message || 'Failed to find one document',
                    code: 500
                }
            };
        }
    }

    async insert(documents) {
        return this.adapter.request('POST', `/collections/${encodeURIComponent(this.collectionName)}`, {
            documents
        });
    }

    async update(filters = {}, update = {}, options = {}) {
        Validator.validateFilters(filters);
        Validator.validateOptions(options);

        return this.adapter.request('PATCH', `/collections/${encodeURIComponent(this.collectionName)}`, {
            filters,
            update,
            options
        });
    }

    async updateById(id, update = {}, options = {}) {
        Validator.validateOptions(options);

        return this.adapter.request('PATCH', `/collections/${encodeURIComponent(this.collectionName)}/${id}`, {
            update,
            options
        });
    }

    async delete(filters) {
        if (!filters) {
            throw new Error('Delete operation requires filters to prevent accidental deletion of all documents. {} or use .drop() to delete entire collection.');
        }

        Validator.validateFilters(filters);

        return this.adapter.request('DELETE', `/collections/${encodeURIComponent(this.collectionName)}`, {
            filters
        });
    }

    async deleteById(id) {
        if (!id) {
            throw new Error('deleteById operation requires a valid document ID.');
        }
        return this.adapter.request('DELETE', `/collections/${encodeURIComponent(this.collectionName)}/${id}`);
    }

    async drop() {
        return this.adapter.request('DELETE', `/collections/${encodeURIComponent(this.collectionName)}/drop`);
    }
}

class LiekoDB {
    constructor(options = {}) {
        this.debug = options.debug || false;

        const isBrowser =
            typeof window !== 'undefined' &&
            typeof window.document !== 'undefined';

        if (isBrowser && !options.token) {
            throw new Error(
                'LiekoDB: A token is required when used in a browser environment. ' +
                'LocalAdapter is only available in Node.js.'
            );
        }
        this.adapter = this._createAdapter(options);

        this._setupShutdown();
    }

    _createAdapter(options) {
        if (options.token) {
            return new HTTPAdapter(options);
        }

        if (typeof process !== 'undefined' && process.versions?.node) {
            return new LocalAdapter(options);
        }

        throw new Error(
            'LiekoDB: LocalAdapter cannot be used outside of Node.js'
        );
    }

    collection(name) {
        Validator.validateCollectionName(name);
        return new Collection(this.adapter, name);
    }

    async listCollections() {
        return this.adapter.listCollections();
    }

    async dropCollection(name) {
        Validator.validateCollectionName(name);
        return this.adapter.dropCollection(name);
    }

    async status() {
        return this.adapter.status();
    }

    async close() {
        //promise
        return await this.adapter.close();
    }

    _setupShutdown() {
        if (typeof process === 'undefined') return;

        process.once('SIGINT', () => this.adapter.shutdown('SIGINT'));
        process.once('SIGTERM', () => this.adapter.shutdown('SIGTERM'));
        process.once('beforeExit', () => this.adapter.flushAll());
    }
}

class Format {

    static formatBytes(bytes) {
        if (bytes === 0) return '0 B';
        if (bytes < 1024) return `${bytes} B`;
        if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`;
        return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
    }

    static formatDuration(ms) {
        if (ms < 0.001) return `${(ms * 1000).toFixed(2)} µs`;
        if (ms < 1) return `${(ms * 1000).toFixed(0)} µs`;
        if (ms < 1000) return `${ms.toFixed(2)} ms`;
        return `${(ms / 1000).toFixed(2)} s`;
    }

    /**
     * TODO: when filter is regex, log is only {}
     * {{ email: { '$regex': /@google\.com$/ } }
     * [LiekoDB] FIND users | Duration: 101µs | Response Size: 175 B | Filters: {email:{$regex:{}}} | Found: 1}
     */
    static formatFilters(filters) {
        if (!filters || Object.keys(filters).length === 0) return '{}';
        const formatted = JSON.stringify(filters, null, 0)
            .replace(/"/g, '')
            .replace(/,/g, ', ');
        if (formatted.length > 80) {
            return formatted.substring(0, 77) + '...';
        }
        return formatted;
    }

    static formatOptions(options) {
        if (!options || Object.keys(options).length === 0) return '';
        const parts = [];
        if (options.sort) parts.push(`sort: ${JSON.stringify(options.sort).replace(/"/g, '')}`);
        if (options.limit) parts.push(`limit: ${options.limit}`);
        if (options.skip) parts.push(`skip: ${options.skip}`);
        if (options.fields) parts.push(`fields: ${JSON.stringify(options.fields).replace(/"/g, '')}`);
        return parts.length > 0 ? ` | ${parts.join(', ')}` : '';
    }

    static formatFieldsLog(fields) {
        if (!fields || typeof fields !== 'object') return '';

        const formatted = Object.entries(fields)
            .map(([key, value]) => {
                if (value === 1 || value === true) return `+${key}`;
                if (value === -1 || value === false) return `-${key}`;
                return `${key}:${value}`;
            })
            .join(', ');

        return `Fields: {${formatted}}`;
    }
}

class Validator {

    static validateCollectionName(name) {
        if (!name || typeof name !== 'string') {
            throw new Error(`Collection name must be a non-empty string, got: ${typeof name}`);
        }

        if (name.length < 1) {
            throw new Error('Collection name cannot be empty');
        }

        if (name.length > 64) {
            throw new Error(`Collection name too long (${name.length} > 64 characters)`);
        }

        const validNameRegex = /^[a-zA-Z0-9_-]+$/;
        if (!validNameRegex.test(name)) {
            throw new Error(
                `Invalid collection name: "${name}". ` +
                `Only alphanumeric characters, underscores (_) and hyphens (-) are allowed.`
            );
        }

        if (/^[0-9_-]/.test(name)) {
            throw new Error('Collection name cannot start with a number, underscore or hyphen');
        }

        const invalidPatterns = [
            /\.\./,
            /\/|\\/,
            /^\./,
            /\s/,
            /[<>:"|?*]/,
        ];

        for (const pattern of invalidPatterns) {
            if (pattern.test(name)) {
                throw new Error(`Collection name contains invalid characters: "${name}"`);
            }
        }

        return true;
    }

    static validateFilters(filters) {
        if (filters == null || typeof filters !== 'object' || Array.isArray(filters)) {
            throw new Error('Filters must be a non-null plain object');
        }

        const validOperators = ['$eq', '$ne', '$gt', '$gte', '$lt', '$lte', '$in', '$nin',
            '$exists', '$regex', '$and', '$or', '$nor', '$not'];

        for (const key in filters) {
            if (key.startsWith('$') && !validOperators.includes(key)) {
                throw new Error(`Invalid query operator: >> ${key} << \nValid operators: ${validOperators.join(', ')}`);
            }

            const value = filters[key];
            if (value && typeof value === 'object' && !Array.isArray(value)) {
                this.validateFilters(value);
            } else if (Array.isArray(value) && (key === '$and' || key === '$or' || key === '$nor' || key === '$in' || key === '$nin')) {
                value.forEach(item => {
                    if (typeof item === 'object' && item !== null) {
                        this.validateFilters(item);
                    }
                });
            }
        }
    }

    static validateOptions(options) {
        if (options == null || typeof options !== 'object' || Array.isArray(options)) {
            throw new Error('Options must be a non-null plain object');
        }

        const validKeys = ['sort', 'skip', 'limit', 'fields', 'page', 'returnType', 'maxReturn'];

        for (const key in options) {
            if (!validKeys.includes(key)) {
                throw new Error(`Invalid option key: >> ${key} << \nValid keys: ${validKeys.join(', ')}`);
            }

            if (key === 'sort') {
                if (typeof options[key] !== 'object' || options[key] == null || Array.isArray(options[key])) {
                    throw new Error('Sort must be a non-null plain object');
                }
                for (const sortKey in options[key]) {
                    const val = options[key][sortKey];
                    if (![1, -1, true, false].includes(val)) {
                        throw new Error(`Invalid sort value for ${sortKey}: ${val}. Must be 1, -1, true, or false`);
                    }
                }
            } else if (key === 'skip') {
                if (typeof options[key] !== 'number' || options[key] < 0) {
                    throw new Error('Skip must be a non-negative number');
                }
            } else if (key === 'limit') {
                if (typeof options[key] === 'number') {
                    if (options[key] < 0) {
                        throw new Error('Limit must be a non-negative number');
                    }
                } else {
                    throw new Error('Limit must be a non-negative number');
                }
            } else if (key === 'fields') {
                if (typeof options[key] !== 'object' || options[key] == null || Array.isArray(options[key])) {
                    throw new Error('Fields must be a non-null plain object');
                }
                for (const fieldKey in options[key]) {
                    const val = options[key][fieldKey];
                    if (![1, -1, true, false].includes(val)) {
                        throw new Error(`Invalid field value for ${fieldKey}: ${val}. Must be 1, -1, true, or false`);
                    }
                }
            } else if (key === 'page') {
                if (typeof options[key] !== 'number' || options[key] <= 0) {
                    throw new Error('Page must be a positive number');
                }
            } else if (key === 'returnType') {
                const validTypes = ['count', 'ids', 'documents', 'document', 'id'];
                if (!validTypes.includes(options[key])) {
                    throw new Error(
                        `Invalid returnType: "${options[key]}". ` +
                        `Must be one of: ${validTypes.join(', ')}`
                    );
                }
            } else if (key === 'maxReturn') {
                if (typeof options[key] !== 'number' || options[key] < 0 || !Number.isInteger(options[key])) {
                    throw new Error('maxReturn must be a non-negative integer');
                }
            }
        }
    }
}

class Utils {
    static startTimer() {
        return process.hrtime.bigint();
    }

    static endTimer(start) {
        const end = process.hrtime.bigint();
        const diffNs = end - start;
        return Number(diffNs) / 1_000_000;  // ms
    }

    static generateId() {
        return require('crypto').randomBytes(8).toString('hex');
    }

    static getDataSize(data) {
        try {
            return Buffer.byteLength(JSON.stringify(data), 'utf8');
        } catch (e) {
            return 0;
        }
    }

    static reorderDocumentFields = (document) => {
        if (!document || typeof document !== 'object') return document;

        const orderedDocument = {};
        const reservedFields = ['id', 'createdAt', 'updatedAt'];

        if (document.id !== undefined) {
            orderedDocument.id = document.id;
        }

        const normalFields = Object.keys(document)
            .filter(key => !reservedFields.includes(key))
            .sort();

        for (const key of normalFields) {
            orderedDocument[key] = structuredClone(document[key]);
        }

        if (document.createdAt !== undefined) {
            orderedDocument.createdAt = document.createdAt;
        }
        if (document.updatedAt !== undefined) {
            orderedDocument.updatedAt = document.updatedAt;
        }

        return orderedDocument;
    }
}

module.exports = LiekoDB;

if (typeof window !== 'undefined') {
    window.LiekoDB = LiekoDB;
}