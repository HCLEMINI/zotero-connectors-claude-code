/*
    ***** BEGIN LICENSE BLOCK *****

    Claude Bridge — connects the Zotero Connector background service worker to
    the local Claude Code MCP server (independent repo `zotero-claude-bridge`)
    over a loopback WebSocket, so Claude Code can drive page captures into the
    local Zotero desktop client.

    The bridge is a passive responder: the MCP server is the request originator.
    It reuses Zotero.Connector_Browser.captureActiveTab / captureUrl (added by
    this fork) and only orchestrates + transports results.

    Wire format (UTF-8 JSON text frames):
      {v:1, id?, type, action?, data?, ts}
    type:   auth | auth_ok | request | response | notification | error | heartbeat
    action: ping | get_status | capture_active_tab | capture_url | capture_urls

    Auth (MVP): loopback trust. The server binds 127.0.0.1 only; the extension
    sends an `auth` frame on open and the server replies `auth_ok`. No shared
    secret (MV3 cannot read OS username/hostname to compute one).

    ***** END LICENSE BLOCK *****
*/

Zotero.ClaudeBridge = (function () {
	const STATE = {
		DISABLED: 0,
		DISCONNECTED: 1,
		CONNECTING: 2,
		AUTH_PENDING: 3,
		AUTH_OK: 4,
	};

	const DEFAULT_PORT = 24731;
	const WS_PATH = '/bridge';
	const PING_INTERVAL_MS = 30000;
	const HEARTBEAT_WATCHDOG_MS = 60000; // close if no frame for this long after auth
	const BACKOFF_BASE_MS = 1000;
	const BACKOFF_MAX_MS = 30000;
	const CAPTURE_URL_DEFAULT_CONCURRENCY = 3;
	const CAPTURE_URL_MAX_CONCURRENCY = 5;
	const CAPTURE_URL_MAX = 50; // hard cap per capture_urls call

	let state = STATE.DISCONNECTED;
	let ws = null;
	let port = DEFAULT_PORT;
	let reconnectAttempts = 0;
	let pingTimer = null;
	let heartbeatWatchdog = null;
	let reconnectTimer = null;

	function log(...args) {
		Zotero.debug('[ClaudeBridge] ' + args.map((a) =>
			(a && typeof a === 'object') ? JSON.stringify(a) : String(a)).join(' '));
	}

	async function init() {
		try {
			const enabled = await Zotero.Prefs.getAsync('claudeBridge.enabled');
			if (enabled === false) {
				state = STATE.DISABLED;
				log('disabled by pref claudeBridge.enabled=false');
				return;
			}
			const prefPort = await Zotero.Prefs.getAsync('claudeBridge.port');
			if (prefPort) port = prefPort;
		}
		catch (e) {
			log('pref read failed, using defaults: ', (e && e.message) || e);
		}
		_connect();
	}

	function _connect() {
		try {
			state = STATE.CONNECTING;
			const url = `ws://127.0.0.1:${port}${WS_PATH}`;
			log('connecting', url);
			ws = new WebSocket(url);
			ws.onopen = _onOpen;
			ws.onmessage = _onMessage;
			ws.onclose = _onClose;
			ws.onerror = (e) => log('ws error: ', (e && e.message) || e);
		}
		catch (e) {
			log('connect threw: ', (e && e.message) || e);
			_scheduleReconnect();
		}
	}

	function _onOpen() {
		state = STATE.AUTH_PENDING;
		_send({ type: 'auth', client: 'zotero-connector', version: Zotero.version || 'unknown' });
	}

	function _onMessage(ev) {
		let msg;
		try { msg = JSON.parse(ev.data); }
		catch (e) { return; }
		if (!msg || msg.v !== 1) return;
		switch (msg.type) {
			case 'auth_ok':
				state = STATE.AUTH_OK;
				reconnectAttempts = 0;
				log('authenticated');
				Zotero.Connector_Browser.setKeepServiceWorkerAlive(true);
				_startPing();
				break;
			case 'heartbeat':
				_resetHeartbeatWatchdog();
				_send({ type: 'heartbeat', ts: Date.now() });
				break;
			case 'request':
				_dispatch(msg);
				break;
			// response/error are not used: the extension sends no requests.
			default:
				break;
		}
	}

	async function _dispatch(req) {
		const t0 = Date.now();
		const finish = (result) => {
			if (result && typeof result === 'object' && !('durationMs' in result)) {
				result.durationMs = Date.now() - t0;
			}
			_send({ type: 'response', id: req.id, action: req.action, data: result });
		};
		const fail = (err) => {
			_send({ type: 'error', id: req.id, action: req.action, data: {
				success: false,
				error: (err && err.message) || String(err),
				errorType: (err && err.errorType) || 'unknown',
				items: [],
			}});
		};
		try {
			let result;
			switch (req.action) {
				case 'ping':
					result = { success: true, pong: true, items: [] };
					break;
				case 'get_status':
					result = await _getStatus();
					break;
				case 'capture_active_tab': {
					const tabId = req.data && req.data.tabId;
					result = await Zotero.Connector_Browser.captureActiveTab(tabId);
					break;
				}
				case 'capture_url':
					result = await Zotero.Connector_Browser.captureUrl(
						req.data && req.data.url, (req.data && req.data.options) || {});
					break;
				case 'capture_urls':
					result = await _captureUrls(req.data || {});
					break;
				default:
					throw Object.assign(new Error('unknown action: ' + req.action), { errorType: 'unknown' });
			}
			finish(result);
		}
		catch (e) {
			log('dispatch error ', req.action, ': ', (e && e.message) || e);
			fail(e);
		}
	}

	async function _captureUrls(data) {
		const urls = Array.isArray(data.urls) ? data.urls.slice(0, CAPTURE_URL_MAX) : [];
		let concurrency = data.concurrency || CAPTURE_URL_DEFAULT_CONCURRENCY;
		concurrency = Math.max(1, Math.min(concurrency, CAPTURE_URL_MAX_CONCURRENCY));
		const stopOnError = data.stopOnError === true;

		const results = [];
		const queue = urls.slice();
		let stopped = false;

		async function worker() {
			while (queue.length) {
				const url = queue.shift();
				let r;
				try {
					r = await Zotero.Connector_Browser.captureUrl(url);
				}
				catch (e) {
					r = {
						success: false,
						errorType: (e && e.errorType) || 'unknown',
						error: (e && e.message) || String(e),
						url, items: [],
					};
				}
				results.push(r);
				_send({ type: 'notification', action: 'progress', data: {
					url, index: results.length, total: urls.length, success: !!r.success,
				}});
				if (!r.success && stopOnError) {
					stopped = true;
					return;
				}
			}
		}

		const n = Math.min(concurrency, urls.length);
		await Promise.all(Array.from({ length: n }, worker));

		if (stopped) {
			while (queue.length) {
				results.push({
					success: false, errorType: 'cancelled',
					error: 'skipped due to stopOnError', url: queue.shift(), items: [],
				});
			}
		}
		const ok = results.filter((r) => r.success).length;
		return {
			success: ok > 0,
			results,
			summary: { total: urls.length, success: ok, fail: urls.length - ok },
		};
	}

	async function _getStatus() {
		let online = false;
		try { online = await Zotero.Connector.checkIsOnline(); }
		catch (e) {}
		return {
			success: true,
			extension_connected: true,
			zotero_online: online,
			zotero_version: Zotero.version || 'unknown',
			items: [],
		};
	}

	function _onClose(ev) {
		const wasAuth = state === STATE.AUTH_OK;
		state = STATE.DISCONNECTED;
		log('closed ', (ev && ev.code) || '', (ev && ev.reason) || '');
		if (wasAuth) Zotero.Connector_Browser.setKeepServiceWorkerAlive(false);
		_stopPing();
		ws = null;
		_scheduleReconnect();
	}

	function _scheduleReconnect() {
		if (reconnectTimer) clearTimeout(reconnectTimer);
		const delay = Math.min(BACKOFF_MAX_MS, BACKOFF_BASE_MS * (2 ** reconnectAttempts));
		reconnectAttempts++;
		log('reconnect in ', delay, 'ms (attempt ', reconnectAttempts, ')');
		reconnectTimer = setTimeout(() => {
			reconnectTimer = null;
			_connect();
		}, delay);
	}

	function _startPing() {
		_stopPing();
		pingTimer = setInterval(() => {
			_send({ type: 'heartbeat', ts: Date.now() });
		}, PING_INTERVAL_MS);
		_resetHeartbeatWatchdog();
	}

	function _resetHeartbeatWatchdog() {
		if (heartbeatWatchdog) clearTimeout(heartbeatWatchdog);
		heartbeatWatchdog = setTimeout(() => {
			log('heartbeat watchdog fired — closing stale connection');
			try { if (ws) ws.close(); } catch (e) {}
		}, HEARTBEAT_WATCHDOG_MS);
	}

	function _stopPing() {
		if (pingTimer) { clearInterval(pingTimer); pingTimer = null; }
		if (heartbeatWatchdog) { clearTimeout(heartbeatWatchdog); heartbeatWatchdog = null; }
	}

	function _send(obj) {
		if (ws && ws.readyState === WebSocket.OPEN) {
			try {
				ws.send(JSON.stringify(Object.assign({ v: 1, ts: Date.now() }, obj)));
			}
			catch (e) {
				log('send failed: ', (e && e.message) || e);
			}
		}
	}

	return {
		init,
		get state() { return state; },
		get STATE() { return STATE; },
	};
})();

// Boot only after the rest of the extension (Messaging, Connector_Browser,
// Translators, …) has finished initializing.
(async () => {
	try {
		await Zotero.initDeferred.promise;
		await Zotero.ClaudeBridge.init();
	}
	catch (e) {
		Zotero.debug('[ClaudeBridge] boot failed: ' + ((e && e.message) || e));
	}
})();
