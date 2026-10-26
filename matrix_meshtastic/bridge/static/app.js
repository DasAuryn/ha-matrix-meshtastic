"use strict";

const byId = id => document.getElementById(id);
const view = {
  state: {connected: false, rooms: [], verification: [], devices: []},
  selected: null,
  filter: "all",
  query: "",
  histories: new Map(),
  drafts: new Map(),
  transactions: new Map(),
  sending: new Set(),
  reads: new Map(),
  loading: new Set(),
  activeVerification: null,
  verificationTargets: [],
  verificationTargetRoom: null,
  verificationNotice: "",
  settings: null,
  setupOpened: false,
  fingerprintDevice: null,
  managedRoom: null,
  accountGeneration: 0,
  polling: false
};
const timeFormat = new Intl.DateTimeFormat("de-DE", {hour: "2-digit", minute: "2-digit"});
const dayFormat = new Intl.DateTimeFormat("de-DE", {day: "numeric", month: "long", year: "numeric"});

function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = String(text);
  return node;
}

function show(node, visible) {
  node.classList.toggle("hidden", !visible);
}

function errorText(node, message) {
  node.textContent = message || "";
  show(node, Boolean(message));
}

async function api(path, data, requestOptions = {}) {
  const generation = view.accountGeneration;
  const options = {cache: "no-store", credentials: "same-origin", headers: {Accept: "application/json"}};
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), requestOptions.timeout || 70000);
  options.signal = controller.signal;
  if (view.state.account_id) options.headers["X-Matrix-Account"] = view.state.account_id;
  if (data !== undefined) {
    options.method = "POST";
    options.headers["X-Matrix-Messenger"] = "1";
    if (data instanceof FormData) options.body = data;
    else {
      options.headers["Content-Type"] = "application/json";
      options.body = JSON.stringify(data);
    }
  }
  let response;
  try {
    response = await fetch(new URL(path, document.baseURI), options);
  } catch (error) {
    window.clearTimeout(timeout);
    if (error.name === "AbortError") {
      const timedOut = new Error("Die Anfrage dauert zu lange. Verbindung und App-Protokoll prüfen und anschließend erneut versuchen.");
      timedOut.code = "request_timeout";
      throw timedOut;
    }
    throw new Error("Die Verbindung zum Messenger wurde unterbrochen.");
  }
  let value;
  try {
    value = await response.json();
  } catch (error) {
    window.clearTimeout(timeout);
    if (error.name === "AbortError") {
      const timedOut = new Error("Die Antwort dauert zu lange. Verbindung und App-Protokoll prüfen und anschließend erneut versuchen.");
      timedOut.code = "request_timeout";
      throw timedOut;
    }
    throw new Error("Der Messenger ist derzeit nicht erreichbar.");
  }
  window.clearTimeout(timeout);
  if (path !== "api/state" && path !== "api/settings" && path !== "api/account/logout" && generation !== view.accountGeneration) {
    const error = new Error("Das Matrix-Konto wurde gewechselt. Die vorherige Anfrage wurde verworfen.");
    error.staleAccount = true;
    throw error;
  }
  if (!response.ok) {
    const error = new Error(value.error || "Die Aktion konnte nicht ausgeführt werden.");
    error.code = value.code;
    error.details = value.details;
    throw error;
  }
  return value;
}

function roomById(id) {
  return (view.state.rooms || []).find(room => room.room_id === id);
}

function roomName(room) {
  return room.name || room.display_name || room.room_id || "Matrix-Chat";
}

function avatarText(room) {
  return room.is_direct ? roomName(room).replace(/^@/, "").slice(0, 2).toUpperCase() : "#";
}

function dateValue(timestamp) {
  const value = new Date(Number(timestamp || 0));
  return Number.isNaN(value.getTime()) ? new Date(0) : value;
}

function roomTime(timestamp) {
  if (!timestamp) return "";
  const date = dateValue(timestamp);
  const today = new Date();
  return date.toDateString() === today.toDateString() ? timeFormat.format(date) : date.toLocaleDateString("de-DE", {day: "2-digit", month: "2-digit"});
}

function renderRooms() {
  const list = byId("room-list");
  const rooms = [...(view.state.rooms || [])].filter(room => {
    if (view.filter === "direct" && !room.is_direct) return false;
    if (view.filter === "rooms" && room.is_direct) return false;
    return (roomName(room) + " " + room.room_id).toLowerCase().includes(view.query);
  }).sort((left, right) => Number(right.last_message_ts || 0) - Number(left.last_message_ts || 0) || roomName(left).localeCompare(roomName(right), "de"));
  const fragment = document.createDocumentFragment();
  for (const room of rooms) {
    const card = element("button", "room-card" + (room.room_id === view.selected ? " selected" : ""));
    card.type = "button";
    card.setAttribute("aria-label", roomName(room) + (room.membership === "invite" ? ", Einladung" : ""));
    if (room.room_id === view.selected) card.setAttribute("aria-current", "true");
    card.append(element("span", "room-avatar" + (room.is_direct ? " direct" : ""), avatarText(room)));
    const info = element("span", "room-info");
    const top = element("span", "room-topline");
    top.append(element("span", "room-name", roomName(room)), element("time", "room-time", roomTime(room.last_message_ts)));
    const preview = element("span", "room-preview");
    const last = room.last_message;
    const summary = room.membership === "invite" ? "Einladung zum Chat" : typeof last === "string" ? last : last?.body || (room.encrypted ? "Verschlüsselter Chat" : room.is_direct ? "Direktnachricht" : "Matrix-Raum");
    preview.append(element("span", "", summary));
    if (Number(room.unread_count) > 0) preview.append(element("span", "unread-badge", Number(room.unread_count) > 99 ? "99+" : room.unread_count));
    info.append(top, preview);
    card.append(info);
    card.addEventListener("click", () => selectRoom(room.room_id));
    fragment.append(card);
  }
  if (!rooms.length) fragment.append(element("p", "sidebar-empty", view.query ? "Kein Chat passt zu deiner Suche." : view.state.connected ? "Noch keine Chats. Starte eine Direktnachricht oder nimm eine Einladung an." : "Die Chatliste erscheint, sobald Matrix verbunden ist. Bereits geladene Chats bleiben hier verfügbar."));
  list.replaceChildren(fragment);
  byId("list-caption").textContent = rooms.length ? `DEINE CHATS · ${rooms.length}` : "DEINE CHATS";
}

function renderStatus() {
  const connected = Boolean(view.state.connected);
  byId("matrix-status").textContent = connected ? "Mit Matrix verbunden" : "Matrix nicht verbunden";
  byId("matrix-dot").className = "status-dot " + (connected ? "connected" : "error");
  byId("account-name").textContent = view.state.user_id || "Matrix-Konto einrichten";
  byId("account-name").title = view.state.user_id || "";
  byId("welcome-error").textContent = view.state.error || "";
  const radio = view.state.radio || {};
  const labels = {disabled: "Meshtastic ausgeschaltet", blocked: "Funkweiterleitung gesperrt", connecting: "Meshtastic verbindet …", connected: "Meshtastic verbunden", disconnected: "Meshtastic nicht verbunden"};
  byId("mesh-status").textContent = labels[radio.state] || "Meshtastic nicht verbunden";
  byId("mesh-detail").textContent = radio.reason || (radio.state === "connected" ? "Privater Channel für die Bridge bereit" : "Matrix ist unabhängig davon verfügbar");
  byId("welcome-new-chat").disabled = !connected;
  byId("new-chat").disabled = !connected;
  renderHeader();
  renderVerification();
  renderManagementStatus();
}

function renderHeader() {
  const room = roomById(view.selected);
  if (!room) return;
  byId("chat-title").textContent = roomName(room);
  byId("chat-subtitle").textContent = room.membership === "invite" ? "Einladung" : room.room_id;
  byId("header-avatar").textContent = avatarText(room);
  byId("header-avatar").className = "room-avatar" + (room.is_direct ? " direct" : "");
  byId("encryption-label").textContent = room.encrypted ? "◇ Ende-zu-Ende verschlüsselt" : "Matrix-Raum";
  const invited = room.membership === "invite";
  const joined = !room.membership || room.membership === "join";
  show(byId("invitation"), invited);
  show(byId("composer"), !invited);
  byId("join-room").disabled = !view.state.connected;
  byId("room-settings").disabled = !view.state.connected || !joined;
  byId("message-input").disabled = !view.state.connected || !joined || view.sending.has(room.room_id);
  byId("send-message").disabled = byId("message-input").disabled || !byId("message-input").value.trim();
  byId("send-message").textContent = view.sending.has(room.room_id) ? "…" : "↑";
  const notice = view.state.error || (!view.state.connected ? "Matrix ist offline. Du kannst den bereits geladenen Verlauf weiterhin lesen." : "");
  errorText(byId("chat-notice"), notice);
  const missingKeys = (view.state.diagnostics?.rooms_missing_keys || []).some(item => item.room_id === room.room_id && item.count > 0);
  show(byId("crypto-notice"), missingKeys);
}

function historyFor(roomId) {
  if (!view.histories.has(roomId)) view.histories.set(roomId, {messages: new Map(), next: null, initialized: false, signature: ""});
  return view.histories.get(roomId);
}

function renderTimeline(roomId, mode = "refresh") {
  if (view.selected !== roomId) return;
  const history = historyFor(roomId);
  const messages = [...history.messages.values()].sort((left, right) => Number(left.timestamp_ms || 0) - Number(right.timestamp_ms || 0) || String(left.event_id).localeCompare(String(right.event_id)));
  const signature = JSON.stringify(messages.map(message => [message.event_id, message.body, message.sender, message.timestamp_ms, message.decryption_error]));
  show(byId("load-history"), Boolean(history.next));
  show(byId("timeline-empty"), !messages.length);
  byId("timeline-empty").textContent = roomById(roomId)?.membership === "invite" ? "Nach Annahme der Einladung erscheint hier der Verlauf." : history.initialized ? "Hier beginnt euer Gespräch." : "Nachrichten werden geladen …";
  if (signature === history.signature && mode !== "initial") return;
  const timeline = byId("timeline");
  const previousHeight = timeline.scrollHeight;
  const previousTop = timeline.scrollTop;
  const wasAtBottom = previousHeight - previousTop - timeline.clientHeight < 90;
  const fragment = document.createDocumentFragment();
  let previousDay = "";
  let previousSender = "";
  for (const message of messages) {
    const date = dateValue(message.timestamp_ms);
    const day = date.toDateString();
    if (day !== previousDay) {
      fragment.append(element("div", "day-divider", dayFormat.format(date)));
      previousDay = day;
      previousSender = "";
    }
    const own = message.sender === view.state.user_id;
    const row = element("article", "message-row" + (own ? " own" : ""));
    if (!own && previousSender !== message.sender) row.append(element("div", "message-author", message.displayname || message.display_name || message.sender || "Matrix"));
    const bubble = element("div", "message-bubble");
    const locked = Boolean(message.decryption_error || message.decryption_pending || message.pending || message.type === "m.room.encrypted");
    bubble.append(element("p", "message-body" + (locked ? " locked" : ""), locked ? "Verschlüsselte Nachricht · Raumschlüssel noch nicht verfügbar" : message.body || ""));
    const time = element("time", "message-meta", timeFormat.format(date));
    time.dateTime = date.toISOString();
    bubble.append(time);
    row.append(bubble);
    fragment.append(row);
    previousSender = message.sender;
  }
  byId("messages").replaceChildren(fragment);
  history.signature = signature;
  if (mode === "older") timeline.scrollTop = previousTop + timeline.scrollHeight - previousHeight;
  else if (mode === "initial" || wasAtBottom) timeline.scrollTop = timeline.scrollHeight;
  else timeline.scrollTop = previousTop;
}

async function loadHistory(roomId, older = false) {
  const generation = view.accountGeneration;
  if (!roomId || view.loading.has(roomId)) return;
  const room = roomById(roomId);
  if (room?.membership === "invite") return;
  const history = historyFor(roomId);
  if (older && !history.next) return;
  const wasInitialized = history.initialized;
  const before = older ? history.next : null;
  view.loading.add(roomId);
  if (older) byId("load-history").disabled = true;
  try {
    const suffix = before ? `?limit=50&before=${encodeURIComponent(before)}` : "?limit=50";
    const result = await api(`api/rooms/${encodeURIComponent(roomId)}/messages${suffix}`);
    for (const message of result.messages || []) {
      if (message.event_id) history.messages.set(message.event_id, message);
    }
    if (!wasInitialized || older) history.next = result.next_batch && result.next_batch !== before ? result.next_batch : null;
    history.initialized = true;
    renderTimeline(roomId, older ? "older" : wasInitialized ? "refresh" : "initial");
    if (view.selected === roomId) {
      if (byId("composer-error").dataset.source === "history") errorText(byId("composer-error"), "");
      markRead(roomId);
    }
  } catch (error) {
    if (error.staleAccount || generation !== view.accountGeneration) return;
    if (view.selected === roomId) {
      byId("composer-error").dataset.source = "history";
      errorText(byId("composer-error"), error.message);
    }
  } finally {
    if (generation === view.accountGeneration) {
      view.loading.delete(roomId);
      byId("load-history").disabled = false;
    }
  }
}

async function markRead(roomId) {
  const generation = view.accountGeneration;
  if (!view.state.connected || document.hidden) return;
  if (window.matchMedia("(max-width: 620px)").matches && !byId("messenger").classList.contains("chat-open")) return;
  const history = historyFor(roomId);
  const latest = [...history.messages.values()].sort((left, right) => Number(right.timestamp_ms || 0) - Number(left.timestamp_ms || 0))[0]?.event_id;
  if (!latest || view.reads.get(roomId) === latest) return;
  view.reads.set(roomId, latest);
  try {
    await api(`api/rooms/${encodeURIComponent(roomId)}/read`, {});
  } catch {
    if (generation === view.accountGeneration) view.reads.delete(roomId);
  }
}

async function selectRoom(roomId) {
  if (view.selected) view.drafts.set(view.selected, byId("message-input").value);
  if (view.selected !== roomId) {
    view.verificationTargets = [];
    view.verificationTargetRoom = null;
    view.verificationNotice = "";
  }
  view.selected = roomId;
  byId("messenger").classList.add("chat-open");
  show(byId("welcome"), false);
  show(byId("chat-view"), true);
  byId("messages").replaceChildren();
  byId("message-input").value = view.drafts.get(roomId) || "";
  errorText(byId("composer-error"), "");
  show(byId("composer-security"), false);
  renderRooms();
  renderHeader();
  const history = historyFor(roomId);
  history.signature = "";
  show(byId("timeline-empty"), !history.initialized);
  byId("timeline-empty").textContent = history.initialized ? "Hier beginnt euer Gespräch." : "Nachrichten werden geladen …";
  renderTimeline(roomId, "initial");
  await loadHistory(roomId);
}

function transactionId() {
  if (crypto.randomUUID) return crypto.randomUUID();
  const values = new Uint8Array(16);
  crypto.getRandomValues(values);
  return [...values].map(value => value.toString(16).padStart(2, "0")).join("");
}

async function sendMessage(event) {
  event.preventDefault();
  const generation = view.accountGeneration;
  const roomId = view.selected;
  const input = byId("message-input");
  const text = input.value;
  if (!roomId || !text.trim() || view.sending.has(roomId) || !view.state.connected) return;
  let transaction = view.transactions.get(roomId);
  if (!transaction || transaction.text !== text) {
    transaction = {id: transactionId(), text};
    view.transactions.set(roomId, transaction);
  }
  view.sending.add(roomId);
  errorText(byId("composer-error"), "");
  show(byId("composer-security"), false);
  renderHeader();
  try {
    const result = await api(`api/rooms/${encodeURIComponent(roomId)}/send`, {text, transaction_id: transaction.id});
    view.transactions.delete(roomId);
    view.drafts.set(roomId, "");
    if (result.event_id) historyFor(roomId).messages.set(result.event_id, {event_id: result.event_id, sender: view.state.user_id, body: text, timestamp_ms: Date.now(), room_id: roomId});
    if (view.selected === roomId) {
      input.value = "";
      renderTimeline(roomId);
    }
    await loadHistory(roomId);
  } catch (error) {
    if (error.staleAccount || generation !== view.accountGeneration) return;
    if (view.selected === roomId) {
      view.verificationTargets = error.code === "unverified_devices" && Array.isArray(error.details?.devices) ? error.details.devices.filter(device => typeof device.user_id === "string" && typeof device.device_id === "string").map(device => ({...device})) : [];
      view.verificationTargetRoom = error.code === "unverified_devices" ? roomId : null;
      byId("composer-error").dataset.source = "send";
      errorText(byId("composer-error"), error.message + " Dein Text bleibt erhalten. Erneutes Senden verwendet dieselbe Nachrichten-ID.");
      show(byId("composer-security"), ["unverified_devices", "encryption_not_ready"].includes(error.code));
    }
  } finally {
    if (generation === view.accountGeneration) {
      view.sending.delete(roomId);
      renderHeader();
    }
  }
}

function clearAccountView() {
  view.accountGeneration += 1;
  view.histories = new Map();
  view.drafts = new Map();
  view.transactions = new Map();
  view.reads = new Map();
  view.loading = new Set();
  view.sending = new Set();
  view.selected = null;
  view.activeVerification = null;
  view.verificationTargets = [];
  view.verificationTargetRoom = null;
  view.verificationNotice = "";
  view.fingerprintDevice = null;
  view.managedRoom = null;
  view.settings = null;
  view.query = "";
  view.filter = "all";
  for (const tab of document.querySelectorAll("[data-filter]")) {
    const active = tab.dataset.filter === "all";
    tab.classList.toggle("active", active);
    tab.setAttribute("aria-pressed", String(active));
  }
  byId("search").value = "";
  byId("messages").replaceChildren();
  byId("message-input").value = "";
  byId("chat-title").textContent = "";
  byId("chat-subtitle").textContent = "";
  byId("header-avatar").textContent = "";
  byId("encryption-label").textContent = "";
  byId("managed-room-id").value = "";
  byId("room-dialog-title").textContent = "Raum verwalten";
  byId("leave-confirm-text").textContent = "";
  byId("verification-description").textContent = "";
  byId("logout-confirm-text").textContent = "";
  show(byId("logout-confirm"), false);
  byId("verification-devices").replaceChildren();
  byId("verification-emoji").replaceChildren();
  byId("fingerprint-value").textContent = "";
  byId("fingerprint-device").textContent = "";
  byId("key-import-result").textContent = "";
  byId("key-import-form").removeAttribute("aria-busy");
  byId("import-keys").disabled = false;
  byId("import-keys").textContent = "Schlüssel importieren";
  errorText(byId("composer-error"), "");
  show(byId("composer-security"), false);
  show(byId("crypto-notice"), false);
  for (const dialog of document.querySelectorAll("dialog")) {
    if (dialog.open) dialog.close();
    for (const form of dialog.querySelectorAll("form")) form.reset();
  }
  clearSecretInputs();
  byId("settings-bridge-room").replaceChildren();
  show(byId("welcome"), true);
  show(byId("chat-view"), false);
  byId("messenger").classList.remove("chat-open");
}

async function refreshState() {
  const generation = view.accountGeneration;
  const previousAccount = view.state.account_id || view.state.user_id;
  const state = await api("api/state");
  if (generation !== view.accountGeneration) return false;
  const nextAccount = state.account_id || state.user_id;
  const switched = Boolean(previousAccount && previousAccount !== nextAccount);
  if (switched) clearAccountView();
  view.state = {...state, rooms: Array.isArray(state.rooms) ? state.rooms : [], verification: Array.isArray(state.verification) ? state.verification : [], devices: Array.isArray(state.devices) ? state.devices : []};
  renderStatus();
  renderRooms();
  if (view.selected && !roomById(view.selected)) {
    view.selected = null;
    show(byId("welcome"), true);
    show(byId("chat-view"), false);
    byId("messenger").classList.remove("chat-open");
  }
  if (switched) await loadSettings(false);
  return true;
}

async function poll() {
  if (view.polling) return;
  view.polling = true;
  try {
    await refreshState();
    if (view.selected) await loadHistory(view.selected);
  } catch (error) {
    if (error.staleAccount) return;
    view.state.connected = false;
    view.state.error = error.message;
    renderStatus();
  } finally {
    view.polling = false;
    window.setTimeout(poll, document.hidden ? 10000 : 3000);
  }
}

function openDirect() {
  errorText(byId("direct-error"), "");
  byId("direct-dialog").showModal();
  byId("direct-user").focus();
}

async function createDirect(event) {
  event.preventDefault();
  const generation = view.accountGeneration;
  const userId = byId("direct-user").value.trim();
  byId("create-direct").disabled = true;
  errorText(byId("direct-error"), "");
  try {
    const result = await api("api/direct", {user_id: userId});
    await refreshState();
    if (generation !== view.accountGeneration) return;
    if (result.room_id) {
      if (!roomById(result.room_id)) view.state.rooms.push({room_id: result.room_id, name: userId, is_direct: true, membership: "join"});
      await selectRoom(result.room_id);
    }
    byId("direct-dialog").close();
    byId("direct-user").value = "";
  } catch (error) {
    errorText(byId("direct-error"), error.message);
  } finally {
    byId("create-direct").disabled = false;
  }
}

async function joinRoom() {
  const roomId = view.selected;
  byId("join-room").disabled = true;
  try {
    await api(`api/rooms/${encodeURIComponent(roomId)}/join`, {});
    await refreshState();
    await loadHistory(roomId);
  } catch (error) {
    errorText(byId("chat-notice"), error.message);
  } finally {
    byId("join-room").disabled = !view.state.connected;
  }
}

function activeVerification() {
  const verifications = view.state.verification || [];
  const finished = item => ["done", "verified", "cancelled", "canceled", "failed"].includes(item.state);
  const pinned = byId("verification-dialog").open && verifications.find(item => item.transaction_id === view.activeVerification);
  if (pinned && finished(pinned)) {
    view.verificationNotice = ["done", "verified"].includes(pinned.state) ? "Die Gerätebestätigung ist abgeschlossen." : "Die Gerätebestätigung wurde beendet. Du kannst unten eine neue Bestätigung starten.";
    view.activeVerification = null;
  }
  return pinned && !finished(pinned) ? pinned : verifications.find(item => !finished(item));
}

function renderVerification() {
  const verification = activeVerification();
  show(byId("verification-banner"), Boolean(verification && !["done", "verified", "cancelled", "canceled", "failed"].includes(verification.state)));
  if (verification) byId("verification-banner-text").textContent = "Gerätebestätigung mit " + (verification.user_id || "Matrix");
  if (!byId("verification-dialog").open) return;
  const devices = byId("verification-devices");
  devices.replaceChildren();
  const emoji = byId("verification-emoji");
  emoji.replaceChildren();
  const confirm = byId("verification-confirm");
  const cancel = byId("verification-cancel");
  show(byId("security-guide"), !verification);
  const pendingKeys = Number(view.state.diagnostics?.pending_encrypted_count || 0);
  byId("key-diagnostics").textContent = pendingKeys ? `${pendingKeys} gespeicherte Nachrichten warten auf Raumschlüssel. Für alte Nachrichten kann ein Schlüsselimport erforderlich sein.` : "Zurzeit sind keine fehlenden Raumschlüssel bekannt.";
  byId("retry-keys").disabled = !view.state.connected;
  show(confirm, Boolean(verification));
  show(cancel, Boolean(verification));
  if (!verification) {
    const deviceKey = device => JSON.stringify([device.user_id, device.device_id]);
    const isCurrent = device => device.own_device || device.device_id === view.state.device_id && device.user_id === view.state.user_id;
    const targets = view.verificationTargetRoom === view.selected ? view.verificationTargets : [];
    const targetKeys = new Set(targets.map(deviceKey));
    const allDevices = new Map(targets.map(device => [deviceKey(device), {...device}]));
    for (const device of view.state.devices || []) allDevices.set(deviceKey(device), {...allDevices.get(deviceKey(device)), ...device});
    const needsConfirmation = device => targetKeys.has(deviceKey(device)) && !device.verified && !isCurrent(device);
    const required = [...allDevices.values()].filter(needsConfirmation);
    byId("verification-description").textContent = [view.verificationNotice, required.length ? "Für den Versand in diesem Chat die markierten Geräte bestätigen. Ein Schlüsselimport bestätigt keine Geräte." : "", "Bestätige Geräte, um verschlüsselte Chats zu verwenden. Die Bestätigung muss auf beiden Geräten übereinstimmen. Dieses Gerät: " + (view.state.device_id || "noch nicht verbunden") + "."].filter(Boolean).join(" ");
    for (const device of [...allDevices.values()].sort((left, right) => Number(needsConfirmation(right)) - Number(needsConfirmation(left)))) {
      const row = element("div", "device-row");
      const copy = element("div", "device-copy");
      copy.append(element("strong", "", device.display_name || device.device_id), element("small", "", `${device.user_id || ""} · ${device.device_id || ""}`));
      if (needsConfirmation(device)) copy.append(element("strong", "", "Für Versand bestätigen"));
      row.append(copy);
      if (isCurrent(device)) row.append(element("span", "verified-label", "Dieses Gerät"));
      else if (device.verified) row.append(element("span", "verified-label", "Bestätigt"));
      else {
        const actions = element("div", "device-actions");
        const button = element("button", "quiet-button", "Symbole vergleichen");
        button.disabled = !view.state.connected;
        button.addEventListener("click", () => startVerification(device, button));
        actions.append(button);
        if (device.fingerprint) {
          const fingerprint = element("button", "quiet-button", "Fingerabdruck vergleichen");
          fingerprint.disabled = !view.state.connected;
          fingerprint.addEventListener("click", () => openFingerprint(device));
          actions.append(fingerprint);
        }
        row.append(actions);
      }
      devices.append(row);
    }
    if (!allDevices.size) devices.append(element("p", "sidebar-empty", "Noch keine Geräte verfügbar. Öffne einen verschlüsselten Chat oder starte die Bestätigung in einem anderen Matrix-Client."));
    return;
  }
  view.verificationNotice = "";
  view.activeVerification = verification.transaction_id;
  const values = verification.emoji || verification.emojis || [];
  const state = verification.state || "requested";
  const complete = ["done", "verified"].includes(state);
  const cancelled = ["cancelled", "canceled", "failed"].includes(state);
  const canCompare = values.length > 0 && !complete && !cancelled;
  byId("verification-description").textContent = complete ? "Die Gerätebestätigung ist abgeschlossen." : cancelled ? "Die Gerätebestätigung wurde beendet. Du kannst eine neue Bestätigung starten." : state === "confirmed" ? "Du hast die Symbole bestätigt. Warte auf die Bestätigung des anderen Geräts." : canCompare ? "Vergleiche diese Symbole mit dem anderen Gerät. Bestätige nur, wenn alle Symbole in derselben Reihenfolge übereinstimmen." : `${verification.user_id || "Matrix"} · ${verification.device_id || ""}. ` + (["requested", "request", "started", "start", "incoming"].includes(state) ? "Nimm die Anfrage an und vergleiche anschließend die Symbole auf beiden Geräten." : "Warte auf das andere Gerät. Dort die Gerätebestätigung öffnen.");
  for (const item of values) {
    const symbol = Array.isArray(item) ? item[0] : item.emoji || item.symbol || "";
    const description = Array.isArray(item) ? item[1] : item.description || item.name || "";
    const tile = element("div", "verification-item");
    tile.append(element("span", "", symbol), element("small", "", description));
    emoji.append(tile);
  }
  confirm.textContent = canCompare ? "Symbole stimmen überein" : "Anfrage annehmen";
  confirm.dataset.action = canCompare ? "confirm" : "accept";
  confirm.disabled = !view.state.connected || complete || cancelled || state === "confirmed" || !canCompare && !["requested", "request", "started", "start", "incoming"].includes(state);
  cancel.textContent = complete || cancelled ? "Schließen" : "Abbrechen";
  cancel.dataset.action = complete || cancelled ? "close" : "cancel";
}

function openVerification() {
  errorText(byId("verification-error"), "");
  if (!byId("verification-dialog").open) {
    view.verificationNotice = "";
    byId("verification-dialog").showModal();
  }
  renderVerification();
}

async function startVerification(device, button) {
  button.disabled = true;
  errorText(byId("verification-error"), "");
  try {
    const result = await api("api/verification/start", {user_id: device.user_id, device_id: device.device_id});
    view.activeVerification = result.transaction_id || null;
    await refreshState();
  } catch (error) {
    errorText(byId("verification-error"), error.message);
    button.disabled = false;
  }
}

async function verificationAction(action) {
  if (action === "close") {
    byId("verification-dialog").close();
    view.activeVerification = null;
    return;
  }
  const verification = activeVerification();
  if (!verification) return;
  byId("verification-confirm").disabled = true;
  errorText(byId("verification-error"), "");
  try {
    await api(`api/verification/${encodeURIComponent(verification.transaction_id)}`, {action});
    if (action === "cancel") {
      byId("verification-dialog").close();
      view.activeVerification = null;
    }
    await refreshState();
  } catch (error) {
    errorText(byId("verification-error"), error.message);
  } finally {
    renderVerification();
  }
}

const settingInputs = {
  matrix: {homeserver: "homeserver", user_id: "user-id", password: "password", access_token: "access-token", device_id: "device-id", room_id: "room-id"},
  meshtastic: {enabled: "enabled", connection: "connection", channel_index: "channel-index", channel_psk: "channel-psk", tcp_host: "tcp-host", tcp_port: "tcp-port", serial_device: "serial-device", ble_address: "ble-address", mqtt_host: "mqtt-host", mqtt_port: "mqtt-port", mqtt_username: "mqtt-username", mqtt_password: "mqtt-password", mqtt_tls: "mqtt-tls", mqtt_topic: "mqtt-topic", mqtt_channel_name: "mqtt-channel-name", mqtt_gateway_id: "mqtt-gateway-id"},
  forwarding: {include_sender: "include-sender", max_message_age_seconds: "max-age", message_interval_seconds: "message-interval", long_message: "long-message", max_parts: "max-parts"}
};
const secretSettings = ["password", "access_token", "channel_psk", "mqtt_password"];

function clearSecretInputs() {
  for (const field of ["password", "access-token", "channel-psk", "mqtt-password"]) {
    byId("settings-" + field).value = "";
    byId("settings-" + field + "-clear").checked = false;
  }
  byId("settings-channel-psk").type = "password";
  byId("show-channel-psk").checked = false;
}

function setSettingsTab(name) {
  for (const button of document.querySelectorAll("[data-settings-tab]")) {
    button.classList.toggle("active", button.dataset.settingsTab === name);
    button.setAttribute("aria-pressed", String(button.dataset.settingsTab === name));
  }
  for (const section of document.querySelectorAll("[data-settings-section]")) show(section, section.dataset.settingsSection === name);
}

function renderRadioFields() {
  show(byId("radio-fields"), byId("settings-enabled").checked);
  for (const section of document.querySelectorAll("[data-radio-connection]")) show(section, section.dataset.radioConnection === byId("settings-connection").value);
}

function populateBridgeRooms(value) {
  const selector = byId("settings-bridge-room");
  selector.replaceChildren();
  const empty = element("option", "", "Keinen Raum weiterleiten");
  empty.value = "";
  selector.append(empty);
  for (const room of view.state.rooms || []) {
    if (room.membership && room.membership !== "join") continue;
    const option = element("option", "", roomName(room));
    option.value = room.room_id;
    selector.append(option);
  }
  if (value && ![...selector.options].some(option => option.value === value)) {
    const option = element("option", "", "Manuell: " + value);
    option.value = value;
    selector.append(option);
  }
  selector.value = value || "";
}

function fillSettings(value) {
  view.settings = value;
  const defaults = {
    matrix: {homeserver: "https://matrix.org", user_id: "", device_id: "", room_id: ""},
    meshtastic: {enabled: false, connection: "tcp", channel_index: 0, tcp_host: "", tcp_port: 4403, serial_device: "", ble_address: "", mqtt_host: "", mqtt_port: 1883, mqtt_username: "", mqtt_tls: false, mqtt_topic: "msh/EU_868/2/e", mqtt_channel_name: "", mqtt_gateway_id: ""},
    forwarding: {include_sender: true, max_message_age_seconds: 300, message_interval_seconds: 5, long_message: "split", max_parts: 4}
  };
  for (const [group, fields] of Object.entries(settingInputs)) for (const [key, suffix] of Object.entries(fields)) {
    const input = byId("settings-" + suffix);
    if (secretSettings.includes(key)) input.value = "";
    else if (input.type === "checkbox") input.checked = Boolean(value[group]?.[key] ?? defaults[group][key]);
    else input.value = value[group]?.[key] ?? defaults[group][key] ?? "";
  }
  for (const [group, key, suffix] of [["matrix", "password", "password"], ["matrix", "access_token", "access-token"], ["meshtastic", "channel_psk", "channel-psk"], ["meshtastic", "mqtt_password", "mqtt-password"]]) {
    byId("settings-" + suffix + "-status").textContent = value[group]?.["has_" + key] ? "Gespeichert. Leer lassen, um den vorhandenen Wert zu behalten." : "Noch nicht gespeichert.";
  }
  clearSecretInputs();
  populateBridgeRooms(value.matrix?.room_id || "");
  renderRadioFields();
  renderManagementStatus();
}

async function loadSettings(autoOpen = false) {
  try {
    const value = await api("api/settings");
    view.settings = value;
    renderManagementStatus();
    if (autoOpen && !view.setupOpened && (value.configured === false || value.config_valid === false)) {
      view.setupOpened = true;
      fillSettings(value);
      setSettingsTab("matrix");
      byId("settings-dialog").showModal();
    }
    return value;
  } catch (error) {
    if (error.staleAccount) return null;
    if (byId("settings-dialog").open) errorText(byId("settings-error"), error.message);
    return null;
  }
}

function renderManagementStatus() {
  const needsSetup = view.state.setup_required === true || view.settings?.configured === false || view.settings?.config_valid === false;
  byId("settings-title").textContent = needsSetup ? "Matrix einrichten" : "Einstellungen";
  byId("settings-eyebrow").textContent = needsSetup ? "ERSTE EINRICHTUNG" : "VERWALTUNG";
  byId("settings-intro").textContent = needsSetup ? "Melde dein Matrix-Konto an. Meshtastic kannst du später einrichten." : "Matrix und die optionale Funkweiterleitung direkt hier verwalten.";
  show(byId("welcome-setup"), needsSetup);
  show(byId("welcome-new-chat"), !needsSetup);
  byId("open-rooms").disabled = !view.state.connected;
  byId("settings-security").disabled = !view.state.connected;
  show(byId("logout-account"), view.settings?.configured === true);
  const status = view.state.connected ? "Verbunden. Deine Chats werden automatisch abgeglichen. Verschlüsselte Chats bei Bedarf unter Geräte & Schlüssel bestätigen." : view.state.error || view.settings?.validation_error || "Speichere die Anmeldung. Danach werden deine Chats automatisch synchronisiert.";
  byId("setup-progress").textContent = status;
}

async function openSettings() {
  errorText(byId("settings-error"), "");
  errorText(byId("settings-result"), "");
  setSettingsTab("matrix");
  if (!byId("settings-dialog").open) byId("settings-dialog").showModal();
  byId("save-settings").disabled = true;
  const inputs = [...byId("settings-form").querySelectorAll("input, select")];
  for (const input of inputs) input.disabled = true;
  const value = await loadSettings();
  if (value) fillSettings(value);
  for (const input of inputs) input.disabled = !value;
  byId("save-settings").disabled = !value;
}

async function saveSettings(event) {
  event.preventDefault();
  const payload = {matrix: {}, meshtastic: {}, forwarding: {}};
  for (const [group, fields] of Object.entries(settingInputs)) for (const [key, suffix] of Object.entries(fields)) {
    const input = byId("settings-" + suffix);
    payload[group][key] = input.type === "checkbox" ? input.checked : input.type === "number" ? Number(input.value) : secretSettings.includes(key) ? input.value : input.value.trim();
    if (secretSettings.includes(key)) payload[group][key + "_clear"] = byId("settings-" + suffix + "-clear").checked;
  }
  byId("save-settings").disabled = true;
  errorText(byId("settings-error"), "");
  errorText(byId("settings-result"), "");
  try {
    const value = await api("api/settings", payload);
    fillSettings(value);
    errorText(byId("settings-result"), "Gespeichert. Die Verbindung wird jetzt mit diesen Einstellungen hergestellt.");
    await refreshState();
  } catch (error) {
    errorText(byId("settings-error"), error.message);
  } finally {
    clearSecretInputs();
    byId("save-settings").disabled = false;
  }
}

async function discoverSerial() {
  byId("discover-serial").disabled = true;
  try {
    const result = await api("api/devices/discover");
    const list = byId("serial-devices");
    list.replaceChildren();
    for (const device of result.serial || []) {
      const option = element("option", "", device.label || device.path);
      option.value = device.path;
      list.append(option);
    }
    if ((result.serial || []).length === 1 && !byId("settings-serial-device").value) byId("settings-serial-device").value = result.serial[0].path;
    byId("serial-status").textContent = result.serial?.length ? `${result.serial.length} USB-Geräte gefunden. Das passende Gerät auswählen.` : "Kein serielles Gerät gefunden. Verbindung und Gerätezugriff prüfen.";
  } catch (error) {
    byId("serial-status").textContent = error.message;
  } finally {
    byId("discover-serial").disabled = false;
  }
}

function showRoomsDialog() {
  errorText(byId("rooms-error"), "");
  byId("rooms-dialog").showModal();
}

async function activateRoom(result, fallbackName) {
  const generation = view.accountGeneration;
  await refreshState();
  if (generation !== view.accountGeneration) return false;
  if (result.room_id) {
    if (!roomById(result.room_id)) view.state.rooms.push({room_id: result.room_id, name: fallbackName || result.room_id, membership: "join"});
    await selectRoom(result.room_id);
  }
  return true;
}

async function createRoom(event) {
  event.preventDefault();
  const name = byId("new-room-name").value.trim();
  const invite = byId("new-room-invite").value.split(/[\s,;]+/).filter(Boolean);
  byId("create-room").disabled = true;
  errorText(byId("rooms-error"), "");
  try {
    const result = await api("api/rooms/create", {name, invite, encrypted: byId("new-room-encrypted").checked});
    if (!await activateRoom(result, name)) return;
    byId("rooms-dialog").close();
    byId("create-room-form").reset();
  } catch (error) {
    errorText(byId("rooms-error"), error.message);
  } finally {
    byId("create-room").disabled = false;
  }
}

async function joinExisting(event) {
  event.preventDefault();
  byId("join-existing").disabled = true;
  errorText(byId("rooms-error"), "");
  try {
    const result = await api("api/rooms/join", {room_id: byId("join-existing-id").value.trim()});
    if (!await activateRoom(result)) return;
    byId("rooms-dialog").close();
    byId("join-existing-form").reset();
  } catch (error) {
    errorText(byId("rooms-error"), error.message);
  } finally {
    byId("join-existing").disabled = false;
  }
}

function openRoomSettings() {
  const room = roomById(view.selected);
  if (!room) return;
  view.managedRoom = room.room_id;
  byId("room-dialog-title").textContent = roomName(room);
  byId("managed-room-id").value = room.room_id;
  byId("managed-room-name").value = roomName(room);
  byId("invite-room-user").value = "";
  errorText(byId("room-error"), "");
  errorText(byId("room-result"), "");
  show(byId("leave-confirm"), false);
  byId("room-dialog").showModal();
}

async function roomAction(path, payload, message, buttonId) {
  const generation = view.accountGeneration;
  if (!view.managedRoom) return;
  byId(buttonId).disabled = true;
  errorText(byId("room-error"), "");
  errorText(byId("room-result"), "");
  try {
    await api(`api/rooms/${encodeURIComponent(view.managedRoom)}/${path}`, payload);
    await refreshState();
    if (generation !== view.accountGeneration) return;
    errorText(byId("room-result"), message);
    if (path === "leave") byId("room-dialog").close();
  } catch (error) {
    errorText(byId("room-error"), error.message);
  } finally {
    byId(buttonId).disabled = false;
  }
}

function openFingerprint(device) {
  view.fingerprintDevice = {user_id: device.user_id, device_id: device.device_id, fingerprint: device.fingerprint};
  byId("fingerprint-device").textContent = `${device.user_id} · ${device.device_id}`;
  byId("fingerprint-value").textContent = String(device.fingerprint).replace(/\s/g, "").match(/.{1,4}/g)?.join(" ") || device.fingerprint;
  byId("fingerprint-confirmed").checked = false;
  byId("fingerprint-submit").disabled = true;
  errorText(byId("fingerprint-error"), "");
  byId("fingerprint-dialog").showModal();
}

async function verifyFingerprint(event) {
  event.preventDefault();
  if (!view.fingerprintDevice || !byId("fingerprint-confirmed").checked) return;
  byId("fingerprint-submit").disabled = true;
  try {
    await api("api/verification/device", {...view.fingerprintDevice, confirmed: true});
    byId("fingerprint-dialog").close();
    await refreshState();
  } catch (error) {
    errorText(byId("fingerprint-error"), error.message);
  } finally {
    byId("fingerprint-submit").disabled = !byId("fingerprint-confirmed").checked;
  }
}

async function retryKeys() {
  byId("retry-keys").disabled = true;
  errorText(byId("verification-error"), "");
  try {
    await api("api/keys/retry", {});
    await refreshState();
    if (view.selected) await loadHistory(view.selected);
  } catch (error) {
    errorText(byId("verification-error"), error.message);
  } finally {
    byId("retry-keys").disabled = !view.state.connected;
  }
}

async function importKeys(event) {
  event.preventDefault();
  const generation = view.accountGeneration;
  const file = byId("key-file").files[0];
  const passphrase = byId("key-passphrase").value;
  const resultText = byId("key-import-result");
  resultText.textContent = "";
  if (!file) {
    resultText.textContent = "Bitte zuerst die exportierte Schlüsseldatei auswählen.";
    return;
  }
  if (!passphrase) {
    resultText.textContent = "Bitte die beim Export gewählte Passphrase eingeben.";
    return;
  }
  if (file.size > 10 * 1024 * 1024) {
    resultText.textContent = "Die Schlüsseldatei darf höchstens 10 MB groß sein.";
    return;
  }
  const data = new FormData();
  data.append("file", file);
  data.append("passphrase", passphrase);
  const button = byId("import-keys");
  button.disabled = true;
  button.textContent = "Schlüssel werden importiert …";
  byId("key-import-form").setAttribute("aria-busy", "true");
  resultText.textContent = "Die Datei wird übertragen und entschlüsselt. Bei größeren Exporten kann das einige Minuten dauern. Bitte die Oberfläche geöffnet lassen.";
  let finished = false;
  const started = Date.now();
  const progress = window.setInterval(() => {
    if (finished || generation !== view.accountGeneration) return;
    const seconds = Math.floor((Date.now() - started) / 1000);
    resultText.textContent = `Import läuft seit ${seconds} Sekunden. Die Datei wird geprüft und die Schlüssel werden gespeichert. Der Matrix-Chat bleibt verfügbar.`;
  }, 3000);
  try {
    const result = await api("api/keys/import", data, {timeout: 315000});
    finished = true;
    if (generation !== view.accountGeneration) return;
    const remaining = result.pending_encrypted_count ?? result.remaining ?? view.state.diagnostics?.pending_encrypted_count;
    const imported = result.imported_keys;
    const decrypted = result.decrypted_messages;
    resultText.textContent = "Import abgeschlossen." + (imported === undefined ? "" : ` ${imported} neue Schlüssel gespeichert.`) + (decrypted === undefined ? "" : ` ${decrypted} Nachrichten entschlüsselt.`) + (remaining === undefined ? "" : remaining ? ` Für ${remaining} Nachrichten fehlen noch weitere Schlüssel.` : " Für die gespeicherten Nachrichten fehlen keine Schlüssel mehr.");
    byId("key-file").value = "";
    try {
      await refreshState();
      if (view.selected) await loadHistory(view.selected);
    } catch {
      if (generation === view.accountGeneration) resultText.textContent += " Die Ansicht konnte noch nicht aktualisiert werden. Bitte den Chat erneut öffnen.";
    }
  } catch (error) {
    if (error.staleAccount || generation !== view.accountGeneration) return;
    resultText.textContent = error.message;
  } finally {
    finished = true;
    window.clearInterval(progress);
    if (generation === view.accountGeneration) {
      byId("key-passphrase").value = "";
      byId("key-import-form").removeAttribute("aria-busy");
      button.disabled = false;
      button.textContent = "Schlüssel importieren";
    }
  }
}

async function logoutAccount() {
  byId("confirm-logout").disabled = true;
  errorText(byId("settings-error"), "");
  try {
    const result = await api("api/account/logout", {confirmed: true});
    clearAccountView();
    view.state = {connected: false, user_id: "", rooms: [], devices: [], verification: [], setup_required: true};
    const settings = await loadSettings(false);
    if (settings) fillSettings(settings);
    setSettingsTab("matrix");
    errorText(byId("settings-result"), result.logout_notice || "Abgemeldet. Du kannst jetzt ein Matrix-Konto einrichten.");
    if (!byId("settings-dialog").open) byId("settings-dialog").showModal();
    await refreshState();
  } catch (error) {
    errorText(byId("settings-error"), error.message);
  } finally {
    byId("confirm-logout").disabled = false;
  }
}

byId("search").addEventListener("input", event => { view.query = event.target.value.toLowerCase(); renderRooms(); });
for (const button of document.querySelectorAll("[data-filter]")) button.addEventListener("click", () => {
  view.filter = button.dataset.filter;
  for (const tab of document.querySelectorAll("[data-filter]")) {
    tab.classList.toggle("active", tab === button);
    tab.setAttribute("aria-pressed", String(tab === button));
  }
  renderRooms();
});
byId("new-chat").addEventListener("click", openDirect);
byId("welcome-new-chat").addEventListener("click", openDirect);
byId("close-direct").addEventListener("click", () => byId("direct-dialog").close());
byId("direct-form").addEventListener("submit", createDirect);
byId("composer").addEventListener("submit", sendMessage);
byId("message-input").addEventListener("input", () => { if (view.selected) view.drafts.set(view.selected, byId("message-input").value); renderHeader(); });
byId("message-input").addEventListener("keydown", event => { if (event.key === "Enter" && !event.shiftKey && !event.isComposing) { event.preventDefault(); byId("composer").requestSubmit(); } });
byId("load-history").addEventListener("click", () => loadHistory(view.selected, true));
byId("join-room").addEventListener("click", joinRoom);
byId("back-to-chats").addEventListener("click", () => byId("messenger").classList.remove("chat-open"));
byId("account-security").addEventListener("click", openVerification);
byId("verification-banner").addEventListener("click", openVerification);
byId("close-verification").addEventListener("click", () => byId("verification-dialog").close());
byId("verification-dialog").addEventListener("close", () => { view.activeVerification = null; renderVerification(); });
byId("verification-confirm").addEventListener("click", () => verificationAction(byId("verification-confirm").dataset.action));
byId("verification-cancel").addEventListener("click", () => verificationAction(byId("verification-cancel").dataset.action));
byId("open-settings").addEventListener("click", openSettings);
byId("welcome-setup").addEventListener("click", openSettings);
byId("close-settings").addEventListener("click", () => byId("settings-dialog").close());
byId("settings-dialog").addEventListener("close", clearSecretInputs);
byId("settings-form").addEventListener("submit", saveSettings);
for (const button of document.querySelectorAll("[data-settings-tab]")) button.addEventListener("click", () => setSettingsTab(button.dataset.settingsTab));
byId("settings-enabled").addEventListener("change", renderRadioFields);
byId("settings-connection").addEventListener("change", renderRadioFields);
byId("settings-bridge-room").addEventListener("change", event => { byId("settings-room-id").value = event.target.value; });
byId("settings-room-id").addEventListener("input", event => populateBridgeRooms(event.target.value));
byId("generate-psk").addEventListener("click", () => {
  const key = crypto.getRandomValues(new Uint8Array(32));
  byId("settings-channel-psk").value = btoa(String.fromCharCode(...key));
  byId("settings-channel-psk-clear").checked = false;
  byId("show-channel-psk").checked = true;
  byId("settings-channel-psk").type = "text";
  byId("settings-channel-psk-status").textContent = "Neuer privater Schlüssel erzeugt. Auch auf dem Funkgerät eintragen und anschließend speichern.";
});
byId("show-channel-psk").addEventListener("change", event => { byId("settings-channel-psk").type = event.target.checked ? "text" : "password"; });
byId("copy-psk").addEventListener("click", async () => {
  const value = byId("settings-channel-psk").value;
  try {
    if (!value) throw new Error("Zum Kopieren einen neuen Schlüssel eingeben oder erzeugen. Gespeicherte Schlüssel werden nicht zurückgelesen.");
    await navigator.clipboard.writeText(value);
    byId("settings-channel-psk-status").textContent = "Schlüssel kopiert. Auf dem Funkgerät denselben Schlüssel verwenden.";
  } catch (error) {
    byId("settings-channel-psk-status").textContent = error.message || "Kopieren nicht möglich. Den angezeigten Schlüssel manuell kopieren.";
  }
});
byId("discover-serial").addEventListener("click", discoverSerial);
byId("settings-security").addEventListener("click", () => { byId("settings-dialog").close(); openVerification(); });
byId("logout-account").addEventListener("click", () => {
  byId("logout-confirm-text").textContent = `Von „${view.settings?.matrix?.user_id || view.state.user_id || "diesem Matrix-Konto"}“ wirklich abmelden?`;
  show(byId("logout-confirm"), true);
});
byId("cancel-logout").addEventListener("click", () => show(byId("logout-confirm"), false));
byId("confirm-logout").addEventListener("click", logoutAccount);
byId("open-rooms").addEventListener("click", showRoomsDialog);
byId("close-rooms").addEventListener("click", () => byId("rooms-dialog").close());
byId("create-room-form").addEventListener("submit", createRoom);
byId("join-existing-form").addEventListener("submit", joinExisting);
byId("room-settings").addEventListener("click", openRoomSettings);
byId("close-room-dialog").addEventListener("click", () => byId("room-dialog").close());
byId("rename-room-form").addEventListener("submit", event => { event.preventDefault(); roomAction("rename", {name: byId("managed-room-name").value.trim()}, "Raumname gespeichert.", "rename-room"); });
byId("invite-room-form").addEventListener("submit", event => { event.preventDefault(); roomAction("invite", {user_id: byId("invite-room-user").value.trim()}, "Einladung gesendet.", "invite-room"); });
byId("leave-room").addEventListener("click", () => {
  const room = roomById(view.managedRoom);
  byId("leave-confirm-text").textContent = `Den Raum „${room ? roomName(room) : view.managedRoom}“ wirklich verlassen? Für einen erneuten Beitritt kann eine neue Einladung erforderlich sein.`;
  show(byId("leave-confirm"), true);
});
byId("cancel-leave").addEventListener("click", () => show(byId("leave-confirm"), false));
byId("confirm-leave").addEventListener("click", () => roomAction("leave", {confirmed: true}, "Raum verlassen.", "confirm-leave"));
byId("close-fingerprint").addEventListener("click", () => byId("fingerprint-dialog").close());
byId("fingerprint-dialog").addEventListener("close", () => { view.fingerprintDevice = null; byId("fingerprint-confirmed").checked = false; });
byId("fingerprint-confirmed").addEventListener("change", event => { byId("fingerprint-submit").disabled = !event.target.checked; });
byId("fingerprint-form").addEventListener("submit", verifyFingerprint);
byId("retry-keys").addEventListener("click", retryKeys);
byId("key-import-form").addEventListener("submit", importKeys);
byId("verification-dialog").addEventListener("close", () => { byId("key-passphrase").value = ""; byId("key-file").value = ""; });
byId("composer-security").addEventListener("click", openVerification);
byId("chat-security").addEventListener("click", openVerification);
document.addEventListener("visibilitychange", () => { if (!document.hidden && view.selected) markRead(view.selected); });
loadSettings(true).finally(poll);
