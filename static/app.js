const { createApp } = Vue;

createApp({
  data() {
    return {
      page: "dashboard",
      loading: false,
      health: {},
      stats: {},
      wallets: [],
      listenerTasks: [],
      copyTasks: [],
      strategyTasks: [],
      transactions: [],
      logs: [],
      rpcConfigs: [],
      toast: "",
      ws: null,

      // ── Wallet name edit ────────────────────────────────────────────
      editingWalletId: null,
      editingName: "",

      // ── Lazy balance cache ───────────────────────────────────────────
      // walletBalances[id] = undefined | 'loading' | '0.1234'
      walletBalances: {},

      // ── Wallet modals ───────────────────────────────────────────────
      modal: null,           // 'pk' | 'holdings' | 'transfer' | null
      modalWallet: null,

      // Private key modal
      pkStep: 1,
      pkValue: "",
      pkVisible: false,

      // Holdings modal
      holdings: [],
      holdingsLoading: false,
      holdingsRequested: false,  // true after user clicks Check Holdings

      // Transfer modal
      xferForm: { to_address: "", token: "", amount: "" },
      xferAddrValid: null,
      xferLoading: false,

      listenerForm: {
        target_address: "",
        platforms: ["fourmeme", "flap"],
        label: "",
      },
      listenerAddrValid: null,

      // ── Listener label inline edit ───────────────────────────────────
      editingListenerId: null,
      editingListenerLabel: "",

      // ── Listener task detail ─────────────────────────────────────────
      selectedListenerTask: null,
      taskEvents: [],
      taskEventsLoading: false,
      _taskEventPollTimer: null,
      _taskEventMaxId: 0,

      // ── Unread event counts per listener task (task.id → count) ──────
      unreadCounts: {},
      // ── Last start time per listener task (task.id → ISO string) ─────
      listenerStartTimes: {},

      // ── Token name/symbol cache (lower-case address → {name, symbol} | 'loading') ──
      tokenNames: {},

      // ── BNB USD price (refreshed every 60s) ──────────────────────────
      bnbPrice: null,

      copyForm: {
        target_address: "",
        wallet_id: null,
        buy_mode: "fixed",
        buy_value: 0.2,
        sell_mode: "mirror",
        slippage: 3,
        gas_multiplier: 1.1,
      },

      strategyForm: {
        wallet_id: null,
        token: "",
        take_profit: 20,
        stop_loss: 10,
      },

      rpcForm: {
        label: "",
        chain: "bsc",
        rpc_url: "",
        ws_url: "",
        chain_id: 56,
      },

      logFilter: "",  // category filter on dashboard log panel
      warnPopups: [],  // [{type:'warn'|'error', title:'...', msg:'...'}]
    };
  },

  computed: {
    filteredLogs() {
      if (!this.logFilter) return this.logs;
      return this.logs.filter(l => l.category === this.logFilter);
    },
    listenerTaskMap() {
      const m = {};
      for (const t of this.listenerTasks) m[t.id] = t;
      return m;
    },
    xferAddrClass() {
      if (this.xferAddrValid === true)  return "addr-ok";
      if (this.xferAddrValid === false) return "addr-bad";
      return "";
    },
    listenerAddrClass() {
      if (this.listenerAddrValid === true)  return "addr-ok";
      if (this.listenerAddrValid === false) return "addr-bad";
      return "";
    },
  },

  methods: {
    // ── HTTP ───────────────────────────────────────────────────────────
    async api(url, options = {}) {
      const res = await fetch(url, {
        headers: { "Content-Type": "application/json" },
        ...options,
      });
      if (!res.ok) {
        let msg = `HTTP ${res.status}`;
        try { const b = await res.json(); msg = b.detail || JSON.stringify(b); } catch (_) {}
        throw new Error(msg);
      }
      return res.json();
    },

    // ── Helpers ────────────────────────────────────────────────────────
    shortAddr(addr) {
      if (!addr || addr.length < 14) return addr || "-";
      return `${addr.slice(0, 8)}…${addr.slice(-6)}`;
    },
    walletLabel(addr) {
      if (!addr) return "-";
      const w = this.wallets.find(w => w.address.toLowerCase() === addr.toLowerCase());
      return w ? `${w.label || 'Wallet'}#${w.id}` : this.shortAddr(addr);
    },
    _parseDate(ts) {
      if (!ts) return null;
      const d = new Date(ts.endsWith?.("Z") ? ts : ts + "Z");
      return isNaN(d) ? null : d;
    },
    _fmtFull(d) {
      const pad = n => String(n).padStart(2, "0");
      const off = -d.getTimezoneOffset();
      const tz = "GMT" + (off >= 0 ? "+" : "") + (off / 60);
      return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())} ${tz}`;
    },
    fmtTime(ts) {
      const d = this._parseDate(ts);
      return d ? this._fmtFull(d) : (ts || "--");
    },
    fmtTimeShort(ts) {
      const d = this._parseDate(ts);
      if (!d) return ts || "--";
      const pad = n => String(n).padStart(2, "0");
      return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
    },
    showToast(msg, ms = 2500) {
      this.toast = msg;
      setTimeout(() => { if (this.toast === msg) this.toast = ""; }, ms);
    },

    // ── Refresh ────────────────────────────────────────────────────────
    // refreshAll: topbar button — only loads dashboard data (no slow wallet RPC calls)
    async refreshAll() {
      this.loading = true;
      await Promise.allSettled([
        this.loadHealth(),
        this.loadStats(),
        this.loadTransactions(),
        this.loadLogs(),
      ]);
      this.loading = false;
    },

    // refreshDashboard: dashboard refresh button — same as above
    async refreshDashboard() {
      this.loading = true;
      await Promise.allSettled([
        this.loadHealth(),
        this.loadStats(),
        this.loadTransactions(),
        this.loadLogs(),
      ]);
      this.loading = false;
    },

    async loadHealth() {
      try { this.health = await this.api("/api/health"); } catch (_) {}
    },
    async loadStats() {
      try { this.stats = await this.api("/api/dashboard"); } catch (_) {}
    },
    async loadWallets() {
      try { this.wallets = await this.api("/api/wallets"); } catch (_) {}
    },

    // ── Per-wallet lazy balance ────────────────────────────────────────
    async fetchWalletBalance(wallet) {
      this.walletBalances[wallet.id] = "loading";
      try {
        const r = await this.api(`/api/wallets/${wallet.id}/balance`);
        this.walletBalances[wallet.id] = r.balance;
        wallet.bnb_balance = r.balance;
        if (this.modalWallet?.id === wallet.id) {
          this.modalWallet.bnb_balance = r.balance;
        }
      } catch (_) {
        this.walletBalances[wallet.id] = "error";
      }
    },

    // ── Per-wallet refresh (BNB + token holdings) ─────────────────────
    async refreshWallet(wallet) {
      this.walletBalances[wallet.id] = "loading";
      const [balRes, tokRes] = await Promise.allSettled([
        this.api(`/api/wallets/${wallet.id}/balance`),
        this.api(`/api/wallets/${wallet.id}/tokens`),
      ]);
      if (balRes.status === "fulfilled") {
        this.walletBalances[wallet.id] = balRes.value.balance;
        wallet.bnb_balance = balRes.value.balance;
        if (this.modalWallet?.id === wallet.id) {
          this.modalWallet.bnb_balance = balRes.value.balance;
        }
      } else {
        this.walletBalances[wallet.id] = "error";
      }
      if (tokRes.status === "fulfilled") {
        // If holdings modal is open, refresh it too
        if (this.modal === "holdings" && this.modalWallet?.id === wallet.id) {
          this.holdings = tokRes.value;
        }
      }
      this.showToast(`#${wallet.id} refreshed`);
    },
    async loadListenerTasks() {
      try {
        this.listenerTasks = await this.api("/api/listener-tasks");
        // Record start time for running tasks we haven't seen yet
        for (const t of this.listenerTasks) {
          if (t.status === 'running' && !this.listenerStartTimes[t.id]) {
            this.listenerStartTimes[t.id] = new Date().toISOString();
          } else if (t.status !== 'running') {
            delete this.listenerStartTimes[t.id];
          }
        }
        // Refresh selected task status if detail panel is open
        if (this.selectedListenerTask) {
          const updated = this.listenerTasks.find(t => t.id === this.selectedListenerTask.id);
          if (updated) this.selectedListenerTask = updated;
        }
      } catch (_) {}
    },
    async loadCopyTasks() {
      try { this.copyTasks = await this.api("/api/copy-tasks"); } catch (_) {}
    },
    async loadStrategyTasks() {
      try { this.strategyTasks = await this.api("/api/strategy-tasks"); } catch (_) {}
    },
    async loadTransactions() {
      try {
        this.transactions = await this.api("/api/transactions?limit=50");
        this.prefetchTokenNames(this.transactions);
        this.$nextTick(() => {
          const feed = this.$refs.activityFeed;
          if (feed) feed.scrollTop = 0; // newest on top
        });
      } catch (_) {}
    },
    async loadRpcConfigs() {
      try { this.rpcConfigs = await this.api("/api/rpc-configs"); } catch (_) {}
    },
    async loadLogs() {
      try {
        const data = await this.api("/api/logs?limit=300");
        this.logs = data.reverse();
      } catch (_) {}
    },
    clearLogs() { this.logs = []; },

    // ── Wallets ────────────────────────────────────────────────────────
    async generateWallet() {
      try {
        const r = await this.api("/api/wallets/generate", { method: "POST", body: JSON.stringify({ count: 1 }) });
        this.showToast(`Wallet created: ${r.addresses[0].slice(0,10)}...`);
        await Promise.all([this.loadWallets(), this.loadStats()]);
      } catch (e) { this.showToast(`Create failed: ${e.message}`); }
    },

    // ── Wallet name inline edit ────────────────────────────────────────
    startEditName(wallet) {
      this.editingWalletId = wallet.id;
      this.editingName = wallet.label || "";
      // Focus the input on next tick after v-if renders it
      this.$nextTick(() => {
        const el = this.$refs.nameInput;
        if (el) { const inp = Array.isArray(el) ? el[0] : el; inp.focus(); inp.select(); }
      });
    },
    async saveWalletName(wallet) {
      if (this.editingWalletId !== wallet.id) return;
      const name = this.editingName.trim();
      this.editingWalletId = null;
      if (!name || name === (wallet.label || "")) return; // no change
      try {
        await this.api(`/api/wallets/${wallet.id}/name`, {
          method: "PATCH",
          body: JSON.stringify({ name }),
        });
        wallet.label = name; // update local ref instantly (no full reload)
        this.showToast(`Renamed to "${name}"`);
      } catch (e) {
        this.showToast(`Rename failed: ${e.message}`);
      }
    },

    // ── Modal helpers ──────────────────────────────────────────────────
    closeModal() {
      this.modal = null;
      this.modalWallet = null;
      this.pkStep = 1;
      this.pkValue = "";
      this.pkVisible = false;
      this.holdings = [];
      this.xferForm = { to_address: "", token: "", amount: "" };
      this.xferAddrValid = null;
      this.xferLoading = false;
    },

    // ── Private key modal ──────────────────────────────────────────────
    openPk(wallet) {
      this.modalWallet = wallet;
      this.pkStep = 1;
      this.pkValue = "";
      this.pkVisible = false;
      this.modal = "pk";
    },
    async confirmPk() {
      try {
        const r = await this.api(`/api/wallets/${this.modalWallet.id}/private-key`);
        this.pkValue = r.private_key;
        this.pkStep = 2;
      } catch (e) { this.showToast(`Failed to fetch: ${e.message}`); }
    },
    async copyText(text) {
      try {
        await navigator.clipboard.writeText(text);
        this.showToast("Copied to clipboard");
      } catch (_) {
        // fallback for non-HTTPS
        const el = document.createElement("textarea");
        el.value = text;
        document.body.appendChild(el);
        el.select();
        document.execCommand("copy");
        document.body.removeChild(el);
        this.showToast("Copied");
      }
    },

    // ── Holdings modal ─────────────────────────────────────────────────
    async openHoldings(wallet) {
      this.modalWallet = wallet;
      this.holdings = [];
      this.holdingsLoading = true;
      this.modal = "holdings";
      try {
        this.holdings = await this.api(`/api/wallets/${wallet.id}/tokens`);
      } catch (e) { this.showToast(`Failed to load holdings: ${e.message}`); }
      finally { this.holdingsLoading = false; }
    },
    async doPanicSell(holding) {
      if (!confirm(`Sell all ${holding.symbol}? This action cannot be undone.`)) return;
      const slippage = parseInt(prompt("Max slippage (%)", "5") || "5", 10);
      try {
        const r = await this.api("/api/wallets/panic-sell", {
          method: "POST",
          body: JSON.stringify({
            wallet_address: this.modalWallet.address,
            token: holding.token,
            slippage,
          }),
        });
        if (r.status === "submitted") {
          this.showToast(`Panic sell submitted: ${r.tx_hash.slice(0,12)}…`);
          // Refresh holdings after a moment
          setTimeout(() => this.openHoldings(this.modalWallet), 2000);
        } else {
          this.showToast(`Panic sell failed: ${r.message}`);
        }
      } catch (e) { this.showToast(`Panic sell failed: ${e.message}`); }
    },

    // ── Transfer modal ─────────────────────────────────────────────────
    async openTransfer(wallet) {
      this.modalWallet = wallet;
      this.holdings = [];
      this.xferForm = { to_address: "", token: "", amount: "" };
      this.xferAddrValid = null;
      this.modal = "transfer";
      // Auto-fetch BNB balance if not yet loaded (needed for setXferMax / display)
      if (this.walletBalances[wallet.id] === undefined) {
        this.fetchWalletBalance(wallet); // fire-and-forget, non-blocking
      }
      // Load holdings for token selector (silently)
      try { this.holdings = await this.api(`/api/wallets/${wallet.id}/tokens`); } catch (_) {}
    },
    validateXferAddr() {
      const v = this.xferForm.to_address.trim();
      if (!v) { this.xferAddrValid = null; return; }
      // EVM address: 0x + 40 hex chars
      this.xferAddrValid = /^0x[0-9a-fA-F]{40}$/.test(v);
    },
    setXferMax() {
      if (!this.xferForm.token) {
        // Fill full BNB balance; backend deducts gas before sending max
        const bal = Number(
          this.walletBalances[this.modalWallet.id] || this.modalWallet.bnb_balance || 0
        );
        this.xferForm.amount = bal.toFixed(8);
      } else {
        const h = this.holdings.find(h => h.token === this.xferForm.token);
        if (h) this.xferForm.amount = h.balance;
      }
    },
    async doTransfer() {
      if (this.xferAddrValid !== true || !this.xferForm.amount) return;
      this.xferLoading = true;
      const toAddress = this.xferForm.to_address.trim();
      try {
        const r = await this.api(`/api/wallets/${this.modalWallet.id}/transfer`, {
          method: "POST",
          body: JSON.stringify({
            to_address: toAddress,
            token: this.xferForm.token,
            amount: Number(this.xferForm.amount),
          }),
        });
        if (r.status === "submitted") {
          this.showToast(`Transfer submitted: ${r.tx_hash.slice(0,12)}…`);
          this.closeModal();
          await this.loadWallets();
        } else {
          this.showToast(`Transfer failed: ${r.message}`);
        }
      } catch (e) { this.showToast(`Transfer failed: ${e.message}`); }
      finally { this.xferLoading = false; }
    },

    // ── Listener task detail ───────────────────────────────────────────
    async openListenerDetail(task) {
      if (this.selectedListenerTask?.id === task.id) {
        this.closeListenerDetail(); return;
      }
      this.closeListenerDetail();
      this.selectedListenerTask = task;
      this.taskEvents = [];
      this._taskEventMaxId = 0;
      this.unreadCounts[task.id] = 0;  // clear badge
      await this.loadTaskEvents(task.id);
      this._startTaskEventPoll(task);
    },
    closeListenerDetail() {
      clearInterval(this._taskEventPollTimer);
      this._taskEventPollTimer = null;
      this.selectedListenerTask = null;
      this.taskEvents = [];
      this._taskEventMaxId = 0;
    },
    async loadTaskEvents(taskId) {
      this.taskEventsLoading = true;
      try {
        const rows = await this.api(`/api/listener-tasks/${taskId}/events?limit=200`);
        this.taskEvents = rows; // already DESC
        this._taskEventMaxId = rows.length ? rows[0].id : 0;
        this.prefetchTokenNames(rows);
        this._scrollEventBox();
      } catch (_) {}
      finally { this.taskEventsLoading = false; }
    },
    async _pollTaskEvents(taskId) {
      if (!this.selectedListenerTask || this.selectedListenerTask.id !== taskId) return;
      try {
        const url = `/api/listener-tasks/${taskId}/events?limit=50&after_id=${this._taskEventMaxId}`;
        const newRows = await this.api(url);
        if (newRows.length) {
          // newRows are DESC; prepend to the front of taskEvents
          this.taskEvents = [...newRows, ...this.taskEvents].slice(0, 500);
          this._taskEventMaxId = newRows[0].id;
          this.prefetchTokenNames(newRows);
          this._scrollEventBox();
        }
      } catch (_) {}
    },
    _startTaskEventPoll(task) {
      clearInterval(this._taskEventPollTimer);
      const id = task.id;
      this._taskEventPollTimer = setInterval(() => {
        if (this.selectedListenerTask?.status === 'running') {
          this._pollTaskEvents(id);
        }
      }, 2500);
    },
    _scrollEventBox() {
      this.$nextTick(() => {
        const box = this.$refs.eventBox;
        if (box) box.scrollTop = 0; // newest on top
      });
    },
    parseExtra(extraStr) {
      try { return JSON.parse(extraStr || '{}'); } catch { return {}; }
    },
    txCounterparty(tx) {
      const ex = this.parseExtra(tx.extra);
      return ex.counterparty || "";
    },
    swapSoldToken(tx) {
      const ex = this.parseExtra(tx.extra);
      const sold = ex.sold_tokens;
      return sold && sold.length ? sold[0].token : "";
    },

    // ── Token name/symbol helpers ──────────────────────────────────────
    // Call this in templates — returns symbol immediately from cache,
    // triggers async fetch if missing (Vue reactivity updates the view).
    tokenSymbol(addr) {
      if (!addr || addr === 'UNKNOWN') return '?';
      const k = addr.toLowerCase();
      const v = this.tokenNames[k];
      if (!v) { this.fetchTokenName(addr); return this.shortAddr(addr); }
      if (v === 'loading') return this.shortAddr(addr);
      return v.symbol || this.shortAddr(addr);
    },
    tokenName(addr) {
      if (!addr || addr === 'UNKNOWN') return '?';
      const k = addr.toLowerCase();
      const v = this.tokenNames[k];
      if (!v || v === 'loading') return '';
      return v.name || '';
    },
    async fetchTokenName(addr) {
      if (!addr || addr === 'UNKNOWN') return;
      const k = addr.toLowerCase();
      if (this.tokenNames[k] !== undefined) return;
      this.tokenNames[k] = 'loading';
      try {
        const r = await this.api(`/api/token-name?address=${addr}`);
        this.tokenNames[k] = r;
      } catch {
        this.tokenNames[k] = { name: '', symbol: '' };
      }
    },
    // Pre-fetch all token addresses from a batch of events
    prefetchTokenNames(events) {
      const seen = new Set();
      for (const ev of events) {
        if (ev.token && ev.token !== 'UNKNOWN' && !seen.has(ev.token.toLowerCase())) {
          seen.add(ev.token.toLowerCase());
          this.fetchTokenName(ev.token);
        }
      }
    },

    platformStyle(p) {
      if (p === 'fourmeme') return 'border-color:rgba(31,216,200,.3);color:var(--c1)';
      if (p === 'flap')     return 'border-color:rgba(79,158,255,.3);color:var(--c4)';
      if (p === 'dex')      return 'border-color:rgba(247,181,85,.3);color:var(--c2)';
      return 'border-color:var(--line);color:var(--muted)';
    },
    fmtAction(action) {
      if (action === 'transfer_in')  return 'XFER IN';
      if (action === 'transfer_out') return 'XFER OUT';
      return action.toUpperCase();
    },
    fmtEvAmount(ev) {
      const amt = Number(ev.amount);
      if (!amt) return '';
      const quote = this.quoteLabel(ev);
      if (quote === 'BNB' && this.bnbPrice) {
        const usd = this.fmtNum((amt * this.bnbPrice).toFixed(1));
        return `${amt.toFixed(4)} BNB ($${usd})`;
      }
      if (quote === 'BNB') return `${amt.toFixed(4)} BNB`;
      return `$${this.fmtNum(amt.toFixed(2))} ${quote}`;
    },
    fmtNum(n) {
      // Add commas to number string: 1234567 → 1,234,567
      const s = String(n);
      const parts = s.split('.');
      parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ',');
      return parts.join('.');
    },
    fmtTokenAmt(amtStr) {
      if (!amtStr || amtStr === '0') return '—';
      try {
        const n = BigInt(amtStr);
        const decimals = 18;
        const divisor = BigInt(10 ** decimals);
        const whole = n / divisor;
        const frac = n % divisor;
        const fracStr = frac.toString().padStart(decimals, '0').slice(0, 4);
        return `${this.fmtNum(whole)}.${fracStr}`;
      } catch {
        return amtStr;
      }
    },

    // ── BNB price ──────────────────────────────────────────────────────
    async fetchBnbPrice() {
      try {
        const r = await fetch("https://api.binance.com/api/v3/ticker/price?symbol=BNBUSDT");
        const d = await r.json();
        this.bnbPrice = parseFloat(d.price) || null;
      } catch (_) {}
    },

    // ── Activity helpers ───────────────────────────────────────────────
    activityCategory(tx) {
      const t = tx.source_task_type;
      const a = tx.action;
      if (t === 'listener')  return 'detected';
      if (t === 'copytrade') return a === 'buy' ? 'follow buy' : 'follow sell';
      if (t === 'strategy') {
        const ex = this.parseExtra(tx.extra);
        if (ex.reason === 'take_profit') return 'take profit';
        if (ex.reason === 'stop_loss')   return 'stop lose';
        return a === 'sell' ? 'take profit' : 'detected';
      }
      return tx.status || 'detected';
    },
    categoryStyle(cat) {
      switch (cat) {
        case 'detected':   return 'color:#7a9ab0;border-color:rgba(100,140,170,.3)';
        case 'follow buy': return 'color:var(--c1);border-color:rgba(31,216,200,.4)';
        case 'follow sell':return 'color:var(--c3);border-color:rgba(255,124,112,.4)';
        case 'take profit':return 'color:var(--c1);border-color:rgba(31,216,200,.4)';
        case 'stop lose':  return 'color:var(--c3);border-color:rgba(255,124,112,.4)';
        default:           return 'color:var(--muted)';
      }
    },
    // Return uppercase quote token label for a transaction
    quoteLabel(tx) {
      const extra = this.parseExtra(tx.extra);
      const q = extra.quote || '';
      if (q === 'bnb')  return 'BNB';
      if (q === 'usdt') return 'USDT';
      if (q === 'usdc') return 'USDC';
      // PancakeSwap stores quote in extra.quote
      return q.toUpperCase() || 'BNB';
    },
    fmtActivityAmount(tx) {
      const amt = Number(tx.amount);
      if (!amt) return '';
      const quote = this.quoteLabel(tx);
      if (quote === 'BNB') {
        if (this.bnbPrice) {
          const usd = this.fmtNum((amt * this.bnbPrice).toFixed(0));
          return `$${usd} (${amt.toFixed(4)} BNB)`;
        }
        return `${amt.toFixed(4)} BNB`;
      }
      return `$${this.fmtNum(amt.toFixed(2))} (${this.fmtNum(amt.toFixed(2))} ${quote})`;
    },

    // ── Listener ───────────────────────────────────────────────────────
    validateListenerAddr() {
      const v = this.listenerForm.target_address.trim();
      if (!v) { this.listenerAddrValid = null; return; }
      this.listenerAddrValid = /^0x[0-9a-fA-F]{40}$/.test(v);
    },
    parsePlatforms(platforms) {
      if (!platforms) return [];
      try { return JSON.parse(platforms); } catch { return [platforms]; }
    },
    // ── Listener label inline edit ─────────────────────────────────────
    startEditListenerLabel(task) {
      this.editingListenerId = task.id;
      this.editingListenerLabel = task.label || "";
      this.$nextTick(() => {
        const el = this.$refs.listenerLabelInput;
        if (el) { const inp = Array.isArray(el) ? el[0] : el; inp.focus(); inp.select(); }
      });
    },
    async saveListenerLabel(task) {
      if (this.editingListenerId !== task.id) return;
      const label = this.editingListenerLabel.trim();
      this.editingListenerId = null;
      if (label === (task.label || "")) return;
      try {
        await this.api(`/api/listener-tasks/${task.id}/label`, {
          method: "PATCH",
          body: JSON.stringify({ label: label || "unnamed" }),
        });
        task.label = label || "unnamed";
        if (this.selectedListenerTask?.id === task.id) {
          this.selectedListenerTask.label = task.label;
        }
        this.showToast(`Renamed to "${task.label}"`);
      } catch (e) { this.showToast(`Rename failed: ${e.message}`); }
    },

    pushWarn(type, title, msg, ms = 8000) {
      this.warnPopups.push({ type, title, msg });
      if (this.warnPopups.length > 4) this.warnPopups.shift();
      setTimeout(() => {
        const idx = this.warnPopups.findIndex(w => w.title === title && w.msg === msg);
        if (idx >= 0) this.warnPopups.splice(idx, 1);
      }, ms);
    },

    async deleteListenerTask(task) {
      if (task.status === 'running') {
        this.showToast('Stop the listener before deleting'); return;
      }
      if (!confirm(`Delete listener #${task.id}${task.label ? ' (' + task.label + ')' : ''}? This will also delete all its detected events.`)) return;
      try {
        await this.api(`/api/listener-tasks/${task.id}`, { method: 'DELETE' });
        this.showToast(`Listener #${task.id} deleted`);
        if (this.selectedListenerTask?.id === task.id) this.closeListenerDetail();
        await Promise.all([this.loadListenerTasks(), this.loadStats()]);
      } catch (e) { this.showToast(`Delete failed: ${e.message}`); }
    },

    async createListenerTask() {
      if (this.listenerAddrValid !== true) return;
      if (this.listenerForm.platforms.length === 0) {
        this.showToast("Please select at least one platform"); return;
      }
      try {
        await this.api("/api/listener-tasks", {
          method: "POST",
          body: JSON.stringify({
            target_address: this.listenerForm.target_address.trim(),
            platforms: this.listenerForm.platforms,
            label: this.listenerForm.label.trim(),
            chain: "bsc",
            config: {},
          }),
        });
        this.listenerForm.target_address = "";
        this.listenerForm.label = "";
        this.listenerAddrValid = null;
        this.showToast("Listener task created");
        await Promise.all([this.loadListenerTasks(), this.loadStats()]);
      } catch (e) { this.showToast(`Create failed: ${e.message}`); }
    },

    // ── Copy trade ─────────────────────────────────────────────────────
    async createCopyTask() {
      try {
        await this.api("/api/copy-tasks", {
          method: "POST",
          body: JSON.stringify({
            target_address: this.copyForm.target_address,
            wallet_id: Number(this.copyForm.wallet_id),
            buy_mode: this.copyForm.buy_mode,
            buy_value: Number(this.copyForm.buy_value),
            sell_mode: this.copyForm.sell_mode,
            slippage: Number(this.copyForm.slippage),
            gas_multiplier: Number(this.copyForm.gas_multiplier),
            config: {},
          }),
        });
        this.showToast("Copy trade task created");
        await Promise.all([this.loadCopyTasks(), this.loadStats()]);
      } catch (e) { this.showToast(`Create failed: ${e.message}`); }
    },

    // ── Start all non-running tasks ─────────────────────────────────────
    async startAllTasks(kind) {
      const tasks = kind === 'listener' ? this.listenerTasks : this.copyTasks;
      const routeBase = kind === 'listener' ? '/api/listener-tasks' : '/api/copy-tasks';
      const toStart = tasks.filter(t => t.status !== 'running');
      if (!toStart.length) return;
      const results = await Promise.allSettled(
        toStart.map(t =>
          this.api(`${routeBase}/${t.id}/status`, {
            method: "PATCH", body: JSON.stringify({ status: "running" }),
          })
        )
      );
      const ok = results.filter(r => r.status === 'fulfilled').length;
      if (kind === 'listener') {
        const now = new Date().toISOString();
        toStart.forEach(t => { this.listenerStartTimes[t.id] = now; });
      }
      this.showToast(`Started ${ok}/${toStart.length} ${kind} task(s)`);
      await Promise.all([
        this.loadListenerTasks(), this.loadCopyTasks(), this.loadStats(),
      ]);
    },

    // ── Task status ────────────────────────────────────────────────────
    async setTaskStatus(kind, id, status) {
      const routes = {
        listener: `/api/listener-tasks/${id}/status`,
        copy:     `/api/copy-tasks/${id}/status`,
      };
      try {
        await this.api(routes[kind], { method: "PATCH", body: JSON.stringify({ status }) });
        if (kind === 'listener') {
          if (status === 'running') {
            this.listenerStartTimes[id] = new Date().toISOString();
          } else {
            delete this.listenerStartTimes[id];
          }
        }
        this.showToast(`Task #${id} → ${status}`);
        await Promise.all([
          this.loadListenerTasks(),
          this.loadCopyTasks(),
          this.loadStats(),
        ]);
      } catch (e) { this.showToast(`Status update failed: ${e.message}`); }
    },

    // ── RPC ────────────────────────────────────────────────────────────
    async saveRpc() {
      if (!this.rpcForm.rpc_url.trim()) return;
      try {
        await this.api("/api/rpc-configs", {
          method: "POST",
          body: JSON.stringify(this.rpcForm),
        });
        this.rpcForm = { label: "", chain: "bsc", rpc_url: "", ws_url: "", chain_id: 56 };
        this.showToast("RPC endpoint added");
        await this.loadRpcConfigs();
      } catch (e) { this.showToast(`Save failed: ${e.message}`); }
    },
    async activateRpc(id) {
      try {
        await this.api(`/api/rpc-configs/${id}/activate`, { method: "PATCH" });
        this.showToast("RPC switched");
        await this.loadRpcConfigs();
      } catch (e) { this.showToast(`Switch failed: ${e.message}`); }
    },
    async deleteRpc(id) {
      try {
        await this.api(`/api/rpc-configs/${id}`, { method: "DELETE" });
        this.showToast("RPC deleted");
        await this.loadRpcConfigs();
      } catch (e) { this.showToast(`Delete failed: ${e.message}`); }
    },

    // ── WebSocket log feed ─────────────────────────────────────────────
    connectWs() {
      const proto = location.protocol === "https:" ? "wss" : "ws";
      this.ws = new WebSocket(`${proto}://${location.host}/ws/logs`);
      this.ws.onmessage = (ev) => {
        const d = JSON.parse(ev.data);
        // Attach listener label for real-time log display
        let listenerLabel = null;
        if (d.category === 'listener' && d.task_id) {
          const task = this.listenerTaskMap[d.task_id];
          listenerLabel = task?.label || null;
        }
        this.logs.push({
          level:          d.level    || "INFO",
          category:       d.category || "system",
          message:        d.message  || "",
          tx_hash:        d.tx_hash  || null,
          task_id:        d.task_id  || null,
          listener_label: listenerLabel,
          timestamp:      new Date().toISOString(),
        });
        if (this.logs.length > 500) this.logs = this.logs.slice(-500);
        // Auto-scroll whichever logBox is currently visible
        this.$nextTick(() => {
          const box = this.$refs.logBox;
          if (box) box.scrollTop = box.scrollHeight;
        });
        // Keep stats fresh on every backend event
        this.loadStats();
        // Warn popup when listener pauses or gets interrupted
        if (d.category === 'listener' && d.message) {
          const m = d.message;
          if (/paused/i.test(m)) {
            const id = m.match(/#(\d+)/)?.[1] || '?';
            const task = this.listenerTaskMap[id];
            const label = task?.label ? ` (${task.label})` : '';
            this.pushWarn('warn', `Listener #${id}${label} paused`, 'The listener has stopped watching. Restart it to continue monitoring.');
            this.loadListenerTasks();
          } else if (/error|interrupted|failed/i.test(m) && d.level === 'ERROR') {
            const id = m.match(/#(\d+)/)?.[1] || '?';
            const task = this.listenerTaskMap[id];
            const label = task?.label ? ` (${task.label})` : '';
            this.pushWarn('error', `Listener #${id}${label} error`, m.slice(0, 120));
            this.loadListenerTasks();
          }
        }
        // If a listener trade event comes in, update unread count or refresh detail panel
        if (d.category === 'listener' && d.task_id) {
          if (this.selectedListenerTask?.id === d.task_id) {
            this._pollTaskEvents(d.task_id);
          } else {
            this.unreadCounts[d.task_id] = (this.unreadCounts[d.task_id] || 0) + 1;
          }
        }
      };
      this.ws.onclose = () => {
        this.health = {};  // show node offline
        setTimeout(() => this.connectWs(), 3000);
      };
    },
  },

  async mounted() {
    await this.refreshAll();   // loads health + stats + transactions + logs
    this.connectWs();
    this.fetchBnbPrice();
    setInterval(() => this.loadStats(),        15_000);
    setInterval(() => this.loadTransactions(), 10_000);
    setInterval(() => this.fetchBnbPrice(),    60_000);
  },
}).mount("#app");
