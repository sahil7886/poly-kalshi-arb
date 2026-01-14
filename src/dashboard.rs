// src/dashboard.rs
// Embedded dashboard server using Axum + WebSocket

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

// =============================================================================
// DASHBOARD STATE
// =============================================================================

/// Statistics for dashboard display
#[derive(Default)]
pub struct DashboardStats {
    /// Total PnL in cents
    pub total_pnl_cents: AtomicI64,
    /// Trade count
    pub trade_count: AtomicU32,
    /// Win count (trades with positive profit)
    pub win_count: AtomicU32,
    /// Arbs detected count
    pub arbs_detected: AtomicU32,
    /// Sum of arb spreads in cents (for averaging)
    pub arb_sum_cents: AtomicI64,
    /// Sum of liquidity samples (min contracts available)
    pub liquidity_sum: AtomicI64,
    /// Count of liquidity samples
    pub liquidity_count: AtomicU32,
    /// Sum of profit per trade in cents
    pub profit_sum_cents: AtomicI64,
}

/// Market state for display
#[derive(Clone, Serialize, Default)]
pub struct MarketDisplay {
    pub asset: String,
    pub prob: f64,
    pub velocity: f64,
    pub time_left: f64,
}

/// Trade event for display
#[derive(Clone, Serialize)]
pub struct TradeEvent {
    pub action: String,
    pub asset: String,
    pub size: f64,
    pub pnl: Option<f64>,
    pub time: String,
}

/// Shared dashboard state
pub struct DashboardState {
    pub stats: DashboardStats,
    pub markets: RwLock<HashMap<String, MarketDisplay>>,
    /// Broadcast channel for WebSocket updates
    tx: broadcast::Sender<String>,
}

impl DashboardState {
    pub fn new() -> Arc<Self> {
        let (tx, _) = broadcast::channel(256);
        Arc::new(Self {
            stats: DashboardStats::default(),
            markets: RwLock::new(HashMap::new()),
            tx,
        })
    }

    /// Record a detected arb opportunity (non-blocking)
    #[inline]
    pub fn record_arb(&self, arb_cents: i16, min_liquidity: u32) {
        self.stats.arbs_detected.fetch_add(1, Ordering::Relaxed);
        self.stats.arb_sum_cents.fetch_add(arb_cents as i64, Ordering::Relaxed);
        self.stats.liquidity_sum.fetch_add(min_liquidity as i64, Ordering::Relaxed);
        self.stats.liquidity_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a completed trade (non-blocking)
    #[inline]
    pub fn record_trade(&self, profit_cents: i16, is_win: bool) {
        self.stats.trade_count.fetch_add(1, Ordering::Relaxed);
        self.stats.total_pnl_cents.fetch_add(profit_cents as i64, Ordering::Relaxed);
        self.stats.profit_sum_cents.fetch_add(profit_cents as i64, Ordering::Relaxed);
        if is_win {
            self.stats.win_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Update market display (async, but fast)
    pub async fn update_market(&self, id: String, market: MarketDisplay) {
        self.markets.write().await.insert(id, market);
    }

    /// Push a trade event to connected clients
    pub fn push_trade(&self, event: TradeEvent) {
        if let Ok(json) = serde_json::to_string(&serde_json::json!({"type": "trade", "data": event})) {
            let _ = self.tx.send(json);
        }
    }

    /// Get current state as JSON for broadcast
    pub async fn to_json(&self) -> String {
        let stats = &self.stats;
        let trade_count = stats.trade_count.load(Ordering::Relaxed);
        let arbs_detected = stats.arbs_detected.load(Ordering::Relaxed);
        let liquidity_count = stats.liquidity_count.load(Ordering::Relaxed);

        let avg_arb = if arbs_detected > 0 {
            stats.arb_sum_cents.load(Ordering::Relaxed) as f64 / arbs_detected as f64
        } else {
            0.0
        };

        let avg_liquidity = if liquidity_count > 0 {
            stats.liquidity_sum.load(Ordering::Relaxed) as f64 / liquidity_count as f64
        } else {
            0.0
        };

        let avg_profit = if trade_count > 0 {
            stats.profit_sum_cents.load(Ordering::Relaxed) as f64 / trade_count as f64
        } else {
            0.0
        };

        let markets = self.markets.read().await;

        serde_json::json!({
            "type": "state_update",
            "data": {
                "total_pnl": stats.total_pnl_cents.load(Ordering::Relaxed) as f64 / 100.0,
                "trade_count": trade_count,
                "win_count": stats.win_count.load(Ordering::Relaxed),
                "arbs_detected": arbs_detected,
                "avg_arb_cents": avg_arb,
                "avg_liquidity": avg_liquidity,
                "avg_profit_cents": avg_profit,
                "markets": markets.clone(),
                "positions": {}
            }
        }).to_string()
    }

    /// Subscribe to updates
    pub fn subscribe(&self) -> broadcast::Receiver<String> {
        self.tx.subscribe()
    }

    /// Broadcast current state (called by background task)
    pub async fn broadcast_state(&self) {
        let json = self.to_json().await;
        let _ = self.tx.send(json);
    }
}

impl Default for DashboardState {
    fn default() -> Self {
        let (tx, _) = broadcast::channel(256);
        Self {
            stats: DashboardStats::default(),
            markets: RwLock::new(HashMap::new()),
            tx,
        }
    }
}

// =============================================================================
// AXUM HANDLERS
// =============================================================================

async fn index_handler() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<DashboardState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: Arc<DashboardState>) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.subscribe();

    // Send initial state
    if let Ok(json) = serde_json::to_string(&serde_json::json!({"type": "connected"})) {
        let _ = sender.send(Message::Text(json)).await;
    }

    // Forward broadcast messages to this client
    let send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    // Keep connection alive (ignore incoming messages)
    while let Some(Ok(_)) = receiver.next().await {}

    send_task.abort();
}

// =============================================================================
// SERVER
// =============================================================================

/// Start the dashboard server (non-blocking, runs in background)
pub fn start_dashboard_server(state: Arc<DashboardState>, port: u16) {
    let broadcast_state = state.clone();

    // Spawn broadcast loop
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
        loop {
            interval.tick().await;
            broadcast_state.broadcast_state().await;
        }
    });

    // Spawn HTTP server
    tokio::spawn(async move {
        let app = Router::new()
            .route("/", get(index_handler))
            .route("/ws", get(ws_handler))
            .layer(CorsLayer::permissive())
            .with_state(state);

        let listener = match tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await {
            Ok(l) => l,
            Err(e) => {
                warn!("[DASHBOARD] Failed to bind port {}: {}", port, e);
                return;
            }
        };

        info!("[DASHBOARD] Server running at http://localhost:{}", port);

        if let Err(e) = axum::serve(listener, app).await {
            warn!("[DASHBOARD] Server error: {}", e);
        }
    });
}

// =============================================================================
// EMBEDDED HTML TEMPLATE
// =============================================================================

const DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Arb Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }

        :root {
            --bg: #050505;
            --surface: #0a0a0a;
            --border: #151515;
            --text: #e0e0e0;
            --dim: #444;
            --green: #00ff88;
            --red: #ff3355;
            --blue: #3388ff;
            --amber: #ffaa00;
        }

        body {
            font-family: 'JetBrains Mono', monospace;
            background: var(--bg);
            color: var(--text);
            height: 100vh;
            overflow: hidden;
        }

        .container {
            display: grid;
            grid-template-columns: 1fr 320px;
            grid-template-rows: 80px 1fr 200px;
            height: 100vh;
            gap: 1px;
            background: var(--border);
        }

        .header {
            grid-column: 1 / -1;
            background: var(--surface);
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 0 32px;
        }

        .logo {
            display: flex;
            align-items: center;
            gap: 16px;
        }

        .logo h1 {
            font-size: 13px;
            font-weight: 500;
            color: var(--dim);
            letter-spacing: 3px;
            text-transform: uppercase;
        }

        .live-indicator {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 6px 12px;
            background: rgba(0, 255, 136, 0.1);
            border: 1px solid rgba(0, 255, 136, 0.2);
        }

        .live-dot {
            width: 6px;
            height: 6px;
            background: var(--green);
            animation: pulse 1.5s infinite;
        }

        .live-text {
            font-size: 10px;
            color: var(--green);
            text-transform: uppercase;
            letter-spacing: 1px;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.3; }
        }

        .header-stats {
            display: flex;
            gap: 48px;
        }

        .header-stat {
            text-align: right;
        }

        .header-stat-value {
            font-size: 32px;
            font-weight: 600;
            font-variant-numeric: tabular-nums;
        }

        .header-stat-value.positive { color: var(--green); }
        .header-stat-value.negative { color: var(--red); }

        .header-stat-label {
            font-size: 10px;
            color: var(--dim);
            text-transform: uppercase;
            letter-spacing: 1px;
        }

        .chart-area {
            background: var(--surface);
            padding: 24px;
            display: flex;
            flex-direction: column;
        }

        .chart-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
        }

        .chart-title {
            font-size: 11px;
            color: var(--dim);
            text-transform: uppercase;
            letter-spacing: 1px;
        }

        .chart-legend {
            display: flex;
            gap: 16px;
            font-size: 10px;
        }

        .legend-item {
            display: flex;
            align-items: center;
            gap: 6px;
            color: var(--dim);
        }

        .legend-dot {
            width: 8px;
            height: 8px;
        }

        .legend-dot.pnl { background: var(--green); }
        .legend-dot.win { background: var(--green); opacity: 0.5; }
        .legend-dot.loss { background: var(--red); opacity: 0.5; }

        .chart-container {
            flex: 1;
            position: relative;
            min-height: 0;
        }

        #pnl-chart {
            width: 100%;
            height: 100%;
        }

        .sidebar {
            background: var(--surface);
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }

        .sidebar-header {
            padding: 16px 20px;
            border-bottom: 1px solid var(--border);
            font-size: 11px;
            color: var(--dim);
            text-transform: uppercase;
            letter-spacing: 1px;
        }

        .trades-list {
            flex: 1;
            overflow-y: auto;
            padding: 8px;
        }

        .trade-item {
            display: flex;
            align-items: center;
            padding: 12px;
            margin-bottom: 4px;
            background: var(--bg);
            gap: 12px;
        }

        .trade-item.win { border-left: 2px solid var(--green); }
        .trade-item.loss { border-left: 2px solid var(--red); }
        .trade-item.pending { border-left: 2px solid var(--dim); }

        .trade-side {
            font-size: 9px;
            font-weight: 600;
            padding: 4px 8px;
            text-transform: uppercase;
        }

        .trade-side.long { background: rgba(0,255,136,0.15); color: var(--green); }
        .trade-side.short { background: rgba(255,51,85,0.15); color: var(--red); }

        .trade-details {
            flex: 1;
        }

        .trade-asset {
            font-size: 12px;
            font-weight: 500;
        }

        .trade-meta {
            font-size: 10px;
            color: var(--dim);
            margin-top: 2px;
        }

        .trade-pnl {
            font-size: 14px;
            font-weight: 600;
            font-variant-numeric: tabular-nums;
        }

        .trade-pnl.positive { color: var(--green); }
        .trade-pnl.negative { color: var(--red); }

        .markets-strip {
            grid-column: 1 / -1;
            background: var(--surface);
            display: flex;
            gap: 1px;
            overflow-x: auto;
        }

        .market-card {
            flex: 1;
            min-width: 200px;
            padding: 16px 20px;
            background: var(--bg);
            position: relative;
        }

        .market-card.has-position {
            background: var(--surface);
        }

        .market-card.has-position::after {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 2px;
            background: var(--blue);
        }

        .market-top {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 12px;
        }

        .market-asset {
            font-size: 14px;
            font-weight: 600;
        }

        .market-timer {
            font-size: 20px;
            font-weight: 600;
            font-variant-numeric: tabular-nums;
            color: var(--text);
        }

        .market-timer.urgent {
            color: var(--red);
            animation: blink 0.5s infinite;
        }

        @keyframes blink { 50% { opacity: 0.5; } }

        .market-mid {
            display: flex;
            align-items: baseline;
            gap: 12px;
            margin-bottom: 8px;
        }

        .market-prob {
            font-size: 36px;
            font-weight: 700;
            font-variant-numeric: tabular-nums;
            line-height: 1;
        }

        .market-delta {
            font-size: 12px;
            color: var(--dim);
        }

        .market-delta.up { color: var(--green); }
        .market-delta.down { color: var(--red); }

        .market-position {
            display: flex;
            justify-content: space-between;
            padding: 8px 10px;
            font-size: 11px;
            margin-top: 8px;
        }

        .market-position.long { background: rgba(0,255,136,0.1); color: var(--green); }
        .market-position.short { background: rgba(255,51,85,0.1); color: var(--red); }

        .pos-label { font-weight: 500; text-transform: uppercase; }
        .pos-pnl { font-weight: 600; }

        .no-position {
            text-align: center;
            padding: 8px;
            color: var(--dim);
            font-size: 11px;
        }

        .time-progress {
            position: absolute;
            bottom: 0;
            left: 0;
            height: 2px;
            background: var(--blue);
            opacity: 0.5;
            transition: width 1s linear;
        }

        .stats-row {
            display: flex;
            gap: 1px;
            min-width: 300px;
            background: var(--border);
        }

        .stat-cell {
            flex: 1;
            padding: 16px;
            background: var(--bg);
            text-align: center;
        }

        .stat-value {
            font-size: 18px;
            font-weight: 600;
            font-variant-numeric: tabular-nums;
            margin-bottom: 4px;
        }

        .stat-value.green { color: var(--green); }
        .stat-value.red { color: var(--red); }
        .stat-value.amber { color: var(--amber); }

        .stat-label {
            font-size: 9px;
            color: var(--dim);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        ::-webkit-scrollbar { width: 4px; height: 4px; }
        ::-webkit-scrollbar-track { background: var(--bg); }
        ::-webkit-scrollbar-thumb { background: var(--border); }
    </style>
</head>
<body>
    <div class="container">
        <header class="header">
            <div class="logo">
                <h1>Poly-Kalshi Arb</h1>
                <div class="live-indicator">
                    <div class="live-dot"></div>
                    <span class="live-text" id="status">Connecting</span>
                </div>
            </div>
            <div class="header-stats">
                <div class="header-stat">
                    <div class="header-stat-value" id="trades">0</div>
                    <div class="header-stat-label">Trades</div>
                </div>
                <div class="header-stat">
                    <div class="header-stat-value" id="winrate">0%</div>
                    <div class="header-stat-label">Win Rate</div>
                </div>
                <div class="header-stat">
                    <div class="header-stat-value positive" id="pnl">+$0.00</div>
                    <div class="header-stat-label">Session PnL</div>
                </div>
            </div>
        </header>

        <div class="chart-area">
            <div class="chart-header">
                <span class="chart-title">Equity Curve</span>
                <div class="chart-legend">
                    <div class="legend-item"><div class="legend-dot pnl"></div> PnL</div>
                    <div class="legend-item"><div class="legend-dot win"></div> Win</div>
                    <div class="legend-item"><div class="legend-dot loss"></div> Loss</div>
                </div>
            </div>
            <div class="chart-container">
                <canvas id="pnl-chart"></canvas>
            </div>
        </div>

        <div class="sidebar">
            <div class="sidebar-header">Recent Trades</div>
            <div class="trades-list" id="trades-list">
                <div style="text-align:center;padding:40px;color:var(--dim);font-size:11px;">
                    Waiting for trades...
                </div>
            </div>
        </div>

        <div class="markets-strip">
            <div id="markets-container" style="display:flex;gap:1px;flex:1;">
                <!-- Markets populated by JS -->
            </div>
            <div class="stats-row">
                <div class="stat-cell">
                    <div class="stat-value" id="arbs-detected">0</div>
                    <div class="stat-label">Arbs Found</div>
                </div>
                <div class="stat-cell">
                    <div class="stat-value amber" id="avg-arb">0.0¢</div>
                    <div class="stat-label">Avg Arb</div>
                </div>
                <div class="stat-cell">
                    <div class="stat-value" id="avg-liquidity">0</div>
                    <div class="stat-label">Avg Liq</div>
                </div>
                <div class="stat-cell">
                    <div class="stat-value green" id="avg-profit">0.0¢</div>
                    <div class="stat-label">Avg Profit</div>
                </div>
            </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script>
        let ws;
        let pnlChart;
        let pnlHistory = [];
        let trades = [];
        const maxPoints = 200;

        function connect() {
            const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${location.host}/ws`);

            ws.onopen = () => {
                document.getElementById('status').textContent = 'Live';
                initChart();
            };

            ws.onclose = () => {
                document.getElementById('status').textContent = 'Reconnecting...';
                setTimeout(connect, 2000);
            };

            ws.onmessage = (e) => {
                try {
                    const msg = JSON.parse(e.data);
                    if (msg.type === 'state_update') handleStateUpdate(msg.data);
                    else if (msg.type === 'trade') handleTrade(msg.data);
                } catch (err) {
                    console.error('Parse error:', err);
                }
            };
        }

        function initChart() {
            if (pnlChart) return;
            const ctx = document.getElementById('pnl-chart').getContext('2d');
            pnlChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: [],
                    datasets: [{
                        label: 'PnL',
                        data: [],
                        borderColor: '#00ff88',
                        borderWidth: 2,
                        fill: true,
                        backgroundColor: (context) => {
                            const chart = context.chart;
                            const {ctx, chartArea} = chart;
                            if (!chartArea) return null;
                            const gradient = ctx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
                            const lastValue = pnlHistory[pnlHistory.length - 1] || 0;
                            if (lastValue >= 0) {
                                gradient.addColorStop(0, 'rgba(0, 255, 136, 0.15)');
                                gradient.addColorStop(1, 'rgba(0, 255, 136, 0)');
                            } else {
                                gradient.addColorStop(0, 'rgba(255, 51, 85, 0.15)');
                                gradient.addColorStop(1, 'rgba(255, 51, 85, 0)');
                            }
                            return gradient;
                        },
                        tension: 0.3,
                        pointRadius: 0,
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    animation: false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: { legend: { display: false }, tooltip: { enabled: false } },
                    scales: {
                        x: { display: false },
                        y: {
                            position: 'right',
                            grid: { color: 'rgba(255,255,255,0.03)', drawBorder: false },
                            ticks: {
                                color: '#444',
                                font: { family: 'JetBrains Mono', size: 10 },
                                callback: (v) => '$' + v.toFixed(2)
                            }
                        }
                    }
                }
            });
        }

        function updateChart(pnl) {
            pnlHistory.push(pnl);
            if (pnlHistory.length > maxPoints) pnlHistory.shift();
            pnlChart.data.labels = pnlHistory.map((_, i) => i);
            pnlChart.data.datasets[0].data = pnlHistory;
            pnlChart.data.datasets[0].borderColor = pnl >= 0 ? '#00ff88' : '#ff3355';
            pnlChart.update('none');
        }

        function formatPnl(v) {
            const sign = v >= 0 ? '+' : '';
            return sign + '$' + Math.abs(v).toFixed(2);
        }

        function formatTime(minutes) {
            const m = Math.floor(minutes);
            const s = Math.round((minutes - m) * 60);
            return m + ':' + String(s).padStart(2, '0');
        }

        function handleStateUpdate(d) {
            const pnl = d.total_pnl || 0;
            const pnlEl = document.getElementById('pnl');
            pnlEl.textContent = formatPnl(pnl);
            pnlEl.className = 'header-stat-value ' + (pnl >= 0 ? 'positive' : 'negative');

            if (pnlChart) updateChart(pnl);

            const tradeCount = d.trade_count || 0;
            const wins = d.win_count || 0;
            const wr = tradeCount > 0 ? (wins / tradeCount * 100) : 0;

            document.getElementById('trades').textContent = tradeCount;
            const wrEl = document.getElementById('winrate');
            wrEl.textContent = wr.toFixed(0) + '%';
            wrEl.className = 'header-stat-value ' + (wr >= 50 ? 'green' : wr > 0 ? 'red' : '');

            // Stats row
            document.getElementById('arbs-detected').textContent = d.arbs_detected || 0;
            document.getElementById('avg-arb').textContent = (d.avg_arb_cents || 0).toFixed(1) + '¢';
            document.getElementById('avg-liquidity').textContent = Math.round(d.avg_liquidity || 0);
            const avgProfit = d.avg_profit_cents || 0;
            const avgProfitEl = document.getElementById('avg-profit');
            avgProfitEl.textContent = avgProfit.toFixed(1) + '¢';
            avgProfitEl.className = 'stat-value ' + (avgProfit >= 0 ? 'green' : 'red');

            // Markets
            const markets = d.markets || {};
            const container = document.getElementById('markets-container');
            const marketKeys = Object.keys(markets);

            if (marketKeys.length > 0) {
                container.innerHTML = marketKeys.map(cid => {
                    const m = markets[cid];
                    const timeLeft = m.time_left || 0;
                    const vel = m.velocity || 0;
                    const timePercent = (timeLeft / 15) * 100;
                    const deltaClass = vel > 0.001 ? 'up' : vel < -0.001 ? 'down' : '';
                    const deltaSign = vel >= 0 ? '+' : '';

                    return `
                        <div class="market-card">
                            <div class="market-top">
                                <span class="market-asset">${m.asset || '???'}</span>
                                <span class="market-timer ${timeLeft < 2 ? 'urgent' : ''}">${formatTime(timeLeft)}</span>
                            </div>
                            <div class="market-mid">
                                <span class="market-prob">${((m.prob || 0) * 100).toFixed(1)}</span>
                                <span class="market-delta ${deltaClass}">${deltaSign}${(vel * 100).toFixed(2)}%</span>
                            </div>
                            <div class="no-position">—</div>
                            <div class="time-progress" style="width:${timePercent}%"></div>
                        </div>
                    `;
                }).join('');
            }
        }

        function handleTrade(t) {
            const isLong = t.action?.includes('YES') || t.action?.includes('UP');
            const hasPnl = t.pnl != null;
            const isWin = hasPnl && t.pnl >= 0;

            trades.unshift({
                asset: t.asset,
                side: isLong ? 'long' : 'short',
                pnl: t.pnl,
                size: t.size,
                time: t.time,
                isWin: isWin
            });

            if (trades.length > 50) trades.pop();

            document.getElementById('trades-list').innerHTML = trades.map(tr => {
                const statusClass = tr.pnl == null ? 'pending' : (tr.isWin ? 'win' : 'loss');
                const pnlClass = tr.pnl == null ? '' : (tr.pnl >= 0 ? 'positive' : 'negative');
                const pnlText = tr.pnl != null ? formatPnl(tr.pnl) : '$' + (tr.size || 0).toFixed(0);

                return `
                    <div class="trade-item ${statusClass}">
                        <span class="trade-side ${tr.side}">${tr.side}</span>
                        <div class="trade-details">
                            <div class="trade-asset">${tr.asset}</div>
                            <div class="trade-meta">${tr.time}</div>
                        </div>
                        <span class="trade-pnl ${pnlClass}">${pnlText}</span>
                    </div>
                `;
            }).join('');
        }

        connect();
    </script>
</body>
</html>
"##;
