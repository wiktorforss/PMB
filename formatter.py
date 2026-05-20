"""
Formatter — renders data into clean Telegram Markdown messages.
"""

from src.scorer import LeaderboardEntry
from src.signals import Signal


def format_leaderboard(entries: list[LeaderboardEntry]) -> str:
    if not entries:
        return (
            "📭 *No sharp wallets found*\n\n"
            "Either the API returned no data, or no wallets met the accuracy threshold. "
            "Try again in a few minutes."
        )

    lines = ["🏆 *Sharp Wallet Leaderboard*\n", "_Ranked by accuracy score — not PnL_\n"]

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}

    for entry in entries:
        w = entry.wallet
        rank_icon = medals.get(entry.rank, f"`#{entry.rank}`")
        name = w.display_name[:20]

        # Confidence bar (visual)
        score_pct = min(w.confidence_score * 500, 100)  # scale to 0-100
        bar_len = int(score_pct / 10)
        bar = "█" * bar_len + "░" * (10 - bar_len)

        pnl_str = f"+${w.pnl:,.0f}" if w.pnl >= 0 else f"-${abs(w.pnl):,.0f}"

        lines.append(
            f"{rank_icon} *{name}*\n"
            f"   Accuracy: `{w.accuracy_pct}` ({w.correct_trades}/{w.total_trades} calls)\n"
            f"   Edge: `{bar}` {w.confidence_score:.3f}\n"
            f"   PnL: `{pnl_str}` | Vol: `${w.vol:,.0f}`\n"
        )

    lines.append(
        "\n_Edge score = Wilson lower bound above 50% baseline._\n"
        "_Only wallets with 5+ scoreable trades qualify._"
    )

    return "\n".join(lines)


def format_signals(signals: list[Signal]) -> str:
    if not signals:
        return (
            "📭 *No active signals*\n\n"
            "Sharp wallets aren't converging on any market right now. "
            "Check back soon — signals update every 3 minutes."
        )

    lines = ["📡 *Active Signals*\n", "_Markets where sharp wallets independently agree_\n"]

    for i, sig in enumerate(signals, 1):
        # Direction emoji
        direction = "🟢" if sig.outcome == "YES" else "🔴"

        # Strength indicator
        if sig.num_wallets >= 4:
            strength = "🔥 STRONG"
        elif sig.num_wallets == 3:
            strength = "⚡ SOLID"
        else:
            strength = "👀 WATCH"

        # Price display
        price_pct = sig.avg_price * 100
        implied_odds = f"{price_pct:.0f}¢"

        wallet_names = ", ".join(sig.wallets[:4])
        if len(sig.wallets) > 4:
            wallet_names += f" +{len(sig.wallets) - 4} more"

        title_display = sig.market_title[:50]
        if len(sig.market_title) > 50:
            title_display += "…"

        lines.append(
            f"*{i}. {direction} {title_display}*\n"
            f"   Signal: `{strength}` — {sig.num_wallets} sharp wallets on *{sig.outcome}*\n"
            f"   Avg entry: `{implied_odds}` | Size: `${sig.total_size:,.0f}`\n"
            f"   Wallets: _{wallet_names}_\n"
        )

        if sig.url:
            lines.append(f"   [View on Polymarket]({sig.url})\n")

    lines.append(
        "\n_A signal fires when 2+ top-ranked wallets independently hold the same side._\n"
        "_This is not financial advice._"
    )

    return "\n".join(lines)
