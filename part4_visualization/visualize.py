"""Part 4 - Static visualisations of the analytical results (PNG, for the report).

Reads the CSV results written by parts 1-3 (outputs/results) and writes figures to
outputs/figures. Colours: validated reference palette (blue for single series, the
first categorical slots for comparisons, a one-hue blue ramp for magnitude);
text uses neutral ink, never the series colour; one y-axis per chart.

Usage:  python -m part4_visualization.visualize
"""
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt                      # noqa: E402
import matplotlib.ticker as mtick                    # noqa: E402
import numpy as np                                   # noqa: E402
import pandas as pd                                  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402

from common import config                            # noqa: E402

SURFACE, INK, INK2, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#8a8984", "#e6e5e1"
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
NEUTRAL = "#c9c8c2"
SEQ = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
ORDINAL = ["#0d366b", "#184f95", "#256abf", "#3987e5", "#6da7ec"]   # dark -> light, all >= 2:1

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "DejaVu Sans", "font.size": 9.5, "text.color": INK,
    "axes.labelcolor": INK2, "axes.edgecolor": GRID, "axes.titlesize": 12,
    "axes.titleweight": "bold", "axes.titlelocation": "left", "axes.titlecolor": INK,
    "xtick.color": INK2, "ytick.color": INK2, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.8, "axes.spines.top": False, "axes.spines.right": False,
    "legend.frameon": False, "text.parse_math": False, "legend.labelcolor": INK2,
})

R = lambda name: pd.read_csv(os.path.join(config.RESULTS_DIR, name))
money = mtick.FuncFormatter(lambda v, _: f"${v / 1e6:.1f}M" if abs(v) >= 1e6 else f"${v / 1e3:.0f}k")


def subtitle(ax, text):
    ax.text(0, 1.02, text, transform=ax.transAxes, color=INK2, fontsize=9, va="bottom")


def save(fig, name):
    path = os.path.join(config.FIGURES_DIR, name)
    fig.savefig(path, dpi=170, bbox_inches="tight")
    plt.close(fig)
    print("wrote", path)


# 1 ------------------------------------------------------------------ sales over time
def fig_daily_revenue():
    df = R("sparksql_q6_daily_revenue_by_channel.csv")
    df["txn_date"] = pd.to_datetime(df["txn_date"])
    piv = df.pivot(index="txn_date", columns="channel", values="revenue").iloc[1:]   # drop partial first day
    fig, ax = plt.subplots(figsize=(8.6, 3.6))
    for col, color, label in [("web_session", BLUE, "Web sessions"), ("direct", ORANGE, "Direct orders")]:
        s = piv[col].rolling(7, min_periods=1).mean()
        ax.plot(piv.index, piv[col], color=color, alpha=0.25, lw=1)
        ax.plot(s.index, s, color=color, lw=2, label=f"{label} (7-day avg)")
        ax.annotate(f"{label}\n{money(s.iloc[-1], None)}/day", (s.index[-1], s.iloc[-1]),
                    xytext=(6, 0), textcoords="offset points", va="center", color=INK2, fontsize=8.5)
    ax.yaxis.set_major_formatter(money)
    ax.set_ylim(0, None)
    ax.set_title("Daily revenue by channel, Aug–Oct 2026", pad=22)
    subtitle(ax, f"Total ${df['revenue'].sum() / 1e6:,.0f}M over 90 days; web sessions bring "
                 f"{piv['web_session'].sum() / piv.sum().sum():.0%} of revenue")
    ax.legend(loc="lower left", ncol=2)
    ax.margins(x=0.01)
    ax.spines["left"].set_visible(False)
    save(fig, "fig1_daily_revenue_by_channel.png")


# 2 ----------------------------------- revenue & margin by category
def fig_category_margin():
    df = R("mongo_revenue_by_category_month.csv")
    cat = df.groupby("category_name")[["net_revenue", "gross_margin"]].sum()
    cat["margin_pct"] = 100 * cat["gross_margin"] / cat["net_revenue"]
    cat = cat.sort_values("gross_margin")
    fig, ax = plt.subplots(figsize=(8.6, 6.4))
    y = np.arange(len(cat))
    ax.barh(y, cat["gross_margin"], color=BLUE, height=0.72, edgecolor=SURFACE, linewidth=1)
    for i, (_, r) in enumerate(cat.iterrows()):
        ax.text(r["gross_margin"], i, f"  {r['margin_pct']:.0f}% margin", va="center",
                color=INK2, fontsize=7.5)
    ax.set_yticks(y, [n[:34] for n in cat.index], fontsize=8)
    ax.xaxis.set_major_formatter(money)
    ax.grid(axis="y", visible=False)
    ax.set_xlim(0, cat["gross_margin"].max() * 1.18)
    ax.set_title("Gross margin by category (90 days)", pad=22)
    subtitle(ax, f"Revenue per category is similar (${cat['net_revenue'].min() / 1e6:.0f}–"
                 f"{cat['net_revenue'].max() / 1e6:.0f}M); margin rates range "
                 f"{cat['margin_pct'].min():.0f}–{cat['margin_pct'].max():.0f}%, so margin, not sales, separates them")
    save(fig, "fig2_gross_margin_by_category.png")


# 3 -------------------------------------------------------------------- conversion funnel
def fig_funnel():
    df = R("sparksql_q1b_funnel_overall.csv").sort_values("step")
    fig, ax = plt.subplots(figsize=(8.6, 3.4))
    y = np.arange(len(df))[::-1]
    ax.barh(y, df["sessions"], color=ORDINAL, height=0.68)
    prev = None
    for yi, (_, r) in zip(y, df.iterrows()):
        step = f"  ·  {r['sessions'] / prev:.0%} of previous stage" if prev else ""
        ax.text(r["sessions"], yi, f"  {r['sessions'] / 1e6:.2f}M ({r['sessions'] / df['sessions'].iloc[0]:.0%}){step}",
                va="center", color=INK2, fontsize=8.5)
        prev = r["sessions"]
    ax.set_yticks(y, df["stage"].str.capitalize())
    ax.set_xlim(0, df["sessions"].max() * 1.55)
    ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"{v / 1e6:.1f}M"))
    ax.grid(axis="y", visible=False)
    ax.set_title("Session conversion funnel (2M sessions)", pad=22)
    carts = df.set_index("stage")["sessions"]
    subtitle(ax, f"{1 - carts['purchased'] / carts['added to cart']:.0%} of carts are abandoned; "
                 f"the largest leak is cart → checkout")
    save(fig, "fig3_conversion_funnel.png")


# 4 --------------------------------------------------------- top-selling vs most viewed
def fig_sold_vs_viewed():
    df = R("sparksql_q7_top_sold_vs_viewed.csv")
    df["sold_out"] = df["current_stock"] == 0
    top_sold = df.nsmallest(10, "sales_rank")
    fig, ax = plt.subplots(figsize=(8.6, 4.4))
    for flag, color, label in [(False, BLUE, "In stock at end of window"),
                               (True, ORANGE, "Sold out (stock = 0)")]:
        part = df[df["sold_out"] == flag]
        ax.scatter(part["views"], part["units_sold"], s=9, color=color, alpha=0.55, lw=0,
                   label=f"{label} ({len(part):,})")
    ax.scatter(top_sold["views"], top_sold["units_sold"], s=60, facecolor="none", edgecolor=INK,
               lw=1.2, label="Top 10 by units sold")
    r = np.corrcoef(df["views"], df["units_sold"])[0, 1]
    g = df.groupby("sold_out")["revenue"].mean()
    ax.set_xlabel("Product-detail page views")
    ax.set_ylabel("Units sold")
    ax.set_title("Views drive sales; sold-out products drop out of browsing", pad=22)
    subtitle(ax, f"Views vs units r = {r:.2f}. {df['sold_out'].sum():,} products sold out and earned "
                 f"${g[True] / 1e3:.0f}k each vs ${g[False] / 1e3:.0f}k for in-stock products")
    ax.legend(loc="upper left", markerscale=1.6)
    save(fig, "fig4_top_sold_vs_most_viewed.png")


# 5 ---------------------------------------------------------- customer segmentation
def fig_segments():
    age = R("mongo_segment_by_age_group.csv")
    q = R("mongo_segment_by_spend_quartile.csv").reset_index(drop=True)
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(8.6, 3.6), gridspec_kw={"wspace": 0.35})
    a1.bar(age["age_group"], age["users"], color=BLUE, width=0.65)
    for x, (_, r) in enumerate(age.iterrows()):
        a1.text(x, r["users"], f"${r['avg_spent'] / 1e3:.1f}k\navg", ha="center", va="bottom",
                color=INK2, fontsize=7.5)
    a1.set_ylim(0, age["users"].max() * 1.25)
    a1.set_title("Users by age group", fontsize=10.5)
    a1.set_ylabel("Users")
    a1.grid(axis="x", visible=False)
    labels = ["Q4 (lowest)", "Q3", "Q2", "Q1 (top)"]
    share = 100 * q["revenue"] / q["revenue"].sum()
    a2.bar(labels, share, color=ORDINAL[::-1][1:], width=0.65)
    for x, (s, aov) in enumerate(zip(share, q["avg_order_value"])):
        a2.text(x, s, f"{s:.0f}%\nAOV ${aov:.0f}", ha="center", va="bottom", color=INK2, fontsize=7.5)
    a2.set_ylim(0, share.max() * 1.3)
    a2.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
    a2.set_title("Revenue share by spend quartile", fontsize=10.5)
    a2.grid(axis="x", visible=False)
    fig.suptitle("Customer segmentation (MongoDB $facet / $bucketAuto)", x=0.125, ha="left",
                 fontweight="bold", fontsize=12, y=1.04)
    fig.text(0.125, 0.965, "Spend per user barely varies with age; the top quartile (2,500 users) brings "
             f"{share.iloc[-1]:.0f}% of revenue", color=INK2, fontsize=9)
    save(fig, "fig5_customer_segmentation.png")


# 6 -------------------------------------------------------------------- CLV tiers
def fig_clv():
    t = R("integrated_clv_tiers.csv")
    c = R("integrated_clv_correlations.csv")
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(8.6, 3.7), gridspec_kw={"wspace": 0.7,
                                                                         "width_ratios": [1, 1.25]})
    ticks = [f"{tier}\n{a:.0f}%" for tier, a in zip(t["clv_tier"], t["cart_abandonment_pct"])]
    a1.bar(ticks, t["avg_clv_12m"], color=ORDINAL[:4], width=0.65)
    for x, (_, r) in enumerate(t.iterrows()):
        a1.text(x, r["avg_clv_12m"], f"${r['avg_clv_12m'] / 1e3:.0f}k", ha="center", va="bottom",
                color=INK2, fontsize=8)
    a1.tick_params(axis="x", labelsize=8)
    a1.set_xlabel("CLV tier (cart abandonment rate)", fontsize=8.5)
    a1.set_ylim(0, t["avg_clv_12m"].max() * 1.2)
    a1.yaxis.set_major_formatter(money)
    a1.set_title("Avg 12-month CLV by tier", fontsize=10.5)
    a1.grid(axis="x", visible=False)
    names = {"orders": "Orders (90d)", "session_to_order_pct": "Sessions that convert",
             "aov": "Avg order value", "cart_abandonment_rate": "Cart abandonment",
             "sessions": "Sessions (HBase)", "margin_rate": "Margin rate", "avg_pages": "Pages / session",
             "avg_session_sec": "Session length"}
    c = c.iloc[::-1]
    colors = [BLUE if v >= 0 else ORANGE for v in c["corr_with_clv"]]
    a2.barh([names[m] for m in c["metric"]], c["corr_with_clv"], color=colors, height=0.65)
    for i, v in enumerate(c["corr_with_clv"]):
        a2.text(v + (0.02 if v >= 0 else -0.02), i, f"{v:+.2f}", va="center",
                ha="left" if v >= 0 else "right", color=INK2, fontsize=7.5)
    a2.axvline(0, color=MUTED, lw=0.8)
    a2.set_xlim(-1.0, 1.0)
    a2.set_title("Correlation with CLV", fontsize=10.5)
    a2.grid(axis="y", visible=False)
    fig.suptitle("Customer lifetime value: MongoDB value × HBase engagement (2,000 users)",
                 x=0.125, ha="left", fontweight="bold", fontsize=12, y=1.04)
    fig.text(0.125, 0.965, "Top tier = 25% of customers, "
             f"{t['pct_of_total_clv'].iloc[0]:.0f}% of projected value; abandonment falls as CLV rises",
             color=INK2, fontsize=9)
    save(fig, "fig6_clv_tiers.png")


# 7 -------------------------------------------------------------------- cohort heatmap
def fig_cohort():
    df = R("spark_cohort_months_since_registration.csv")
    piv = df.pivot(index="registration_month", columns="months_since_registration",
                   values="revenue_per_member")
    fig, ax = plt.subplots(figsize=(8.6, 3.6))
    cmap = LinearSegmentedColormap.from_list("seq", SEQ)
    im = ax.imshow(piv.values, cmap=cmap, aspect="auto")
    for i in range(piv.shape[0]):
        for j in range(piv.shape[1]):
            v = piv.values[i, j]
            if not np.isnan(v):
                ax.text(j, i, f"${v / 1e3:.1f}k", ha="center", va="center", fontsize=7.5,
                        color="white" if v > np.nanpercentile(piv.values, 50) else INK)
    ax.set_xticks(range(piv.shape[1]), piv.columns)
    ax.set_yticks(range(piv.shape[0]), piv.index)
    ax.set_xlabel("Months since registration")
    ax.set_ylabel("Registration cohort")
    ax.grid(False)
    for s in ax.spines.values():
        s.set_visible(False)
    cb = fig.colorbar(im, ax=ax, shrink=0.85,
                      format=mtick.FuncFormatter(lambda v, _: f"${v / 1e3:.1f}k"))
    cb.outline.set_visible(False)
    cb.ax.tick_params(colors=INK2, labelsize=8)
    ax.set_title("Cohort analysis: revenue per cohort member per month", pad=22)
    subtitle(ax, "Each cohort is visible only for the 3 months of activity data; spend per member is "
                 "flat across cohorts: the colour scale spans only $12.5–13.5k/month")
    save(fig, "fig7_cohort_heatmap.png")


if __name__ == "__main__":
    fig_daily_revenue()
    fig_category_margin()
    fig_funnel()
    fig_sold_vs_viewed()
    fig_segments()
    fig_clv()
    fig_cohort()
