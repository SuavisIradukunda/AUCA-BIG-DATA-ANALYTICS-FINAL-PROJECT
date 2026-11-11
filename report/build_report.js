// Builds report/Technical_Report.docx  (node report/build_report.js)
const fs = require("fs");
const path = require("path");
const {
  Document, Packer, Paragraph, TextRun, ImageRun, Table, TableRow, TableCell,
  AlignmentType, HeadingLevel, LevelFormat, WidthType, ShadingType, BorderStyle,
  Footer, PageNumber, TabStopType,
} = require("docx");

const FIG = path.join(__dirname, "..", "outputs", "figures");
const FONT = "Arial";
const W = 9638; // A4 text width in DXA with 2 cm margins

// ---------- helpers ----------
function runs(text, base = {}) {
  // **bold** and `code` inline markup
  const out = [];
  const re = /(\*\*[^*]+\*\*|`[^`]+`)/g;
  let last = 0, m;
  while ((m = re.exec(text))) {
    if (m.index > last) out.push(new TextRun({ text: text.slice(last, m.index), ...base }));
    const t = m[0];
    if (t.startsWith("**")) out.push(new TextRun({ text: t.slice(2, -2), bold: true, ...base }));
    else out.push(new TextRun({ text: t.slice(1, -1), font: "Consolas", size: 18, ...base }));
    last = m.index + t.length;
  }
  if (last < text.length) out.push(new TextRun({ text: text.slice(last), ...base }));
  return out;
}
const P = (text, opts = {}) => new Paragraph({ children: runs(text), spacing: { after: 100, line: 264 }, ...opts });
const H1 = (t) => new Paragraph({ heading: HeadingLevel.HEADING_1, keepNext: true, keepLines: true, children: [new TextRun(t)] });
const H2 = (t) => new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun(t)] });
const B = (text, level = 0) => new Paragraph({ numbering: { reference: "bullets", level },
  children: runs(text), spacing: { after: 50, line: 252 } });
const N = (text) => new Paragraph({ numbering: { reference: "steps", level: 0 },
  children: runs(text), spacing: { after: 50, line: 252 } });

let figNo = 0;
function figure(file, caption, widthCm = 15) {
  figNo += 1;
  const buf = fs.readFileSync(path.join(FIG, file));
  const w = buf.readUInt32BE(16), h = buf.readUInt32BE(20);
  const px = widthCm / 2.54 * 96;
  return [
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 80, after: 40 }, keepNext: true,
      children: [new ImageRun({ type: "png", data: buf, transformation: { width: px, height: px * h / w },
        altText: { title: caption, description: caption, name: file } })] }),
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 160 },
      children: [new TextRun({ text: `Figure ${figNo}. `, bold: true, size: 17, color: "52514E" }),
                 new TextRun({ text: caption, size: 17, color: "52514E" })] }),
  ];
}

const border = { style: BorderStyle.SINGLE, size: 4, color: "D9D8D4" };
const borders = { top: border, bottom: border, left: border, right: border };
function table(headers, rows, widths) {
  const total = widths.reduce((a, b) => a + b, 0);
  const cell = (text, i, head) => new TableCell({
    borders, width: { size: widths[i], type: WidthType.DXA },
    shading: head ? { fill: "E8F0FB", type: ShadingType.CLEAR, color: "auto" } : undefined,
    margins: { top: 50, bottom: 50, left: 90, right: 90 },
    children: [new Paragraph({ children: runs(text, { size: 17, bold: head || undefined }) })],
  });
  return new Table({
    width: { size: total, type: WidthType.DXA }, columnWidths: widths,
    rows: [new TableRow({ tableHeader: true, children: headers.map((h, i) => cell(h, i, true)) }),
           ...rows.map((r) => new TableRow({ cantSplit: true, children: r.map((c, i) => cell(c, i, false)) }))],
  });
}
const caption = (t) => new Paragraph({ spacing: { before: 60, after: 160 },
  children: [new TextRun({ text: t, size: 17, color: "52514E", italics: true })] });

// ---------- content ----------
const body = [];
const add = (...xs) => xs.flat().forEach((x) => body.push(x));

add(
  new Paragraph({ spacing: { after: 60 }, children: [new TextRun({ text: "Distributed Multi-Model Analytics for E-commerce Data", bold: true, size: 36, font: FONT })] }),
  new Paragraph({ spacing: { after: 40 }, children: [new TextRun({ text: "Technical Report – AUCA Big Data Analytics Final Project", size: 22, color: "52514E" })] }),
  new Paragraph({ spacing: { after: 200 }, border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: "2A78D6", space: 6 } },
    children: [new TextRun({ text: "Author: Irumva   |   November 2026   |   Stack: MongoDB 8.0, HBase data model (happybase API), Apache Spark 4.3 (PySpark), Python 3.13", size: 18, color: "52514E" })] }),

  H1("1. Summary"),
  P("The project builds an analytics system over a synthetic e-commerce platform (10,000 users, 5,000 products, 25 categories, 2,000,000 sessions with 20.0 million page views, and 500,000 orders with 802,251 line items over 90 days, 3.7 GB of JSON). Each technology holds the data it suits: **MongoDB** stores the catalogue, customer profiles and orders as rich documents; **HBase** stores the high-volume, time-ordered browsing stream and daily product metrics; **Spark** cleans the full dataset, runs the heavy batch analytics and joins data across the stores."),
  P("Main findings: web sessions bring 80% of the $387.7M revenue; 69% of carts are abandoned, and the biggest leak is between cart and checkout; 1,927 products (39%) sold out during the window and earned half the revenue of in-stock products; category margin rates range from 18% to 36% on similar revenue; the top 25% of customers hold 31% of projected 12-month value, and cart abandonment is the engagement signal most strongly (negatively) linked to customer value."),

  H1("2. System architecture"),
  P("Raw JSON Lines files produced by the corrected generator are the single source. Two loaders write the operational stores; Spark reads the raw files once, writes typed Parquet, and runs all heavy computation. For cross-store analytics Spark pulls data from MongoDB (aggregation results or projected documents) and HBase (row-key scans) into DataFrames and joins them."),
  table(["Layer", "Technology", "Holds / does", "Access pattern"], [
    ["Ingestion", "Python generator → JSONL", "users, products, categories, sessions_0..19, transactions", "one record per line, splittable"],
    ["Document store", "MongoDB 8.0", "categories, products, users (+purchase summary), transactions (+enriched items)", "lookups by id, rich aggregation pipelines"],
    ["Wide-column store", "HBase (happybase / Thrift)", "user_sessions (time series), product_metrics (daily)", "row-key prefix and range scans"],
    ["Processing", "Spark 4.3 (PySpark, SQL)", "cleaning, Parquet, recommendations, cohorts, SQL, CLV join", "full scans, shuffles, joins"],
    ["Presentation", "matplotlib", "7 static figures", "reads CSV results"],
  ], [1500, 2100, 3538, 2500]),
  caption("Table 1. Components. Every step is a module run by run_all.sh; settings are environment variables (common/config.py)."),
  P("**Generator corrections.** The supplied generator was fixed before use so that the data obeys the relationships in the brief: products now carry `subcategory_id`; stock is consumed in time order and a product that sells out becomes inactive; only active, in-stock products can be added to carts or sold; `conversion_status` matches the transactions exactly (converted ⇔ one transaction); prices come from the price history at purchase time; confirmation pages appear only in converted sessions; session IDs are unique (10-hex IDs collided at 2M sessions); and sessions are streamed to disk instead of held in RAM. A validation script confirmed zero violations of these rules on the full dataset."),

  H1("3. Data modelling (Part 1)"),
  H2("3.1 MongoDB: document model"),
  B("**products** – the category name and the full subcategory (name, `profit_margin`) are **denormalised** into each product; `price_history` stays **embedded** (bounded: 1–3 entries). A product page or a margin calculation is one read, no `$lookup`."),
  B("**transactions** – line items are **embedded** (an order is read and written as a unit). At load time each item is enriched with `category_id`, `subcategory_id` and `profit_margin` (snapshot pattern: the margin that applied when the order was placed), so revenue and margin by category are a single `$unwind` + `$group`."),
  B("**users** – profile plus an embedded, pre-computed `purchase_summary` (orders, spend, AOV, first/last purchase, top-3 categories; computed pattern). Segmentation reads one collection. In production the summary is refreshed by a change stream or a nightly job."),
  B("**categories** – subcategories embedded (small, always read together)."),
  B("**Indexes**: `{user_id, timestamp}` and `{timestamp}` on transactions, sparse `session_id`, multikey `items.product_id`; category/price and stock indexes on products; age group, state and spend on users."),
  P("Loading all 10k users, 5k products and 500k transactions takes 19 s (unordered `insert_many` batches of 10,000, duplicate-tolerant). Three pipelines were implemented (`aggregations.py`): (1) **revenue and gross margin by category and month**, allocating each order's discount to its items (`total/subtotal`) then `$lookup` to category names (2.2 s over 500k orders); (2) **top-selling products** with distinct buyers via `$addToSet`/`$size` and a correlated `$lookup` pipeline into the catalogue; (3) **user segmentation** with `$facet`, computing age-group, purchase-frequency (`$bucket`) and spend-quartile (`$bucketAuto`) segments in one collection scan."),
  H2("3.2 HBase: wide-column model"),
  table(["Table", "Row key", "Column families", "Serves"], [
    ["user_sessions", "`user_id#reverse_ts#session_id`, reverse_ts = 9999999999 − epoch", "`s` session metadata (13 cols); `pv` one column per page view, qualifier 000..015; `c` one column per cart item", "latest N sessions (prefix scan + LIMIT), sessions in a time window (STARTROW/STOPROW), journey of one session"],
    ["product_metrics", "`product_id#yyyymmdd`", "`m`: views, cart_adds, cart_units, purchases, units_sold, revenue", "a product's daily history and view→purchase conversion over any date range"],
  ], [1500, 2600, 3038, 2500]),
  caption("Table 2. HBase schema (DDL with pre-split regions, ROW bloom filters, VERSIONS=1 and Snappy in part1_hbase/schema.hbase)."),
  P("**Why this key.** Leading with `user_id` keeps each user's sessions contiguous and spreads writes across regions (a timestamp-first key would send every new session to the same region). The reversed timestamp sorts a user's newest session first, so \"last 5 sessions\" stops after 5 rows. Page views are **sparse columns** in their own family: 4–16 per row, and they are read only by journey analysis, so session lists that read family `s` never touch them. Families have short names because HBase stores the family name in every cell."),
  P("**Load.** `user_sessions` holds **all sessions of a 2,000-user sample** (20% of users, 400,949 sessions spanning all 90 days). Sampling by user rather than by file keeps every loaded user's history complete, which is what per-user queries and the CLV engagement metrics need. `product_metrics` holds 349,940 product-days computed from all 2M sessions and 500k orders. Sample queries ran in 0.3–3 ms (latest 5 sessions, a 15-day window with 31 sessions, a 13-week conversion scan for one product)."),
  P("**Execution note.** No HBase server could be installed in the build environment (Docker Hub and Apache mirrors were blocked; MongoDB and Spark ran for real). The HBase code therefore ran on `LocalHBaseEmulator`, a small stand-in implementing the happybase calls used (sorted row keys, families, prefix/range scans). The same code connects to real HBase through Thrift when available (`docker compose up`); results are identical in content, but the timings above are not HBase timings."),
  H2("3.3 What goes where, and why"),
  table(["Data", "Store", "Reason"], [
    ["Catalogue, categories", "MongoDB", "small, hierarchical, read as whole entities, updated in place (stock, price)"],
    ["Orders with items", "MongoDB", "natural documents; ad-hoc business aggregations; secondary indexes on several fields"],
    ["User profiles + summary", "MongoDB", "flexible attributes, embedded aggregates for segmentation"],
    ["Sessions & page views", "HBase", "2M rows / 20M events, append-only, always read per user in time order; sparse columns"],
    ["Daily product metrics", "HBase", "time series keyed by entity + day; counter-style increments at high write rates"],
    ["Full-history analytics", "Spark on Parquet", "full scans, large joins and shuffles that neither store is built for"],
  ], [2300, 1700, 5638]),

  new Paragraph({ heading: HeadingLevel.HEADING_1, pageBreakBefore: true, children: [new TextRun("4. Spark processing (Part 2)")] }),
  P("**Cleaning and normalisation** (`spark_clean.py`, 161 s on 2 cores). JSON is read with **explicit schemas** (no inference pass over 3.7 GB; type errors fail fast; `cart_contents` declared as a map). Rules: ISO strings to timestamps; trimmed, case-normalised codes and enums; missing ages imputed with the median, missing referrers set to \"unknown\", missing discounts to 0; rows without keys or with unparseable timestamps dropped; de-duplication on primary keys (latest record wins); stock clamped at 0 and sold-out products marked inactive; sessions and orders of unknown users removed (left-semi join). Nested data is normalised into analysis tables (`page_views` via `posexplode`, `cart_items` from the map, `transaction_items` with the order discount allocated to items and margins attached by a broadcast join). Output is Parquet, with sessions and page views partitioned by date: 402 MB instead of 3.7 GB, and date filters prune partitions. Because the corrected generator produces valid data, no rows were removed (data-quality report in `spark_data_quality.csv`); the rules are defensive."),
  P("**Recommendations** (`spark_recommendations.py`, 86 s). Market-basket metrics (support, confidence, lift) are computed for products bought together (500k baskets) and viewed together (1.98M sessions). Pairs come from a self-join on basket id restricted to `a < b` (half the shuffle, no self-pairs); per-product counts are broadcast. The two signals are blended (0.7 purchase + 0.3 view) into a top-5 list per product, restricted to sellable products (4,608 products covered)."),
  P("**Cohort analysis** (`spark_cohort.py`). Users are grouped by registration month (Feb–Aug 2026), and revenue, active buyers and orders are measured by months since registration and by calendar month, along with month-over-month buyer retention."),
  P("**Spark SQL** (`spark_sql_analytics.py`, 10 queries, 0.5–3.6 s each). These include a funnel with a CTE and conditional aggregation; top-3 products per category with `RANK()` and share of category revenue via window sums; referrer performance (sessions LEFT JOIN orders); revenue by weekday and hour; a price-change impact study (`LAG` over the price history, 14 days before vs after); and sold vs viewed products. Two queries run on data from the operational stores: MongoDB user profiles (pymongo → DataFrame) joined in SQL by age group and region, and HBase `product_metrics` (row keys split into product and day) joined with the MongoDB catalogue for category-level view→purchase conversion. On a cluster the MongoDB Spark Connector and an HBase connector would replace the two loaders; the SQL is unchanged."),
  P("**Optimisation choices:** explicit schemas; columnar Parquet with date partitioning; 16 shuffle partitions instead of 200 for a 2-core machine, with adaptive query execution on; broadcast joins for the small dimension tables (5k products, 25 categories); `cache()` only for tables reused several times; projection pushed into MongoDB."),

  H1("5. Integrated analytics: customer lifetime value (Part 3)"),
  P("**Business question:** which customers will be worth most over the next 12 months, and which browsing behaviours distinguish them, so marketing can protect the top tier and grow the next one?"),
  N("**MongoDB** – one aggregation over `transactions` returns, per user, orders, revenue, gross margin (from the margin-enriched items), discounts, web orders and first/last purchase (aggregation pushed down: 500k orders become 10k rows before leaving the database). Profiles (age group, state) come from `users`."),
  N("**HBase** – for each of the 2,000 sampled users, one prefix scan of `user_sessions` reading only family `s` gives sessions, average duration and pages, converted and abandoned sessions, and recency. Distinct users are enumerated by jumping between row-key prefixes (2,000 scans in 27 s on the emulator)."),
  N("**Spark** – joins the three sources on `user_id` and computes: monthly margin = 90-day margin / 3; CLV₁₂ = monthly margin × Σₜ₌₁..₁₂ rᵗ/(1+d)ᵗ, with r the month-over-month buyer retention measured by the cohort job and d = 1% per month; CLV quartile tiers; tier profiles; and correlations between CLV and each engagement metric."),
  P("Only aggregated, small data moves between systems (10k + 2k rows), so the join is cheap; the expensive parts (scanning orders, scanning sessions) run inside the store that indexes them. At full scale the HBase step would run as a parallel Spark scan by region, not one Python scan per user."),
  ...figure("fig6_clv_tiers.png", "CLV tiers and the engagement signals associated with them (2,000 users present in all three sources)."),
  P("The top tier (T1, 25% of customers) holds 31% of projected value (average $43k vs $27k for T4). Its advantage comes from more orders and a higher order value, not longer visits: session length has no relationship with CLV (r = 0.01), while cart abandonment falls steadily from 72% (T4) to 66% (T1), r = −0.56. **Action:** abandoned-cart recovery for T2–T3 customers is the clearest route to moving customers up a tier."),

  H1("6. Findings and business insights (Part 4)"),
  ...figure("fig1_daily_revenue_by_channel.png", "Daily revenue by channel (Spark SQL Q6). The late rise in direct orders is a generator artefact (section 8)."),
  P("Revenue is stable at about $4.3M per day. Web sessions bring 80% of it and orders not tied to a session bring 20%. Referrer (Spark SQL Q3) and device type (Q1) make almost no difference: conversion is 20.2–20.4% for every referrer and 20.3% on every device."),
  ...figure("fig3_conversion_funnel.png", "Session funnel over all 2M sessions (Spark SQL Q1b)."),
  P("Nearly every session views a product and 65% add to cart, but only 45% of cart sessions reach checkout, and 70% of those complete it. 69% of carts are abandoned. **Action:** the cart page (saved carts, shipping costs shown earlier, one-click checkout) is the highest-leverage fix; checkout itself converts well."),
  ...figure("fig4_top_sold_vs_most_viewed.png", "Units sold vs product-page views for 4,761 viewed products (Spark SQL Q7)."),
  P("Views and sales move together almost perfectly (r = 0.98), and the low-view tail consists almost entirely of products that sold out: 99% of products with fewer than 1,500 views have zero stock. Once a product sells out it disappears from browsing, so the 1,927 sold-out products earned $51k each against $102k for in-stock products. **Action:** stock-outs, not demand, cap sales; reorder points based on HBase daily metrics would protect the top sellers."),
  ...figure("fig2_gross_margin_by_category.png", "Gross margin by category (MongoDB pipeline 1).", 12.5),
  P("Categories sell similar amounts ($13–18M each) but keep very different shares as margin (18–36%), so the most profitable category earns twice the margin of the least profitable on similar sales. **Action:** promotions and ad spend should favour high-margin categories such as Rodriguez-Graham (36%) and Hancock Inc (30%)."),
  ...figure("fig5_customer_segmentation.png", "Customer segmentation (MongoDB pipeline 3)."),
  P("Average spend is almost identical across age groups ($38.6–38.9k), so age is not a useful targeting dimension in this data. Spend quartiles are: the top 2,500 users bring 31% of revenue with an order value 19% above the bottom quartile ($841 vs $707)."),
  ...figure("fig7_cohort_heatmap.png", "Revenue per cohort member by months since registration (Spark cohort job)."),
  P("All cohorts spend $12.5–13.5k per member per month and month-over-month buyer retention is 100%: older and newer customers behave the same. The rise across each row is a calendar effect (October has more trading days in the window than August), not a tenure effect. Two further Spark results support this reading: the 1,235 price changes in the window had no measurable effect on units sold (−0.7% after decreases, −1.1% after increases; Q5), and only 150 product pairs were bought together three or more times. With an expected co-occurrence of about 0.08 per pair, that count is consistent with chance, so the high lift values (median 34) are noise. The recommendation pipeline is sound, but this dataset contains no real basket affinity to find."),

  H1("7. Scalability and technology choices"),
  B("**MongoDB** scales out by sharding: transactions on a hashed `user_id` (even writes, per-user queries hit one shard) or `{user_id, timestamp}`; the catalogue stays unsharded and small. The `purchase_summary` keeps segmentation queries O(users) rather than O(orders)."),
  B("**HBase** scales by region splits: pre-split on `user_id` ranges (salted with a hash prefix if IDs were sequential) and product prefixes; append-only writes and per-user scans stay single-region operations however large the table grows. TTLs could age out old page views."),
  B("**Spark** scales linearly with executors: JSONL is splittable, Parquet is columnar and partitioned, and joins with small tables are broadcast. The jobs ran on 2 cores and 8 GB in about 6 minutes in total; on a cluster the same code runs unchanged with `SPARK_MASTER`."),
  B("**Selection logic:** use MongoDB when the query is about an entity and its nested parts (an order, a customer); use HBase when the query is \"a key's events over time\" at high volume; use Spark when the query scans everything or joins sources. The CLV query uses all three for exactly those reasons."),

  H1("8. Limitations and future work"),
  B("**Synthetic data is uniform.** Users, products and referrers are drawn uniformly at random, and the brief's volumes (500k orders for 10k users, about 50 orders per user in 90 days) make every customer a frequent buyer. Segment, cohort, retention and affinity differences are therefore small, and r = 100% caps the CLV multiplier. The methods are valid; the business conclusions describe this dataset, not a real shop. Future work: generate users with heterogeneous propensities (e.g. Pareto-distributed purchase rates, product popularity, seasonality) and re-run."),
  B("**Generator artefacts:** direct (non-session) orders are scheduled adaptively to reach the 500k target, which produces the end-of-window rise in Figure 1; stock is never replenished."),
  B("**HBase ran on an emulator** for the reported numbers (section 3.2); run `docker compose up` to reproduce on real HBase. Real counters (`INCR`) and the Spark HBase connector were not exercised."),
  B("**CLV model** is deliberately simple (historical margin × retention-discounted horizon). A BG/NBD + Gamma-Gamma model, and statistical significance tests for basket lift (e.g. a Poisson test with multiple-testing correction), are the next steps; streaming updates of `product_metrics` with Spark Structured Streaming would make the stock-out signal real-time."),

  H1("Appendix: reproducing the results"),
  P("`pip install -r requirements.txt`, `docker compose up -d` (MongoDB and HBase), then `./run_all.sh`. All result tables are in `outputs/results` (CSV), the MongoDB pipelines in `mongo_pipelines.json`, the Spark SQL in `sparksql_queries.sql`, and the figures in `outputs/figures`. The dataset is reproducible with the fixed seed and `--reference-date 2026-10-31T23:59:59`."),
);

const doc = new Document({
  creator: "Irumva",
  title: "Distributed Multi-Model Analytics for E-commerce Data",
  styles: {
    default: { document: { run: { font: FONT, size: 19 } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FONT, size: 26, bold: true, color: "0D366B" },
        paragraph: { spacing: { before: 220, after: 90 }, keepNext: true, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FONT, size: 21, bold: true, color: "184F95" },
        paragraph: { spacing: { before: 140, after: 60 }, keepNext: true, outlineLevel: 1 } },
    ],
  },
  numbering: { config: [
    { reference: "bullets", levels: [{ level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT,
      style: { paragraph: { indent: { left: 400, hanging: 240 } } } }] },
    { reference: "steps", levels: [{ level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT,
      style: { paragraph: { indent: { left: 400, hanging: 280 } } } }] },
  ] },
  sections: [{
    properties: { page: { size: { width: 11906, height: 16838 },
      margin: { top: 1134, bottom: 1134, left: 1134, right: 1134 } } },
    footers: { default: new Footer({ children: [new Paragraph({
      tabStops: [{ type: TabStopType.RIGHT, position: W }],
      children: [new TextRun({ text: "AUCA Big Data Analytics – Final Project", size: 16, color: "8A8984" }),
                 new TextRun({ children: ["\tPage ", PageNumber.CURRENT, " of ", PageNumber.TOTAL_PAGES], size: 16, color: "8A8984" })] })] }) },
    children: body,
  }],
});

Packer.toBuffer(doc).then((buf) => {
  fs.writeFileSync(path.join(__dirname, "Technical_Report.docx"), buf);
  console.log("wrote report/Technical_Report.docx");
});
