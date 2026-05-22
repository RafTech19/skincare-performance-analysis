# 🌿 Beautiverse Sales Performance Analysis
### *Turning Beauty Data Into Business Strategy*

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10-blue?style=for-the-badge&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/PySpark-ETL_Pipeline-orange?style=for-the-badge&logo=apachespark&logoColor=white"/>
  <img src="https://img.shields.io/badge/PostgreSQL-NeonDB-green?style=for-the-badge&logo=postgresql&logoColor=white"/>
  <img src="https://img.shields.io/badge/Pandas-Data_Analysis-150458?style=for-the-badge&logo=pandas&logoColor=white"/>
  <img src="https://img.shields.io/badge/Status-Completed-brightgreen?style=for-the-badge"/>
</p>

---

## 📖 Table of Contents

- [Project Overview](#-project-overview)
- [The Problem](#-the-problem)
- [Business Questions](#-business-questions)
- [Tech Stack & Tools](#-tech-stack--tools)
- [ETL Pipeline](#-etl-pipeline)
- [Database Schema](#-database-schema)
- [Key Findings](#-key-findings)
- [Conclusion](#-conclusion)
- [Strategic Recommendations](#-strategic-recommendations)
- [Dashboard](#-dashboard)
- [Project Structure](#-project-structure)
- [How to Run](#-how-to-run)

---

## 🌟 Project Overview

**Beautiverse** is a beauty and skincare company that sells across multiple countries, covering product categories including Body Care, Skincare, Makeup, Hair Care, and Home & Accessories.

This project analyzes **4 years of sales transaction data (2020–2023)** to uncover what's really driving profitability — and what's quietly eating into it. The goal is to provide clear, data-backed recommendations that help Beautiverse grow smarter in 2024 and beyond.

> **Think of this project as giving Beautiverse a full business health check-up — finding what's working, what's not, and exactly what to fix.**

| Metric | Value |
|--------|-------|
| 📅 Period Analyzed | 2020 – 2023 |
| 🌍 Markets Covered | USCA, Europe, Asia Pacific, LATAM, Africa |
| 🛍️ Total Products | 3,577 unique products |
| 👥 Total Customers | 51,290+ customers |
| 💰 Total Revenue | $1.06M+ |

---

## ❓ The Problem

Despite growing sales year-over-year, Beautiverse faced a **-12.16% profit decline in 2023** — even though revenue remained high. The company lacked a structured analysis to understand *why*, and where to focus their efforts.

This analysis was built to answer that — across 5 key business dimensions:

```
Sales Trend → Geography → Products → Customers → Discounts
```

---

## 💡 Business Questions

| # | Question | Why It Matters |
|---|----------|----------------|
| 1 | 📈 How has Beautiverse's sales and profit trended from 2020–2023, and are there consistent seasonal patterns? | Identifies when to push marketing hard — and when to protect margins |
| 2 | 🌍 Which regions generate the highest profit? | Guides budget allocation for sales and operations |
| 3 | 🧴 Which products and categories drive the most profit — and which cause losses? | Prioritizes the right products and eliminates dead weight |
| 4 | 👤 Which customer segments are the most valuable, and how do they behave? | Enables targeted marketing and reduces churn |
| 5 | 💸 Does a bigger discount always lead to higher profit? | Uncovers whether the discount strategy is helping or hurting |

---

## 🛠️ Tech Stack & Tools

### Languages & Libraries
| Tool | Purpose |
|------|---------|
| `Python 3.10` | Core programming language |
| `PySpark` | ETL pipeline for data ingestion & transformation |
| `Pandas` | Data manipulation & analysis |
| `Matplotlib & Seaborn` | Data visualization |
| `SciPy` | Statistical testing (normality, correlation, ANOVA) |
| `SQLAlchemy` | Database connection |

### Infrastructure
| Tool | Purpose |
|------|---------|
| `NeonDB (PostgreSQL)` | Cloud data warehouse — stores all dimension & fact tables |
| `Google Colab` | Development & analysis environment |
| `GitHub` | Version control & portfolio hosting |

---

## ⚙️ ETL Pipeline

> **ETL stands for Extract, Transform, Load** — it's the process of taking raw data, cleaning it up, and storing it in a structured database ready for analysis.

This project uses **PySpark** to handle the ETL process — a powerful tool designed to process large volumes of data efficiently.

```
RAW DATA FILES
      │
      ▼
┌─────────────────────────────────┐
│  EXTRACT                        │
│  • Read raw CSV/Excel files     │
│  • Load into PySpark DataFrame  │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│  TRANSFORM                      │
│  • Handle missing values        │
│  • Standardize date formats     │
│  • Normalize text columns       │
│  • Create dimension tables      │
│    (customer, product,          │
│     location, time)             │
│  • Build fact_sales table       │
│  • Calculate derived metrics    │
│    (profit margin, YoY growth)  │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│  LOAD                           │
│  • Connect to NeonDB            │
│  • Write tables via JDBC        │
│  • Validate row counts          │
└─────────────────────────────────┘
```

**Why PySpark?**
PySpark is used here to simulate a real-world data engineering workflow. Even though the dataset fits in memory, building the pipeline with PySpark demonstrates scalability — the same code can handle datasets 100x larger without modification.

---

## 🗄️ Database Schema

The database follows a **Star Schema** design — a standard in data warehousing where one central "fact" table connects to multiple "dimension" tables.

```
                    ┌─────────────────┐
                    │   dim_time      │
                    │─────────────────│
                    │ order_date (PK) │
                    │ year            │
                    │ month           │
                    │ quarter         │
                    │ start_of_month  │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│  dim_customer   │          │          │  dim_product    │
│─────────────────│          │          │─────────────────│
│ customer_id(PK) │          │          │ product_id (PK) │
│ customer_name   │          │          │ product_name    │
│ segment         │          │          │ category        │
│ country         │          │          │ sub_category    │
└────────┬────────┘          │          └────────┬────────┘
         │                   │                   │
         │         ┌─────────┴─────────┐         │
         └────────►│    fact_sales     │◄────────┘
                   │───────────────────│
                   │ order_id          │
                   │ customer_id (FK)  │
                   │ product_id (FK)   │
                   │ location_id (FK)  │
                   │ order_date (FK)   │
                   │ sales             │
                   │ quantity          │
                   │ discount          │
                   │ profit            │
                   └─────────┬─────────┘
                             │
                    ┌────────┴────────┐
                    │  dim_location   │
                    │─────────────────│
                    │ location_id(PK) │
                    │ country         │
                    │ market          │
                    │ region          │
                    └─────────────────┘
```

**Hosted on:** [NeonDB](https://neon.tech/) — a serverless PostgreSQL platform with auto-scaling.

---

## 🔍 Key Findings

### 1️⃣ Sales Trend & Seasonality

<details>
<summary><b>Click to expand findings</b></summary>

- Beautiverse showed **strong YoY profit growth from 2020–2022**, but experienced a **-12.16% decline in 2023** — primarily due to aggressive discounting on products with already-thin margins.
- A **consistent seasonal pattern** exists every year:
  - 📉 **Jan–Feb:** Lowest profit months
  - 📈 **Q3–Q4 (Sep–Dec):** Peak profit season
  - ⚠️ **July:** Sales spike, but profit *drops* — a sign of over-discounting
- Monthly profit is **highly volatile**, suggesting the business is too dependent on promotional events rather than organic demand.

</details>

---

### 2️⃣ Regional Performance

<details>
<summary><b>Click to expand findings</b></summary>

**By Country:**
| Country | Profit Level | Margin | Insight |
|---------|-------------|--------|---------|
| 🇺🇸 United States | Highest | Low (~9% in Makeup) | Volume-driven, not efficient |
| 🇳🇮 Nicaragua | Moderate | >29% | High efficiency, low volume |
| 🇨🇳 China | Moderate | >29% | High efficiency, low volume |
| 🇮🇳 India | Moderate | >29% | High efficiency, low volume |

**By Market:**
| Market | Total Profit | Margin | Status |
|--------|-------------|--------|--------|
| 🌍 Europe | $331.6K | 22.3% | ⭐ Best performer |
| 🌎 USCA | High | Lower | Volume-dependent |
| 🌏 Asia Pacific | Moderate | Lower | Growth potential |

> **ANOVA test confirmed** statistically significant profit differences across markets (p-value < 0.05), meaning each market needs its own tailored strategy.

</details>

---

### 3️⃣ Product & Category Performance

<details>
<summary><b>Click to expand findings</b></summary>

**Category Breakdown:**
| Category | Profit Contribution | Margin | Verdict |
|----------|-------------------|--------|---------|
| 🧴 Body Care | **94.24%** | Healthy | Core business driver |
| 🏠 Home & Accessories | <3% | Highest margin | Underexposed hidden gem |
| 💄 Makeup | <3% | 10.73% | Over-discounted, low demand |

**Pareto Analysis:**
> Only **29.75% of products (1,064 out of 3,577)** generate **80% of total profit**.

**Product Segmentation:**
| Segment | # Products | Total Profit | Action |
|---------|-----------|-------------|--------|
| ⭐ Star Product | 1,314 | ~$817K | Protect & scale |
| 💎 Hidden Gem | 475 | $182K | Boost exposure |
| ⚡ High Demand Low Profit | 553 | $34K | Fix pricing |
| 💀 Low Performer | 1,235 | Very low | Evaluate or drop |

</details>

---

### 4️⃣ Customer Segmentation (RFM Analysis)

<details>
<summary><b>Click to expand findings</b></summary>

> **RFM = Recency, Frequency, Monetary** — a proven method to score customers based on how recently they bought, how often, and how much they spent.

**Segment Distribution:**
| Tier | Segments | % of Customers | Avg. Spend |
|------|----------|----------------|-----------|
| 🏆 Premium | Champions + Loyal | 5.11% | $864–$941 |
| 🌱 Growing | Potential + Promising | 40.81% | Low–Medium |
| 😴 Inactive | At Risk + Cannot Lose + Hibernating + Lost | 50.3% | Previously high |

**Critical Funnel Drop:**
```
New Customers    →  2,846 people
Potential        →    658 people  (-77%)
Loyal            →    456 people  (-84%)
Champions        →    434 people  (-85%)
```
> Only **1 in 6 new customers** ever becomes loyal. This is a major retention problem.

</details>

---

### 5️⃣ Discount vs. Profit

<details>
<summary><b>Click to expand findings</b></summary>

**Spearman Correlation: rho = -0.6, p-value < 0.05**

> This means there's a **statistically significant negative relationship** between discount size and profit — bigger discounts = less profit.

| Discount Range | Profit Impact | Verdict |
|---------------|---------------|---------|
| 0% | Highest, consistent profit | ✅ Best |
| 0% – 17% | Positive, healthy margin | ✅ Sweet spot |
| 17% – 25% | Declining but still positive | ⚠️ Use selectively |
| 25% – 50% | Turns negative | ❌ Avoid |
| 50% – 85% | Severe losses | 🚫 Never |

**What-If Simulation (if discounts >25% are restricted):**
| Scenario | Margin Change | Order Change |
|----------|--------------|-------------|
| 30% retention | +6.0% | -10% |
| **50% retention** | **+4.6%** | **-6.55%** ← Optimal |
| 70% retention | +3.0% | -3.3% |

</details>

---

## 📌 Conclusion

Beautiverse is a fundamentally healthy business — but it's leaving money on the table in three critical areas:

1. **Over-discounting** is the #1 profit killer. The 2023 profit decline was largely self-inflicted through aggressive discounts that didn't generate enough volume to compensate.

2. **Portfolio concentration risk** is dangerously high. With 94% of profit coming from Body Care alone, any disruption to that category could be catastrophic.

3. **Customer retention is broken.** An 85% drop-off from new customers to champions means the company is constantly spending to acquire customers it can't keep.

The good news: all three are fixable with targeted, data-driven strategies.

---

## 🎯 Strategic Recommendations

### 📅 Seasonal Strategy
- **January–February:** Launch retention campaigns and light promotions to stimulate demand during the slow season
- **July:** Replace aggressive discounts with bundling and upselling to protect margins during the anomaly month
- **Q3–Q4:** Double down on high-margin products with full inventory support during peak season

### 🌍 Regional Strategy
- **USA:** Cap discounts, focus on customer retention — the volume is already there, the margin needs protecting
- **Europe:** Use as a benchmark and invest more in marketing and product availability — it's the most efficient market
- **High-margin, low-volume countries (Nicaragua, China, India):** Run targeted local campaigns to scale what's already working

### 🧴 Product Strategy
- **Prioritize the top 30% of products** that generate 80% of profit — protect their margins aggressively
- **Body Care:** Reduce discounting; maintain stock reliability
- **Home & Accessories:** Increase visibility — the margin is the highest, but nobody knows about it
- **Hidden Gems:** Bundle with Star Products to increase exposure
- **Low Performers:** Audit quarterly — improve, reduce, or discontinue

### 👤 Customer Strategy
- **"Cannot Lose Them" & "At Risk" segments:** These customers once spent significantly. Launch re-engagement campaigns (personalized offers, loyalty rewards) before they're gone for good
- **New Customers:** Build an onboarding journey to convert them into repeat buyers — the current funnel loses 77% at the first step
- **Champions & Loyal Customers:** Give them VIP treatment — early access, exclusive products, referral incentives

### 💸 Discount Strategy
- **Hard cap discounts at 25%** company-wide
- **Primary strategy:** 0–17% discount range
- **Selective use only:** 17–25% for specific strategic moments (end-of-season, new product launch)
- **Never use:** Discounts >25% — they consistently destroy profit
- **Apply discounts by segment**, not blanket promotions — high-value customers shouldn't need a discount to buy

---

## 📊 Dashboard

> 🔗 ****[View Interactive Dashboard](https://app.powerbi.com/groups/me/reports/84611003-7965-43d8-ba36-c29f7ce0b756/68e9adab6632c684e67f?experience=power-bi&bookmarkGuid=492e72245a095af67cbc)**** *(Power BI Dashboard)*

The dashboard covers:
- 📈 Monthly profit & sales trend (2020–2023)
- 🌍 Profit by country and market
- 🧴 Category and product performance breakdown
- 👤 RFM customer segment distribution
- 💸 Discount range vs. profit analysis

---

## 📁 Project Structure

```
beautiverse-sales-analysis/
│
├── 📓 notebooks/
│   └── skincare_sales_analysis.ipynb   # Main analysis notebook
│
├── ⚙️ etl/
│   └── pyspark_etl_pipeline.py         # PySpark ETL script
│
├── 🗄️ schema/
│   └── star_schema.sql                 # Database schema definition
│
├── 📊 dashboard/
│   └── beautiverse_dashboard.pdf       # Dashboard export
│
├── 📄 README.md                        # You are here
└── 📋 requirements.txt                 # Python dependencies
```

---

## 🚀 How to Run

### Prerequisites
```bash
pip install pandas numpy matplotlib seaborn scipy sqlalchemy pyspark
```

### 1. Clone the repository
```bash
git clone https://github.com/yourusername/beautiverse-sales-analysis.git
cd beautiverse-sales-analysis
```

### 2. Set up environment variables
Create a `.env` file in the root directory:
```env
DB_HOST=your_neondb_host
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=your_database
```

> ⚠️ **Never hardcode credentials in your notebook.** Always use environment variables.

### 3. Run the ETL pipeline
```bash
python etl/pyspark_etl_pipeline.py
```

### 4. Open the analysis notebook
```bash
jupyter notebook notebooks/skincare_sales_analysis.ipynb
```

---

## 👨‍💻 About the Author

**[Your Name]**
*Aspiring Data Analyst | Python · SQL · Data Visualization*

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?style=flat-square&logo=linkedin)](https://linkedin.com/in/muhammad-rafli-febriyanto-969984216/)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-black?style=flat-square&logo=github)](https://github.com/RafTech19)
[![Email](https://img.shields.io/badge/Email-Contact-red?style=flat-square&logo=gmail)](rafli.bim05@gmail.com)

---

<p align="center">
  <i>⭐ If you found this project useful or interesting, please give it a star!</i>
</p>
