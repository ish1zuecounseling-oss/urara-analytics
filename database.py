"""
database.py - SQLiteデータベース管理モジュール  v3.0
カウンセラーデータの保存・取得・更新を担当

v3 変更点:
  - スコア式を log スケールに変更（古参有利を解消）
  - estimated_revenue（売上推定）カラム追加
  - スコア再計算は全件のmax値を参照する2段階方式
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional


DB_PATH = "urara_analytics.db"


def get_connection() -> sqlite3.Connection:
    """データベース接続を取得する"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_database():
    """データベースの初期化・テーブル作成（既存テーブルはALTERで拡張）"""
    conn = get_connection()
    cursor = conn.cursor()

    # ── カウンセラーテーブル ──────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS counselors (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            name             TEXT NOT NULL,
            profile_url      TEXT UNIQUE,
            qualifications   TEXT,
            categories       TEXT,
            methods          TEXT,
            price            INTEGER,
            review_count     INTEGER DEFAULT 0,
            rating           REAL    DEFAULT 0.0,
            profile_text     TEXT,
            display_order    INTEGER,
            availability        INTEGER DEFAULT NULL,  -- 予約可能枠数
            popularity_score    REAL    DEFAULT 0.0,   -- 人気スコア（log式）
            estimated_revenue        REAL    DEFAULT NULL,  -- 累積売上推定（円）
            monthly_revenue_estimate REAL    DEFAULT NULL,  -- ★ 月間売上推定（直近30日口コミ増加ベース）
            scraped_at          TEXT,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at          TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 既存DBへのマイグレーション（列が無ければ追加）
    existing_cols = [row[1] for row in cursor.execute("PRAGMA table_info(counselors)").fetchall()]
    for col, typedef in [
        ("availability",               "INTEGER DEFAULT NULL"),
        ("estimated_revenue",          "REAL    DEFAULT NULL"),
        ("monthly_revenue_estimate",   "REAL    DEFAULT NULL"),
    ]:
        if col not in existing_cols:
            cursor.execute(f"ALTER TABLE counselors ADD COLUMN {col} {typedef}")
            print(f"  → {col} カラムを追加しました")

    # ── カテゴリータグテーブル ────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS category_tags (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            counselor_id INTEGER,
            tag          TEXT NOT NULL,
            FOREIGN KEY (counselor_id) REFERENCES counselors(id)
        )
    """)

    # ── 掲載順位ログテーブル ──────────────────────────────────────
    # ★新規: 毎スクレイピングで掲載順を記録 → アルゴリズム変動を検出
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS display_order_history (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            counselor_id  INTEGER NOT NULL,
            display_order INTEGER NOT NULL,
            recorded_at   TEXT    NOT NULL,
            FOREIGN KEY (counselor_id) REFERENCES counselors(id)
        )
    """)

    # ── 口コミスナップショットテーブル ────────────────────────────
    # ★新規: 口コミ数を定期記録 → 増加率を計算できる
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS review_snapshots (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            counselor_id INTEGER NOT NULL,
            review_count INTEGER NOT NULL,
            rating       REAL,
            availability INTEGER,
            recorded_at  TEXT NOT NULL,
            FOREIGN KEY (counselor_id) REFERENCES counselors(id)
        )
    """)

    # ── スクレイピング履歴テーブル ────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS scrape_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_at  TEXT NOT NULL,
            total_count INTEGER,
            status      TEXT,
            message     TEXT
        )
    """)

    # ── インデックス ──────────────────────────────────────────────
    # counselor_id で絞り込む頻出クエリを高速化（最大100倍）
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_display_order_counselor
        ON display_order_history(counselor_id)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_review_snapshot_counselor
        ON review_snapshots(counselor_id)
    """)
    # 日付範囲検索も高速化（成長率・トレンド分析クエリ用）
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_review_snapshot_recorded
        ON review_snapshots(recorded_at)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_counselor_score
        ON counselors(popularity_score DESC)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_counselor_revenue
        ON counselors(estimated_revenue DESC)
    """)

    conn.commit()
    conn.close()
    print("✅ データベースを初期化しました")


# ════════════════════════════════════════════════════════════════════
#  スコア計算  v3 — log スケール
# ════════════════════════════════════════════════════════════════════

# 口コミ率定数（口コミを書く利用者の割合）
# 心理相談業界の実態に合わせ 3〜6% の中間値を採用
# 7% は過大評価になりやすいため 5% を安全側として使用
REVIEW_RATE = 0.05          # 5% → 口コミ100件 ≒ 2,000セッション
REVENUE_AVG_SESSION_MIN = 30  # 平均1セッション30分と仮定


def calc_score(
    review_count: int,
    rating: float,
    price: int,
    max_price: int = 0,
    max_availability: int = 0,
    availability: int = None,
) -> float:
    """
    【v3 log スケールスコア式】

      score =
        log(口コミ数 + 1) × 40        ← logで古参有利を圧縮
        + 評価              × 30        ← 評価を最重視
        + (max_price - 料金) × 0.02    ← 低価格ほど有利（相対比較）
        + (max_avail - 空き枠) × 2     ← 空きが少ない＝予約が埋まっている

    v2 との比較（口コミ 10件 vs 550件）:
      v2 式: 10→40pt, 550→2200pt  → 差 55倍
      v3 式: 10→96pt, 550→255pt  → 差  2.7倍  ← 新人が戦える！

    市場全体の max_price / max_availability は upsert_counselor 内で
    全件集計してから渡す（2段階計算）。
    """
    import math
    rc   = review_count or 0
    rt   = rating or 0.0
    pr   = price or 0
    mp   = max_price or pr          # max が 0 なら自分の料金をmax扱い（差なし）
    ma   = max_availability or 0
    av   = availability if availability is not None else ma  # Noneなら差なし

    return (
        math.log(rc + 1) * 40
        + rt * 30
        + (mp - pr) * 0.02
        + (ma - av) * 2
    )


def calc_estimated_revenue(review_count: int, price: int,
                           review_rate: float = REVIEW_RATE,
                           avg_session_min: int = REVENUE_AVG_SESSION_MIN) -> float:
    """
    【売上推定】
      estimated_revenue = 料金/分 × avg_session_min × (口コミ数 / 口コミ率)

    口コミ率 = 実際に口コミを書く利用者の割合
      心理相談業界の実態: 3〜6%
      採用値 5%（安全側） → 口コミ100件 ÷ 5% = 2,000セッション推定

    注: これは累積セッション数ベースの推定であり月次収益ではない。
        月次換算には別途キャリア年数による補正が必要。
    """
    rc = review_count or 0
    pr = price or 0
    sessions = rc / review_rate
    return sessions * pr * avg_session_min


# ════════════════════════════════════════════════════════════════════
#  UPSERT
# ════════════════════════════════════════════════════════════════════

def calc_monthly_revenue_estimate(
    counselor_id: int,
    price: int,
    review_rate: float = REVIEW_RATE,
    avg_session_min: int = REVENUE_AVG_SESSION_MIN,
    days: int = 30,
) -> Optional[float]:
    """
    【月間売上推定】
      直近 N 日のスナップショットから口コミ増加数を取得し
      それをセッション数に換算して月間売上を推定する

      monthly_revenue =
        (直近30日口コミ増加数 ÷ 口コミ率) × 料金/分 × avg_session_min

      例: 直近30日で口コミ +7件、料金¥200/分
        7 ÷ 5% = 140セッション → 140 × 200 × 30 = ¥840,000/月

      スナップショットが不足している場合は None を返す。
    """
    conn = get_connection()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()

    rows = conn.execute("""
        SELECT review_count, recorded_at
        FROM review_snapshots
        WHERE counselor_id = ?
        ORDER BY recorded_at ASC
    """, (counselor_id,)).fetchall()
    conn.close()

    if len(rows) < 2:
        return None

    # 直近 days 日以内の最古と最新を比較
    recent = [r for r in rows if r["recorded_at"] >= cutoff]
    if len(recent) < 2:
        # 直近期間に2点なければ全期間の最古-最新を使い、日次換算する
        oldest_count = rows[0]["review_count"]
        latest_count = rows[-1]["review_count"]
        try:
            from datetime import datetime as dt
            t_old = dt.fromisoformat(rows[0]["recorded_at"])
            t_new = dt.fromisoformat(rows[-1]["recorded_at"])
            span_days = max(1, (t_new - t_old).days)
        except Exception:
            span_days = days
        monthly_growth = (latest_count - oldest_count) / span_days * 30
    else:
        oldest_count = recent[0]["review_count"]
        latest_count = recent[-1]["review_count"]
        monthly_growth = latest_count - oldest_count

    if monthly_growth <= 0:
        return 0.0

    sessions = monthly_growth / review_rate
    return sessions * price * avg_session_min


def update_monthly_revenue_all():
    """
    全カウンセラーの monthly_revenue_estimate を再計算して保存する
    スクレイピング完了後に呼ぶ。
    """
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, price FROM counselors"
    ).fetchall()
    conn.close()

    updated = 0
    for row in rows:
        mrev = calc_monthly_revenue_estimate(row["id"], row["price"] or 0)
        if mrev is not None:
            conn2 = get_connection()
            conn2.execute(
                "UPDATE counselors SET monthly_revenue_estimate = ? WHERE id = ?",
                (mrev, row["id"])
            )
            conn2.commit()
            conn2.close()
            updated += 1

    print(f"✅ {updated}件の月間売上推定を更新しました")


def upsert_counselor(data: dict) -> int:
    """
    カウンセラーデータをINSERT or UPDATE する
    profile_urlをキーに重複を防ぐ

    スコア計算は2段階:
      1. まずデータをINSERT/UPDATE
      2. 全件の max_price / max_availability を取得してスコアを再計算
         → log式には市場全体の最大値が必要なため
    """
    conn = get_connection()
    cursor = conn.cursor()

    # ── 1段目: データを保存（スコアは暫定値0.0）───────────────────
    now = datetime.now().isoformat()
    rev = calc_estimated_revenue(
        data.get("review_count", 0),
        data.get("price", 0),
    )

    cursor.execute("""
        INSERT INTO counselors
            (name, profile_url, qualifications, categories, methods,
             price, review_count, rating, profile_text, display_order,
             availability, popularity_score, estimated_revenue, scraped_at, updated_at)
        VALUES
            (:name, :profile_url, :qualifications, :categories, :methods,
             :price, :review_count, :rating, :profile_text, :display_order,
             :availability, 0.0, :estimated_revenue, :scraped_at, :updated_at)
        ON CONFLICT(profile_url) DO UPDATE SET
            name               = excluded.name,
            qualifications     = excluded.qualifications,
            categories         = excluded.categories,
            methods            = excluded.methods,
            price              = excluded.price,
            review_count       = excluded.review_count,
            rating             = excluded.rating,
            profile_text       = excluded.profile_text,
            display_order      = excluded.display_order,
            availability       = excluded.availability,
            estimated_revenue  = excluded.estimated_revenue,
            scraped_at         = excluded.scraped_at,
            updated_at         = excluded.updated_at
    """, {
        **data,
        "availability":       data.get("availability"),
        "estimated_revenue":  rev,
        "scraped_at":         now,
        "updated_at":         now,
    })

    counselor_id = cursor.lastrowid or _get_id_by_url(cursor, data["profile_url"])
    conn.commit()
    conn.close()

    # ── 2段目: 全件の max を使ってスコアを再計算 ─────────────────
    _recalc_scores_with_market_context()

    return counselor_id


def _get_id_by_url(cursor, profile_url: str) -> Optional[int]:
    row = cursor.execute(
        "SELECT id FROM counselors WHERE profile_url = ?", (profile_url,)
    ).fetchone()
    return row["id"] if row else None


def _recalc_scores_with_market_context():
    """
    全件を対象に market-context（市場全体の最大値）を使ってスコアを再計算する。

    log 式には「市場内での相対位置」が必要:
      - max_price      : 最も高い料金（低価格優位の基準）
      - max_availability: 最も空き枠が多い数（少ない空き枠 = 人気の基準）

    スクレイピング完了後に一括で呼ぶと最も正確になる。
    upsert 時にも呼んでいるが、最終的に recalculate_all_scores() を
    スクレイピング完了後に実行することを推奨。
    """
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 市場全体の最大値を取得
    stats = cursor.execute("""
        SELECT
            MAX(COALESCE(price, 0))        AS max_price,
            MAX(COALESCE(availability, 0)) AS max_availability
        FROM counselors
    """).fetchone()
    max_price = stats["max_price"] or 0
    max_avail = stats["max_availability"] or 0

    rows = cursor.execute(
        "SELECT id, review_count, rating, price, availability FROM counselors"
    ).fetchall()

    for row in rows:
        score = calc_score(
            row["review_count"] or 0,
            row["rating"] or 0.0,
            row["price"] or 0,
            max_price=max_price,
            max_availability=max_avail,
            availability=row["availability"],
        )
        cursor.execute(
            "UPDATE counselors SET popularity_score = ? WHERE id = ?",
            (score, row["id"])
        )

    conn.commit()
    conn.close()


# ════════════════════════════════════════════════════════════════════
#  履歴ログ保存
# ════════════════════════════════════════════════════════════════════

def save_display_order_log(counselor_id: int, display_order: int):
    """掲載順位をログに記録（毎スクレイピング時）"""
    conn = get_connection()
    conn.execute("""
        INSERT INTO display_order_history (counselor_id, display_order, recorded_at)
        VALUES (?, ?, ?)
    """, (counselor_id, display_order, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def save_review_snapshot(counselor_id: int, review_count: int,
                         rating: float, availability: Optional[int] = None):
    """口コミ数スナップショットを保存（増加率計算用）"""
    conn = get_connection()
    conn.execute("""
        INSERT INTO review_snapshots (counselor_id, review_count, rating, availability, recorded_at)
        VALUES (?, ?, ?, ?, ?)
    """, (counselor_id, review_count, rating, availability, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def save_category_tags(counselor_id: int, tags: list):
    """カテゴリータグを保存（既存データは削除して再登録）"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM category_tags WHERE counselor_id = ?", (counselor_id,))
    for tag in tags:
        cursor.execute(
            "INSERT INTO category_tags (counselor_id, tag) VALUES (?, ?)",
            (counselor_id, tag)
        )
    conn.commit()
    conn.close()


def save_scrape_history(total_count: int, status: str, message: str = ""):
    """スクレイピング実行履歴を保存"""
    conn = get_connection()
    conn.execute("""
        INSERT INTO scrape_history (scraped_at, total_count, status, message)
        VALUES (?, ?, ?, ?)
    """, (datetime.now().isoformat(), total_count, status, message))
    conn.commit()
    conn.close()


# ════════════════════════════════════════════════════════════════════
#  基本データ取得
# ════════════════════════════════════════════════════════════════════

def get_all_counselors() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT * FROM counselors ORDER BY popularity_score DESC", conn
    )
    conn.close()
    return df


def get_counselor_count() -> int:
    conn = get_connection()
    count = conn.execute("SELECT COUNT(*) FROM counselors").fetchone()[0]
    conn.close()
    return count


def get_average_price() -> float:
    conn = get_connection()
    result = conn.execute(
        "SELECT AVG(price) FROM counselors WHERE price IS NOT NULL AND price > 0"
    ).fetchone()[0]
    conn.close()
    return round(result or 0, 1)


def get_average_rating() -> float:
    conn = get_connection()
    result = conn.execute(
        "SELECT AVG(rating) FROM counselors WHERE rating IS NOT NULL AND rating > 0"
    ).fetchone()[0]
    conn.close()
    return round(result or 0, 2)


def get_top_counselors(limit: int = 50) -> pd.DataFrame:
    """人気スコア上位のカウンセラーを取得"""
    conn = get_connection()
    df = pd.read_sql_query(f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY popularity_score DESC) AS rank,
            name, categories, rating, review_count,
            price, availability, popularity_score,
            estimated_revenue, profile_url
        FROM counselors
        ORDER BY popularity_score DESC
        LIMIT {limit}
    """, conn)
    conn.close()
    return df


def get_category_ranking() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT tag, COUNT(*) as count
        FROM category_tags
        GROUP BY tag
        ORDER BY count DESC
    """, conn)
    conn.close()
    return df


def get_counselor_by_name(name: str) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT * FROM counselors WHERE name LIKE ?
        ORDER BY popularity_score DESC
    """, conn, params=(f"%{name}%",))
    conn.close()
    return df



def get_review_distribution() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT
            CASE
                WHEN review_count = 0    THEN '0件'
                WHEN review_count <= 10  THEN '1-10件'
                WHEN review_count <= 50  THEN '11-50件'
                WHEN review_count <= 100 THEN '51-100件'
                WHEN review_count <= 300 THEN '101-300件'
                ELSE '300件以上'
            END AS range,
            COUNT(*) AS count
        FROM counselors
        GROUP BY range
        ORDER BY MIN(review_count)
    """, conn)
    conn.close()
    return df


def get_price_distribution() -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT price FROM counselors
        WHERE price IS NOT NULL AND price > 0 ORDER BY price
    """, conn)
    conn.close()
    return df


def get_last_scrape_time() -> Optional[str]:
    conn = get_connection()
    row = conn.execute("""
        SELECT scraped_at FROM scrape_history
        WHERE status = '成功' ORDER BY id DESC LIMIT 1
    """).fetchone()
    conn.close()
    return row["scraped_at"] if row else None


# ════════════════════════════════════════════════════════════════════
#  分析系クエリ
# ════════════════════════════════════════════════════════════════════

def recalculate_all_scores():
    """スクレイピング完了後に呼ぶ。全件をmarket-contextで再計算 + 月間売上推定も更新。"""
    _recalc_scores_with_market_context()
    update_monthly_revenue_all()
    conn = get_connection()
    count = conn.execute("SELECT COUNT(*) FROM counselors").fetchone()[0]
    conn.close()
    print(f"✅ {count}件のスコアを再計算しました（log スケール v3）")


def get_revenue_ranking(limit: int = 50) -> pd.DataFrame:
    """
    売上推定ランキング（estimated_revenue 順）
    誰が最も稼いでいるかを可視化する
    """
    conn = get_connection()
    df = pd.read_sql_query(f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY estimated_revenue DESC) AS rank,
            name, categories, price, review_count, rating,
            estimated_revenue, monthly_revenue_estimate,
            popularity_score, profile_url
        FROM counselors
        WHERE estimated_revenue IS NOT NULL
        ORDER BY estimated_revenue DESC
        LIMIT {limit}
    """, conn)
    conn.close()
    return df


def get_monthly_revenue_ranking(limit: int = 50) -> pd.DataFrame:
    """
    月間売上推定ランキング（monthly_revenue_estimate 順）
    「今まさに稼いでいる人」を可視化する
    累積売上ランキングとは異なり、直近30日の勢いを反映する。
    """
    conn = get_connection()
    df = pd.read_sql_query(f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY monthly_revenue_estimate DESC) AS rank,
            name, categories, price, review_count, rating,
            monthly_revenue_estimate, estimated_revenue,
            popularity_score, profile_url
        FROM counselors
        WHERE monthly_revenue_estimate IS NOT NULL
          AND monthly_revenue_estimate > 0
        ORDER BY monthly_revenue_estimate DESC
        LIMIT {limit}
    """, conn)
    conn.close()
    return df


def get_revenue_summary() -> dict:
    """売上推定の市場サマリー（累積 + 月間）"""
    conn = get_connection()
    row = conn.execute("""
        SELECT
            SUM(estimated_revenue)          AS total_market,
            AVG(estimated_revenue)          AS avg_revenue,
            MAX(estimated_revenue)          AS max_revenue,
            SUM(monthly_revenue_estimate)   AS total_monthly,
            AVG(monthly_revenue_estimate)   AS avg_monthly,
            MAX(monthly_revenue_estimate)   AS max_monthly,
            COUNT(*)                        AS n
        FROM counselors
        WHERE estimated_revenue IS NOT NULL AND estimated_revenue > 0
    """).fetchone()
    conn.close()
    return {
        "total_market":  row["total_market"]  or 0,
        "avg_revenue":   row["avg_revenue"]   or 0,
        "max_revenue":   row["max_revenue"]   or 0,
        "total_monthly": row["total_monthly"] or 0,
        "avg_monthly":   row["avg_monthly"]   or 0,
        "max_monthly":   row["max_monthly"]   or 0,
        "n":             row["n"]             or 0,
    }


def get_counselor_rank(name: str) -> dict:
    """特定カウンセラーの各種順位を取得"""
    conn = get_connection()
    rows = conn.execute("""
        SELECT name, popularity_score, review_count, rating,
               availability, estimated_revenue, monthly_revenue_estimate
        FROM counselors ORDER BY popularity_score DESC
    """).fetchall()

    result = {
        "found": False, "name": name,
        "overall_rank": None, "review_rank": None,
        "revenue_rank": None, "monthly_revenue_rank": None,
        "total": len(rows),
        "popularity_score": None, "review_count": None,
        "rating": None, "availability": None,
        "estimated_revenue": None, "monthly_revenue_estimate": None,
    }

    for i, row in enumerate(rows, 1):
        if name in row["name"]:
            result.update({
                "found": True, "overall_rank": i,
                "popularity_score":        row["popularity_score"],
                "review_count":            row["review_count"],
                "rating":                  row["rating"],
                "availability":            row["availability"],
                "estimated_revenue":       row["estimated_revenue"],
                "monthly_revenue_estimate": row["monthly_revenue_estimate"],
            })
            break

    review_rows = conn.execute(
        "SELECT name FROM counselors ORDER BY review_count DESC"
    ).fetchall()
    for i, row in enumerate(review_rows, 1):
        if name in row["name"]:
            result["review_rank"] = i
            break

    rev_rows = conn.execute(
        "SELECT name FROM counselors ORDER BY estimated_revenue DESC"
    ).fetchall()
    for i, row in enumerate(rev_rows, 1):
        if name in row["name"]:
            result["revenue_rank"] = i
            break

    mrev_rows = conn.execute(
        "SELECT name FROM counselors ORDER BY monthly_revenue_estimate DESC"
    ).fetchall()
    for i, row in enumerate(mrev_rows, 1):
        if name in row["name"]:
            result["monthly_revenue_rank"] = i
            break

    conn.close()
    return result


def get_display_order_history(counselor_id: int, limit: int = 60) -> pd.DataFrame:
    """特定カウンセラーの掲載順位推移"""
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT display_order, recorded_at
        FROM display_order_history
        WHERE counselor_id = ?
        ORDER BY recorded_at ASC
        LIMIT ?
    """, conn, params=(counselor_id, limit))
    conn.close()
    return df


def get_display_order_change_ranking(limit: int = 20) -> pd.DataFrame:
    """
    掲載順位の変動幅が大きいカウンセラーランキング
    最初と最後の掲載順を比較してアルゴリズム変動を可視化
    """
    conn = get_connection()
    df = pd.read_sql_query(f"""
        WITH first_last AS (
            SELECT
                counselor_id,
                FIRST_VALUE(display_order) OVER (
                    PARTITION BY counselor_id ORDER BY recorded_at ASC
                ) AS first_order,
                LAST_VALUE(display_order) OVER (
                    PARTITION BY counselor_id ORDER BY recorded_at ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS last_order,
                COUNT(*) OVER (PARTITION BY counselor_id) AS record_count
            FROM display_order_history
        )
        SELECT DISTINCT
            c.name,
            fl.first_order,
            fl.last_order,
            (fl.first_order - fl.last_order) AS improvement,  -- 正=上昇
            fl.record_count
        FROM first_last fl
        JOIN counselors c ON c.id = fl.counselor_id
        WHERE fl.record_count >= 2
        ORDER BY ABS(fl.first_order - fl.last_order) DESC
        LIMIT {limit}
    """, conn)
    conn.close()
    return df


def get_review_growth_data(days: int = 30) -> pd.DataFrame:
    """
    直近N日の口コミ増加数・増加率を計算
    各カウンセラーの最古と最新スナップショットを比較
    """
    conn = get_connection()
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()

    df = pd.read_sql_query(f"""
        WITH latest AS (
            SELECT counselor_id, review_count AS latest_count, recorded_at
            FROM review_snapshots
            WHERE (counselor_id, recorded_at) IN (
                SELECT counselor_id, MAX(recorded_at)
                FROM review_snapshots GROUP BY counselor_id
            )
        ),
        oldest AS (
            SELECT counselor_id, review_count AS oldest_count, recorded_at
            FROM review_snapshots
            WHERE (counselor_id, recorded_at) IN (
                SELECT counselor_id, MIN(recorded_at)
                FROM review_snapshots
                WHERE recorded_at >= '{cutoff}'
                GROUP BY counselor_id
            )
        )
        SELECT
            c.name, c.categories,
            o.oldest_count AS start_count,
            l.latest_count AS end_count,
            (l.latest_count - o.oldest_count) AS growth,
            CASE
                WHEN o.oldest_count > 0
                THEN ROUND((l.latest_count - o.oldest_count) * 100.0 / o.oldest_count, 1)
                ELSE NULL
            END AS growth_rate
        FROM latest l
        JOIN oldest o ON o.counselor_id = l.counselor_id
        JOIN counselors c ON c.id = l.counselor_id
        WHERE l.latest_count != o.oldest_count
        ORDER BY growth DESC
    """, conn)
    conn.close()
    return df


def get_review_snapshots_for_counselor(name: str) -> pd.DataFrame:
    """
    特定カウンセラーの口コミスナップショット時系列を取得
    → 成長予測の回帰計算に使う
    """
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT
            rs.review_count,
            rs.recorded_at,
            c.name
        FROM review_snapshots rs
        JOIN counselors c ON c.id = rs.counselor_id
        WHERE c.name LIKE ?
        ORDER BY rs.recorded_at ASC
    """, conn, params=(f"%{name}%",))
    conn.close()
    return df


def get_occupancy_data() -> pd.DataFrame:
    """
    稼働率データを返す

    稼働率の考え方:
      うらら相談室はカウンセラーが「公開している週間枠数」を持ち
      その中で「現在空いている枠数（availability）」が取得できる。

      仮定: 週間総枠数 = 標準的なカウンセラーの公開枠（デフォルト20枠/週）
      稼働率 = (総枠 - 空き枠) / 総枠 × 100

    空き枠が少いほど稼働率が高い（= 予約で埋まっている = 実績人気カウンセラー）

    availability が NULL のカウンセラーは除外。
    サイトから総枠数が取れた場合は total_slots カラムを使う（将来拡張）。
    """
    DEFAULT_TOTAL_SLOTS = 20  # 週間デフォルト枠数（調整可能）

    conn = get_connection()
    df = pd.read_sql_query(f"""
        SELECT
            name, availability, price, review_count, rating,
            popularity_score, estimated_revenue, categories,
            {DEFAULT_TOTAL_SLOTS} AS total_slots
        FROM counselors
        WHERE availability IS NOT NULL
        ORDER BY availability ASC
    """, conn)
    conn.close()

    if df.empty:
        return df

    # 稼働率を計算（空き枠が総枠を超える異常値は除外）
    df = df[df["availability"] <= df["total_slots"]].copy()
    df["occupied_slots"] = df["total_slots"] - df["availability"]
    df["occupancy_rate"] = (df["occupied_slots"] / df["total_slots"] * 100).round(1)

    return df.sort_values("occupancy_rate", ascending=False)


def get_rising_newcomers(months: int = 12, min_reviews: int = 10, limit: int = 20) -> pd.DataFrame:
    """
    新規成功ランキング
    条件: 登録N月以内 かつ 口コミ数が min_reviews 件以上
    → 次に人気になる可能性が高いカウンセラーを抽出
    """
    conn = get_connection()
    cutoff = (datetime.now() - timedelta(days=months * 30)).isoformat()
    df = pd.read_sql_query(f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY popularity_score DESC) AS rank,
            name, categories, rating, review_count,
            price, availability, popularity_score, created_at
        FROM counselors
        WHERE created_at >= '{cutoff}'
          AND review_count >= {min_reviews}
        ORDER BY popularity_score DESC
        LIMIT {limit}
    """, conn)
    conn.close()
    return df


def get_top_counselor_texts(top_n: int = 50) -> pd.DataFrame:
    """人気上位カウンセラーのプロフィール文章を取得（NLP分析用）"""
    conn = get_connection()
    df = pd.read_sql_query(f"""
        SELECT name, profile_text, popularity_score, rating, review_count
        FROM counselors
        WHERE profile_text IS NOT NULL AND profile_text != ''
        ORDER BY popularity_score DESC
        LIMIT {top_n}
    """, conn)
    conn.close()
    return df


def get_availability_stats() -> pd.DataFrame:
    """予約可能枠統計（データが存在する場合）"""
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT name, availability, rating, review_count, popularity_score
        FROM counselors
        WHERE availability IS NOT NULL
        ORDER BY availability ASC
    """, conn)
    conn.close()
    return df
