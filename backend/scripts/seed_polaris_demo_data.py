from __future__ import annotations

import random
from collections import defaultdict
from datetime import datetime, timedelta

import pymysql

HOST = '127.0.0.1'
PORT = 3306
USER = 'root'
PASSWORD = '123456'
DATABASE = 'polaris'
DEMO_USER_PREFIX = 'demo_user_'
DEMO_EMAIL_DOMAIN = 'demo-polaris.local'
RANDOM_SEED = 20260407

USER_BIOS = [
    '?? AI ????????????',
    '????????????????',
    '?????????????',
    '???????????????',
    '?????????????????',
]

TOOL_TAGS = [
    ('ai-writing', 'AI??', '#2563eb', 'edit_note'),
    ('image-gen', '????', '#7c3aed', 'imagesmode'),
    ('productivity', '????', '#059669', 'bolt'),
    ('document', '????', '#ea580c', 'description'),
    ('developer', '???', '#0f172a', 'code'),
    ('research', '????', '#dc2626', 'travel_explore'),
    ('translation', '?????', '#0891b2', 'translate'),
    ('marketing', '????', '#db2777', 'campaign'),
    ('automation', '???', '#4f46e5', 'smart_toy'),
    ('analysis', '????', '#16a34a', 'analytics'),
]

NOTIFICATION_TYPES = ['system', 'subscription', 'tool_update', 'comment_reply']
PAYMENT_METHODS = ['alipay', 'wechat', 'stripe']
AD_POSITIONS = ['home-top', 'home-side', 'tool-detail', 'dashboard-banner']
USER_AGENTS = [
    'Mozilla/5.0 Chrome/124.0 Windows NT 10.0',
    'Mozilla/5.0 Safari/17.4 Mac OS X 14_4',
    'Mozilla/5.0 Mobile Safari/17.0 iPhone',
]
PAGE_URLS = ['/home', '/tools', '/dashboard', '/category/ai', '/category/productivity']
REFERRERS = ['/home', '/search?q=ai', '/category/dev', '/favorites']


def chunked(items: list[tuple], size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def random_dt(start: datetime, end: datetime) -> datetime:
    delta = end - start
    seconds = int(delta.total_seconds())
    return start + timedelta(seconds=random.randint(0, max(seconds, 1)))


def main() -> None:
    random.seed(RANDOM_SEED)
    now = datetime.now().replace(microsecond=0)
    start_180 = now - timedelta(days=180)
    start_60 = now - timedelta(days=60)
    start_30 = now - timedelta(days=30)
    start_14 = now - timedelta(days=14)

    conn = pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        charset='utf8mb4',
        autocommit=False,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT password FROM t_user ORDER BY id LIMIT 1")
            row = cur.fetchone()
            password_hash = row[0] if row and row[0] else '$2a$10$wQx6uBfeM1bJ9F9A8k1bzeQ1v0zQwN1jV7vVQJQ7f5xU1jv1L9Y6m'

            cur.execute("SELECT id FROM t_user WHERE username LIKE %s", (f'{DEMO_USER_PREFIX}%',))
            demo_user_ids = [item[0] for item in cur.fetchall()]
            if demo_user_ids:
                id_list = ','.join(str(int(i)) for i in demo_user_ids)
                cur.execute(f"DELETE FROM t_tool_review WHERE user_id IN ({id_list})")
                cur.execute(f"DELETE FROM t_user_favorite WHERE user_id IN ({id_list})")
                cur.execute(f"DELETE FROM t_notification WHERE user_id IN ({id_list})")
                cur.execute(f"DELETE FROM t_user_subscription WHERE user_id IN ({id_list})")
                cur.execute(f"DELETE FROM t_tool_usage WHERE user_id IN ({id_list})")
                cur.execute(f"DELETE FROM t_ad_click WHERE user_id IN ({id_list})")
                cur.execute(f"DELETE FROM t_ad_impression WHERE user_id IN ({id_list})")
                cur.execute(f"DELETE FROM t_user WHERE id IN ({id_list})")

            cur.execute("DELETE FROM t_tool_tag")
            cur.execute("DELETE FROM t_tag WHERE name LIKE 'demo-%' OR name IN %s", ((tuple(item[0] for item in TOOL_TAGS))),)
        conn.rollback()
    except Exception:
        conn.rollback()

    with conn.cursor() as cur:
        cur.execute("DELETE FROM t_tool_tag")
        cur.execute("DELETE FROM t_tag")
        cur.execute("DELETE FROM t_tool_review")
        cur.execute("DELETE FROM t_user_favorite WHERE id >= 100000")
        cur.execute("DELETE FROM t_notification WHERE id >= 100000")
        cur.execute("DELETE FROM t_user_subscription")
        cur.execute("DELETE FROM t_ad_click")
        cur.execute("DELETE FROM t_ad_impression")
        cur.execute("DELETE FROM t_user WHERE username LIKE %s", (f'{DEMO_USER_PREFIX}%',))

        cur.execute("SELECT id, name_zh, name FROM t_category WHERE deleted = 0 AND status = 1 ORDER BY id")
        categories = cur.fetchall()
        if not categories:
            raise RuntimeError('t_category is empty')

        cur.execute("SELECT id, category_id, COALESCE(name_zh, name), name FROM t_tool WHERE deleted = 0 AND status = 1 ORDER BY id")
        tools = cur.fetchall()
        if not tools:
            raise RuntimeError('t_tool is empty')

        cur.execute("SELECT id, title FROM t_advertisement ORDER BY id")
        ads = cur.fetchall()
        if not ads:
            ad_rows = []
            for idx in range(1, 9):
                start_date = now - timedelta(days=random.randint(10, 90))
                end_date = now + timedelta(days=random.randint(20, 120))
                ad_rows.append((
                    f'????? {idx}',
                    f'?? AI ????????????? {idx}',
                    f'https://cdn.example.com/ad-{idx}.png',
                    f'https://sponsor.example.com/campaign/{idx}',
                    random.randint(0, 3),
                    random.choice(AD_POSITIONS),
                    random.randint(1, 10),
                    start_date,
                    end_date,
                    round(random.uniform(50, 300), 2),
                    round(random.uniform(500, 3000), 2),
                    round(random.uniform(0.2, 2.5), 2),
                    0,
                    0,
                    1,
                ))
            cur.executemany(
                """
                INSERT INTO t_advertisement
                (title, description, image_url, target_url, ad_type, position, priority, start_date, end_date, daily_budget, total_budget, cost_per_click, impression_count, click_count, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                ad_rows,
            )
            cur.execute("SELECT id, title FROM t_advertisement ORDER BY id")
            ads = cur.fetchall()

        user_rows = []
        total_users = 180
        for idx in range(1, total_users + 1):
            created_at = random_dt(start_180, now - timedelta(days=1))
            last_login = random_dt(max(created_at, start_60), now)
            email_verified = 1 if random.random() < 0.92 else 0
            phone_verified = 1 if random.random() < 0.68 else 0
            plan_type = random.choices([0, 1, 2], weights=[70, 24, 6], k=1)[0]
            user_rows.append((
                f'{DEMO_USER_PREFIX}{idx:03d}',
                password_hash,
                created_at,
                f'{DEMO_USER_PREFIX}{idx:03d}@{DEMO_EMAIL_DOMAIN}',
                f'????{idx:03d}',
                f'https://api.dicebear.com/7.x/identicon/svg?seed={idx}',
                None,
                random.choice(USER_BIOS),
                random.choice(['zh-CN', 'en-US']),
                plan_type,
                now + timedelta(days=random.randint(15, 365)) if plan_type else None,
                1,
                last_login,
                f'10.0.{random.randint(1, 20)}.{random.randint(2, 250)}',
                created_at,
                created_at,
                0,
                email_verified,
                last_login if email_verified else None,
                f'138{random.randint(10000000, 99999999)}',
                phone_verified,
                last_login if phone_verified else None,
                random.choice([1, 1, 1, 2]),
            ))
        cur.executemany(
            """
            INSERT INTO t_user
            (username,password,password_updated_at,email,nickname,avatar,avatar_config,bio,language,plan_type,plan_expired_at,status,last_login_at,last_login_ip,created_at,updated_at,deleted,email_verified,email_verified_at,phone,phone_verified,phone_verified_at,register_type)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            user_rows,
        )
        cur.execute("SELECT id, username, created_at, last_login_at, plan_type FROM t_user WHERE username LIKE %s ORDER BY id", (f'{DEMO_USER_PREFIX}%',))
        demo_users = cur.fetchall()
        user_ids = [row[0] for row in demo_users]

        tag_rows = [(name, name_zh, color, icon, 0, idx, 1) for idx, (name, name_zh, color, icon) in enumerate(TOOL_TAGS, start=1)]
        cur.executemany(
            "INSERT INTO t_tag (name,name_zh,color,icon,tool_count,sort_order,status) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            tag_rows,
        )
        cur.execute("SELECT id, name FROM t_tag ORDER BY id")
        tags = cur.fetchall()
        tag_map = {name: tag_id for tag_id, name in tags}

        tool_tag_rows = []
        category_to_tag_names = defaultdict(list)
        for cat_id, cat_name_zh, cat_name in categories:
            key = (cat_name_zh or cat_name or '').lower()
            if '??' in key or '??' in key:
                category_to_tag_names[cat_id] = ['ai-writing', 'document', 'productivity']
            elif '??' in key or '??' in key:
                category_to_tag_names[cat_id] = ['image-gen', 'productivity']
            elif '??' in key or '??' in key:
                category_to_tag_names[cat_id] = ['developer', 'automation', 'analysis']
            elif '??' in key or '??' in key:
                category_to_tag_names[cat_id] = ['marketing', 'analysis']
            else:
                category_to_tag_names[cat_id] = ['productivity', 'automation']
        for tool_id, category_id, _, _ in tools:
            names = set(category_to_tag_names.get(category_id, ['productivity']))
            while len(names) < 3:
                names.add(random.choice([item[0] for item in TOOL_TAGS]))
            for tag_name in sorted(names)[:3]:
                tool_tag_rows.append((tool_id, tag_map[tag_name]))
        cur.executemany(
            "INSERT INTO t_tool_tag (tool_id, tag_id) VALUES (%s,%s)",
            tool_tag_rows,
        )
        cur.execute(
            "UPDATE t_tag t SET tool_count = (SELECT COUNT(*) FROM t_tool_tag tt WHERE tt.tag_id = t.id)"
        )

        favorite_rows = []
        favorite_seen = set()
        for user_id, _, created_at, *_ in demo_users:
            fav_count = random.randint(2, 8)
            for tool_id, *_ in random.sample(tools, min(fav_count, len(tools))):
                key = (user_id, tool_id)
                if key in favorite_seen:
                    continue
                favorite_seen.add(key)
                fav_time = random_dt(max(created_at, start_60), now)
                favorite_rows.append((user_id, tool_id, fav_time, fav_time, 0))
        for batch in chunked(favorite_rows, 500):
            cur.executemany(
                "INSERT INTO t_user_favorite (user_id, tool_id, created_at, updated_at, deleted) VALUES (%s,%s,%s,%s,%s)",
                batch,
            )

        usage_rows = []
        tool_usage_count = defaultdict(int)
        tool_view_count = defaultdict(int)
        for user_id, _, created_at, last_login_at, _ in demo_users:
            event_count = random.randint(18, 65)
            for _ in range(event_count):
                tool_id, *_ = random.choice(tools)
                used_at = random_dt(max(created_at, start_180), now)
                duration = random.randint(20, 2400)
                usage_rows.append((
                    user_id,
                    tool_id,
                    used_at,
                    duration,
                    f'10.1.{random.randint(1, 40)}.{random.randint(2, 250)}',
                    random.choice(USER_AGENTS),
                    used_at,
                    used_at,
                    0,
                ))
                tool_usage_count[tool_id] += 1
                tool_view_count[tool_id] += random.randint(1, 4)
            if last_login_at:
                for _ in range(random.randint(1, 4)):
                    tool_id, *_ = random.choice(tools)
                    used_at = random_dt(max(last_login_at - timedelta(days=7), start_14), now)
                    usage_rows.append((
                        user_id,
                        tool_id,
                        used_at,
                        random.randint(60, 1800),
                        f'10.2.{random.randint(1, 40)}.{random.randint(2, 250)}',
                        random.choice(USER_AGENTS),
                        used_at,
                        used_at,
                        0,
                    ))
                    tool_usage_count[tool_id] += 1
                    tool_view_count[tool_id] += random.randint(2, 6)
        for batch in chunked(usage_rows, 1000):
            cur.executemany(
                """
                INSERT INTO t_tool_usage (user_id, tool_id, used_at, duration, ip_address, user_agent, created_at, updated_at, deleted)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                batch,
            )

        review_rows = []
        tool_rating_total = defaultdict(int)
        tool_rating_count = defaultdict(int)
        tool_review_count = defaultdict(int)
        for user_id, username, created_at, *_ in demo_users:
            review_count = random.randint(1, 4)
            for tool_id, _, tool_name_zh, tool_name in random.sample(tools, min(review_count, len(tools))):
                rating = random.choices([3, 4, 5], weights=[15, 45, 40], k=1)[0]
                review_time = random_dt(max(created_at, start_60), now)
                review_rows.append((
                    tool_id,
                    user_id,
                    rating,
                    f'{tool_name_zh or tool_name} ????',
                    f'{username} ?? {tool_name_zh or tool_name} ?????????????????????',
                    random.randint(0, 30),
                    1,
                    review_time,
                    review_time,
                ))
                tool_rating_total[tool_id] += rating
                tool_rating_count[tool_id] += 1
                tool_review_count[tool_id] += 1
        for batch in chunked(review_rows, 500):
            cur.executemany(
                "INSERT INTO t_tool_review (tool_id,user_id,rating,title,content,helpful_count,status,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                batch,
            )

        sub_rows = []
        payment_seq = 100000
        for user_id, _, created_at, _, plan_type in demo_users:
            if plan_type == 0 and random.random() < 0.82:
                continue
            actual_plan = plan_type if plan_type in (1, 2) else random.choice([1, 2])
            start_date = random_dt(max(created_at, start_180), now - timedelta(days=3))
            duration_days = 30 if actual_plan == 1 else 365
            end_date = start_date + timedelta(days=duration_days)
            status = 1 if end_date >= now else 2
            amount = 39.9 if actual_plan == 1 else 399.0
            payment_seq += 1
            sub_rows.append((
                user_id,
                actual_plan,
                amount,
                random.choice(PAYMENT_METHODS),
                f'PAY{payment_seq}',
                start_date,
                end_date,
                status,
                start_date,
                start_date,
            ))
        for batch in chunked(sub_rows, 500):
            cur.executemany(
                "INSERT INTO t_user_subscription (user_id,plan_type,amount,payment_method,payment_id,start_date,end_date,status,created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                batch,
            )

        notification_rows = []
        for user_id, _, created_at, *_ in demo_users:
            n_count = random.randint(4, 12)
            for i in range(n_count):
                created = random_dt(max(created_at, start_30), now)
                is_read = 1 if random.random() < 0.7 else 0
                notification_rows.append((
                    user_id,
                    0,
                    None,
                    random.choice(NOTIFICATION_TYPES),
                    random.choice(['??????', '??????', '??????', '????']),
                    random.choice(['??????????????', '?????????????????', '???????????', '????????????']),
                    random.choice(['/tools', '/subscription', '/notifications', '/dashboard']),
                    is_read,
                    created,
                    created,
                    created if is_read else None,
                    0,
                ))
        for batch in chunked(notification_rows, 1000):
            cur.executemany(
                "INSERT INTO t_notification (user_id,is_global,global_notification_id,type,title,content,link_url,is_read,created_at,updated_at,read_at,deleted) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                batch,
            )

        impression_rows = []
        click_rows = []
        ad_impression_count = defaultdict(int)
        ad_click_count = defaultdict(int)
        for ad_id, _ in ads:
            imp_count = random.randint(200, 700)
            for _ in range(imp_count):
                created = random_dt(start_30, now)
                user_id = random.choice(user_ids + [0] * 4)
                impression_rows.append((
                    ad_id,
                    user_id,
                    f'172.16.{random.randint(1, 20)}.{random.randint(2, 250)}',
                    random.choice(USER_AGENTS),
                    random.choice(PAGE_URLS),
                    created,
                ))
                ad_impression_count[ad_id] += 1
                if random.random() < 0.18:
                    click_time = created + timedelta(seconds=random.randint(5, 600))
                    click_rows.append((
                        ad_id,
                        user_id,
                        f'172.16.{random.randint(1, 20)}.{random.randint(2, 250)}',
                        random.choice(USER_AGENTS),
                        random.choice(REFERRERS),
                        click_time,
                    ))
                    ad_click_count[ad_id] += 1
        for batch in chunked(impression_rows, 1000):
            cur.executemany(
                "INSERT INTO t_ad_impression (ad_id,user_id,ip_address,user_agent,page_url,created_at) VALUES (%s,%s,%s,%s,%s,%s)",
                batch,
            )
        for batch in chunked(click_rows, 1000):
            cur.executemany(
                "INSERT INTO t_ad_click (ad_id,user_id,ip_address,user_agent,referrer_url,created_at) VALUES (%s,%s,%s,%s,%s,%s)",
                batch,
            )

        for tool_id, _, _, _ in tools:
            rating_count = tool_rating_count[tool_id]
            rating_score = round(tool_rating_total[tool_id] / rating_count, 2) if rating_count else 0
            cur.execute(
                "UPDATE t_tool SET use_count=%s, view_count=%s, review_count=%s, rating_count=%s, rating_score=%s WHERE id=%s",
                (
                    tool_usage_count[tool_id],
                    tool_usage_count[tool_id] * random.randint(2, 5) + tool_view_count[tool_id],
                    tool_review_count[tool_id],
                    rating_count,
                    rating_score,
                    tool_id,
                ),
            )
        for ad_id, _ in ads:
            cur.execute(
                "UPDATE t_advertisement SET impression_count=%s, click_count=%s WHERE id=%s",
                (ad_impression_count[ad_id], ad_click_count[ad_id], ad_id),
            )

    conn.commit()

    with conn.cursor() as cur:
        print('seed complete')
        for table in ['t_user','t_tool','t_tool_usage','t_tool_review','t_user_favorite','t_notification','t_user_subscription','t_ad_click','t_ad_impression','t_tag','t_tool_tag']:
            cur.execute(f'SELECT COUNT(*) FROM {table}')
            print(f'{table}\t{cur.fetchone()[0]}')
    conn.close()


if __name__ == '__main__':
    main()
