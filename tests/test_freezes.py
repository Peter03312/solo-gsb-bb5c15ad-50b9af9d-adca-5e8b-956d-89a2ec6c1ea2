"""通道额度冻结/恢复功能与并发安全测试（真实 PostgreSQL + TestClient）。

覆盖需求（按地区+渠道临时冻结新额度发放）：
- 操作接收地区、渠道、冻结/恢复状态；冻结须填原因；查询返回当前状态、
  原因与变更时刻。
- 冻结期间新的直接发行、额度预留、候补受理均明确拒绝（channel_frozen）
  且不占额、不入队。
- 既有预留仍可确认/取消；已通过申请仍可撤销/改期；授权仍可改挂；
  但这些操作释放的额度在冻结期不得使候补成交。
- 到期预留与候补仍按原有效期惰性结算（冻结不延长有效期）。
- 恢复时先按原受理顺序处理本通道仍有效的候补，再允许新请求占额。
- 冻结/恢复与并发发行按通道串行；重复同态操作不改动原因与变更时刻。
- 不存在授权的通道、缺失冻结原因、非法状态明确报错；其他通道不受影响。

运行：
    DATABASE_URL=postgresql+psycopg://licensing@/licensing?host=/tmp&port=5439 \
        python3 -m tests.test_freezes
"""
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select, text
from sqlalchemy import update as sa_update

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+psycopg://licensing@/licensing?host=/tmp&port=5439",
)

from fastapi.testclient import TestClient  # noqa: E402

from app.database import Base, SessionLocal, engine, init_db  # noqa: E402
from app.main import app  # noqa: E402
from app import models  # noqa: E402

CST = timezone(timedelta(hours=8))
OCT_START = "2026-10-01T00:00:00+08:00"
OCT_END = "2026-11-01T00:00:00+08:00"
OCCUR = "2026-10-15T12:00:00+08:00"

passed = 0
failed = 0


def check(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        print(f"  FAIL  {name}  {detail}")


def new_client():
    return TestClient(app)


def make_material(c, code):
    return c.post(
        "/api/v1/materials", json={"code": code, "name": code}
    ).json()["id"]


def make_auth(c, mid, count, region="CN", channel="web",
              starts=OCT_START, ends=OCT_END):
    return c.post(
        "/api/v1/authorizations",
        json={
            "material_id": mid,
            "region": region,
            "channel": channel,
            "starts_at": starts,
            "ends_at": ends,
            "max_count": count,
        },
    ).json()["id"]


def apply(c, mids, region="CN", channel="web", occur=OCCUR):
    return c.post(
        "/api/v1/distributions",
        json={
            "material_ids": mids,
            "region": region,
            "channel": channel,
            "occur_at": occur,
        },
    )


def reserve(c, mids, ttl=15, region="CN", channel="web", occur=OCCUR):
    return c.post(
        "/api/v1/reservations",
        json={
            "material_ids": mids,
            "region": region,
            "channel": channel,
            "occur_at": occur,
            "ttl_minutes": ttl,
        },
    )


def waitlist(c, mids, ttl=60, region="CN", channel="web", occur=OCCUR):
    return c.post(
        "/api/v1/waitlists",
        json={
            "material_ids": mids,
            "region": region,
            "channel": channel,
            "occur_at": occur,
            "ttl_minutes": ttl,
        },
    )


def freeze(c, region, channel, status, reason=None):
    body = {"region": region, "channel": channel, "status": status}
    if reason is not None:
        body["reason"] = reason
    return c.put("/api/v1/channel-freezes", json=body)


def get_freeze(c, region, channel):
    return c.get(f"/api/v1/channel-freezes/{region}/{channel}")


def force_expire_reservation(reservation_id):
    """直接把预留到期时刻改为当前时刻（左闭右开：now == expires_at 即逾期）。"""
    db = SessionLocal()
    try:
        db.execute(
            sa_update(models.Reservation)
            .where(models.Reservation.id == reservation_id)
            .values(expires_at=func.now())
        )
        db.commit()
    finally:
        db.close()


def set_waitlist_expiry(waitlist_id, at_sql):
    db = SessionLocal()
    try:
        db.execute(
            sa_update(models.Waitlist)
            .where(models.Waitlist.id == waitlist_id)
            .values(expires_at=text(at_sql))
        )
        db.commit()
    finally:
        db.close()


def books(auth_id):
    """账实相符：used/reserved 与有效明细数一致且不超额。"""
    db = SessionLocal()
    try:
        auth = db.get(models.Authorization, auth_id)
        approved_items = db.scalar(
            select(func.count())
            .select_from(models.DistributionItem)
            .join(
                models.Distribution,
                models.DistributionItem.distribution_id
                == models.Distribution.id,
            )
            .where(
                models.DistributionItem.authorization_id == auth_id,
                models.Distribution.status == "approved",
            )
        )
        pending_items = db.scalar(
            select(func.count())
            .select_from(models.ReservationItem)
            .join(
                models.Reservation,
                models.ReservationItem.reservation_id == models.Reservation.id,
            )
            .where(
                models.ReservationItem.authorization_id == auth_id,
                models.Reservation.status == "pending",
                models.Reservation.expires_at > func.now(),
            )
        )
        ok = (
            auth.used_count == approved_items
            and auth.reserved_count == pending_items
            and 0 <= auth.used_count
            and 0 <= auth.reserved_count
            and auth.used_count + auth.reserved_count <= auth.max_count
        )
        return ok, auth.used_count, auth.reserved_count
    finally:
        db.close()


def main() -> int:
    global failed
    Base.metadata.drop_all(bind=engine)
    init_db()
    c = new_client()

    print("== 1. 参数与状态校验 ==")
    m = make_material(c, "fz-m")
    a = make_auth(c, m, 2)

    # 有授权但从未冻结：查询派生 active（changed_at 为 null）。
    r = get_freeze(c, "CN", "web")
    check("未冻结查询 200 active", r.status_code == 200, r.text)
    b = r.json()
    check("派生 active", b["status"] == "active", b)
    check("无原因", b["reason"] is None, b)
    check("无 frozen_at", b["frozen_at"] is None, b)
    check("无 changed_at", b["changed_at"] is None, b)

    # 完全不存在的通道（无任何授权也无冻结记录）→ 404。
    r = get_freeze(c, "ZZ", "nosuch")
    check("未知通道查询 404", r.status_code == 404, r.text)

    r = freeze(c, "CN", "web", "weird", reason="x")
    check("非法状态 422 invalid_freeze_status",
          r.status_code == 422
          and r.json()["error"]["code"] == "invalid_freeze_status", r.text)

    r = freeze(c, "CN", "web", "frozen")
    check("缺原因 422 freeze_reason_required",
          r.status_code == 422
          and r.json()["error"]["code"] == "freeze_reason_required", r.text)

    r = freeze(c, "ZZ", "nosuch", "frozen", reason="r")
    check("无授权通道冻结 422 channel_without_authorization",
          r.status_code == 422
          and r.json()["error"]["code"]
          == "channel_without_authorization", r.text)

    # 空白原因（strip 后为空）也算缺失。
    r = freeze(c, "CN", "web", "frozen", reason="   ")
    check("空白原因 422", r.status_code == 422, r.text)

    print("== 2. 冻结：返回状态/原因/时刻，重复同态不改写 ==")
    r = freeze(c, "CN", "web", "frozen", reason="合规核查-暂停发新")
    check("冻结 200", r.status_code == 200, r.text)
    b1 = r.json()
    check("status frozen", b1["status"] == "frozen", b1)
    check("原因回显", b1["reason"] == "合规核查-暂停发新", b1)
    check("frozen_at 有值", b1["frozen_at"] is not None, b1)
    check("changed_at==frozen_at",
          b1["changed_at"] == b1["frozen_at"], b1)
    check("unfrozen_at 为空", b1["unfrozen_at"] is None, b1)
    frozen_at_1 = b1["frozen_at"]

    r = freeze(c, "CN", "web", "frozen", reason="另一个原因，应被忽略")
    b_again = r.json()
    check("重复冻结仍 200", r.status_code == 200, r.text)
    check("重复冻结原因不改写",
          b_again["reason"] == "合规核查-暂停发新", b_again)
    check("重复冻结时刻不改写",
          b_again["frozen_at"] == frozen_at_1
          and b_again["changed_at"] == frozen_at_1, b_again)

    print("== 3. 冻结期：新直接发行/预留/候补一律拒绝且不占额 ==")
    r = apply(c, [m])
    check("直接发行 422 channel_frozen",
          r.status_code == 422
          and r.json()["error"]["code"] == "channel_frozen", r.text)

    r = reserve(c, [m], ttl=10)
    check("额度预留 422 channel_frozen",
          r.status_code == 422
          and r.json()["error"]["code"] == "channel_frozen", r.text)

    r = waitlist(c, [m], ttl=30)
    check("候补受理 422 channel_frozen",
          r.status_code == 422
          and r.json()["error"]["code"] == "channel_frozen", r.text)

    # 不占额、不入队。
    ok, used, reserved = books(a)
    check("拒绝不占额", ok and used == 0 and reserved == 0,
          f"used={used} reserved={reserved}")
    r = c.get("/api/v1/waitlists?status=pending")
    check("候补队列仍为空", r.json() == [], r.text)

    print("== 4. 其他通道不受冻结影响 ==")
    a_us = make_auth(c, m, 2, region="US", channel="web")
    r = apply(c, [m], region="US", channel="web")
    check("其他通道可发行", r.status_code == 201, r.text)
    r = reserve(c, [m], ttl=10, region="US", channel="web")
    check("其他通道可预留", r.status_code == 201, r.text)
    ok, used, reserved = books(a_us)
    check("其他通道账实相符", ok and used == 1 and reserved == 1,
          f"used={used} reserved={reserved}")
    c.post("/api/v1/reservations/1/cancel")

    print("== 5. 冻结期既有预留仍可确认（槽位转正式占用） ==")
    # 先恢复，建一条预留并占满额度，再冻结。
    r = freeze(c, "CN", "web", "active")
    check("恢复 200", r.status_code == 200 and r.json()["status"] == "active",
          r.text)
    a1 = make_auth(c, make_material(c, "fz-m2"), 1, region="CN",
                   channel="shop")
    # CN/web：授权 a max=2。预留 1 笔（占用 1），直接发行 1 笔（占满）。
    r = reserve(c, [m], ttl=20)
    check("恢复后可预留", r.status_code == 201, r.text)
    rid = r.json()["id"]
    r = apply(c, [m])
    check("恢复后可发行", r.status_code == 201, r.text)
    d_existing = r.json()["id"]
    ok, used, reserved = books(a)
    check("占满 used=1 reserved=1", ok and used == 1 and reserved == 1,
          f"used={used} reserved={reserved}")

    # 冻结期间排一个候补到队列（需先在冻结前入队，因为冻结期拒绝受理）。
    # 场景：CN/web 额度已满 → 候补 pending 不占额；然后冻结。
    r = waitlist(c, [m], ttl=60)
    check("满额候补入队", r.status_code == 201
          and r.json()["status"] == "pending", r.text)
    w_pending = r.json()["id"]

    freeze(c, "CN", "web", "frozen", reason="冻结期既有操作演练")

    # 5a) 既有预留确认：成功，reserved->used，不触发候补成交。
    r = c.post(f"/api/v1/reservations/{rid}/confirm")
    check("冻结期确认预留 200", r.status_code == 200, r.text)
    check("确认转申请",
          r.json()["status"] == "confirmed"
          and r.json()["distribution_id"] is not None, r.text)
    r = c.get(f"/api/v1/waitlists/{w_pending}")
    check("确认未使候补成交",
          r.json()["status"] == "pending", r.text)
    # 确认是槽位转换：总量不变 used=2 reserved=0（max=2）。
    ok, used, reserved = books(a)
    check("确认后账实相符 used=2 reserved=0",
          ok and used == 2 and reserved == 0,
          f"used={used} reserved={reserved}")

    print("== 6. 冻结期已通过申请可撤销/改期，但释放额度不成交候补 ==")
    r = c.post(f"/api/v1/distributions/{d_existing}/revoke")
    check("冻结期撤销 200", r.status_code == 200
          and r.json()["status"] == "revoked", r.text)
    r = c.get(f"/api/v1/waitlists/{w_pending}")
    check("撤销释放未成交候补",
          r.json()["status"] == "pending", r.text)
    ok, used, reserved = books(a)
    check("撤销后 used=1", ok and used == 1, f"used={used}")

    # 6b) 改期（新时刻仍在授权期内）：允许；归属/额度变化也不成交候补。
    other_occur = "2026-10-20T09:00:00+08:00"
    r = c.post(
        f"/api/v1/distributions/{d_existing}/reschedule",
        json={"occur_at": other_occur},
    )
    check("已撤销申请改期仍 409", r.status_code == 409, r.text)

    # 对确认预留生成的那笔（approved）改期：同一瞬间幂等 + 真正改期均可。
    confirmed_dist = c.get(f"/api/v1/reservations/{rid}").json()[
        "distribution_id"
    ]
    r = c.post(
        f"/api/v1/distributions/{confirmed_dist}/reschedule",
        json={"occur_at": other_occur},
    )
    check("冻结期改期允许", r.status_code == 200, r.text)
    r = c.get(f"/api/v1/waitlists/{w_pending}")
    check("改期未成交候补", r.json()["status"] == "pending", r.text)
    ok, used, reserved = books(a)
    check("改期后账实相符", ok and used == 1 and reserved == 0,
          f"used={used} reserved={reserved}")

    print("== 6b. 冻结期取消既有预留释放额度也不成交候补 ==")
    # 新通道：预留占槽 + 候补排队；冻结后取消预留，候补不得成交。
    m6 = make_material(c, "fz-m6")
    a6 = make_auth(c, m6, 1, region="CN", channel="fax")
    r = reserve(c, [m6], ttl=20, region="CN", channel="fax")
    check("fax 预留占槽", r.status_code == 201, r.text)
    rid6 = r.json()["id"]
    r = waitlist(c, [m6], ttl=60, region="CN", channel="fax")
    check("fax 候补入队（额度被预留占住）",
          r.status_code == 201 and r.json()["status"] == "pending", r.text)
    w6 = r.json()["id"]
    freeze(c, "CN", "fax", "frozen", reason="fax 取消预留演练")
    r = c.post(f"/api/v1/reservations/{rid6}/cancel")
    check("冻结期取消既有预留允许",
          r.status_code == 200 and r.json()["status"] == "cancelled", r.text)
    ok, used, reserved = books(a6)
    check("取消释放 reserved=0", ok and reserved == 0 and used == 0,
          f"used={used} reserved={reserved}")
    r = c.get(f"/api/v1/waitlists/{w6}")
    check("取消预留释放未使候补成交（冻结期）",
          r.json()["status"] == "pending", r.text)
    # 恢复后该候补才按受理顺序成交。
    r = freeze(c, "CN", "fax", "active")
    check("fax 恢复 200", r.status_code == 200, r.text)
    r = c.get(f"/api/v1/waitlists/{w6}")
    check("恢复后 fax 候补成交",
          r.json()["status"] == "fulfilled", r.text)

    print("== 7. 冻结期授权仍可改挂，迁出释放不成交候补 ==")
    # CN/shop 通道：a1 max=1 被申请占用；候补 w_shop 排队。
    # 找到 a1 对应的素材：a1 是为 fz-m2 创建的。
    db = SessionLocal()
    m2_id = db.get(models.Authorization, a1).material_id
    db.close()
    r = apply(c, [m2_id], region="CN", channel="shop")
    check("CN/shop 占用申请", r.status_code == 201, r.text)
    r = waitlist(c, [m2_id], ttl=60, region="CN", channel="shop")
    check("CN/shop 候补入队", r.status_code == 201
          and r.json()["status"] == "pending", r.text)
    w_shop = r.json()["id"]

    a1_repl = make_auth(c, m2_id, 5, region="CN", channel="shop")
    freeze(c, "CN", "shop", "frozen", reason="shop 通道冻结演练")
    r = c.post(
        "/api/v1/authorizations/migrate",
        json={
            "source_authorization_id": a1,
            "replacement_authorization_id": a1_repl,
        },
    )
    check("冻结期改挂允许", r.status_code == 200, r.text)
    check("改挂迁移 1 条", r.json()["migrated_count"] == 1, r.text)
    r = c.get(f"/api/v1/waitlists/{w_shop}")
    check("改挂释放未成交候补（冻结期）",
          r.json()["status"] == "pending", r.text)

    print("== 8. 冻结不延长有效期：到期预留仍结算、候补仍失效 ==")
    # 8a) 冻结期新预留无法创建；用恢复后创建、再冻结、再强改到期时刻的方式。
    freeze(c, "CN", "shop", "active")
    m3 = make_material(c, "fz-m3")
    a3 = make_auth(c, m3, 2, region="CN", channel="app")
    r = reserve(c, [m3], ttl=20, region="CN", channel="app")
    check("CN/app 预留创建", r.status_code == 201, r.text)
    rid3 = r.json()["id"]
    freeze(c, "CN", "app", "frozen", reason="app 到期演练")
    force_expire_reservation(rid3)
    # 触发惰性结算（单条预留查询）：应落库 expired 且归还 reserved。
    r = c.get(f"/api/v1/reservations/{rid3}")
    check("冻结期到期预留仍结算 expired",
          r.json()["status"] == "expired", r.text)
    ok, used, reserved = books(a3)
    check("到期预留已释放 reserved=0", ok and reserved == 0,
          f"reserved={reserved}")
    r = c.post(f"/api/v1/reservations/{rid3}/confirm")
    check("到期预留冻结期仍不可确认 409",
          r.status_code == 409
          and r.json()["error"]["code"] == "reservation_expired", r.text)

    # 8b) 候补有效期：冻结期排不到成交，到期仍被标记 expired（不延长）。
    # CN/web 仍冻结，w_pending 已在队列；强改其 expires_at 为已过期。
    set_waitlist_expiry(w_pending, "now() - interval '1 minute'")
    r = c.get(f"/api/v1/waitlists/{w_pending}")
    check("冻结期过期候补仍失效",
          r.status_code == 200
          and r.json()["status"] == "expired"
          and r.json()["expiry_reason"] == "ttl_expired"
          and r.json()["expired_at"] is not None, r.text)
    r = c.post(f"/api/v1/waitlists/{w_pending}/cancel")
    check("失效候补取消 409",
          r.status_code == 409
          and r.json()["error"]["code"] == "waitlist_already_expired", r.text)

    print("== 9. 恢复：先按受理顺序处理仍有效候补，再放通新请求 ==")
    # 构造 CN/app：a3 max=2。两笔候补排队（均 pending），当前额度全空。
    # 先恢复再占满额度，然后入两笔候补，再冻结+释放额度+恢复验证 FIFO。
    freeze(c, "CN", "app", "active")
    r = apply(c, [m3], region="CN", channel="app")
    check("CN/app 占满用申请 1", r.status_code == 201, r.text)
    d_app1 = r.json()["id"]
    r = apply(c, [m3], region="CN", channel="app")
    check("CN/app 占满用申请 2", r.status_code == 201, r.text)
    d_app2 = r.json()["id"]
    r = waitlist(c, [m3], ttl=120, region="CN", channel="app")
    check("候补 A 入队", r.json()["status"] == "pending", r.text)
    wA = r.json()["id"]
    r = waitlist(c, [m3], ttl=120, region="CN", channel="app")
    check("候补 B 入队", r.json()["status"] == "pending", r.text)
    wB = r.json()["id"]

    # 冻结期间撤销两笔申请，释放 2 个额度——但不得成交。
    freeze(c, "CN", "app", "frozen", reason="恢复顺序演练")
    c.post(f"/api/v1/distributions/{d_app1}/revoke")
    c.post(f"/api/v1/distributions/{d_app2}/revoke")
    ok, used, _ = books(a3)
    check("冻结期撤销后额度空闲 used=0", ok and used == 0, f"used={used}")
    r = c.get(f"/api/v1/waitlists/{wA}")
    check("A 冻结期仍 pending", r.json()["status"] == "pending", r.text)

    # 恢复：A、B 按受理顺序依次成交（max=2），随后新请求额度不足。
    r = freeze(c, "CN", "app", "active")
    check("恢复 200 active",
          r.status_code == 200 and r.json()["status"] == "active", r.text)
    b = r.json()
    check("恢复 unfrozen_at 有值", b["unfrozen_at"] is not None, b)
    check("恢复 reason 清空", b["reason"] is None, b)

    r = c.get(f"/api/v1/waitlists/{wA}")
    check("恢复后 A 成交",
          r.json()["status"] == "fulfilled"
          and r.json()["distribution_id"] is not None, r.text)
    r = c.get(f"/api/v1/waitlists/{wB}")
    check("恢复后 B 成交", r.json()["status"] == "fulfilled", r.text)

    # 候补先占额：恢复后新的直接申请应额度不足（422）。
    r = apply(c, [m3], region="CN", channel="app")
    check("恢复后候补优先占额，新申请被拒",
          r.status_code == 422
          and r.json()["error"]["code"] == "distribution_rejected", r.text)
    ok, used, _ = books(a3)
    check("恢复成交账实相符 used=2", ok and used == 2, f"used={used}")

    print("== 10. 恢复时过期候补让出位置，后续有效候补按序成交 ==")
    m4 = make_material(c, "fz-m4")
    a4 = make_auth(c, m4, 1, region="CN", channel="tv")
    r = apply(c, [m4], region="CN", channel="tv")
    d_tv = r.json()["id"]
    r = waitlist(c, [m4], ttl=120, region="CN", channel="tv")
    wOld = r.json()["id"]
    r = waitlist(c, [m4], ttl=120, region="CN", channel="tv")
    wNew = r.json()["id"]
    freeze(c, "CN", "tv", "frozen", reason="tv 过期演练")
    # 旧候补在冻结期间过期（冻结不延长）。
    set_waitlist_expiry(wOld, "now() - interval '1 minute'")
    c.post(f"/api/v1/distributions/{d_tv}/revoke")
    # 旧候选取样确认仍 expired（读路径惰性失效）。
    r = c.get(f"/api/v1/waitlists/{wOld}")
    check("旧候补冻结期已过期", r.json()["status"] == "expired", r.text)
    freeze(c, "CN", "tv", "active")
    r = c.get(f"/api/v1/waitlists/{wOld}")
    check("恢复后旧候补保持 expired",
          r.json()["status"] == "expired", r.text)
    r = c.get(f"/api/v1/waitlists/{wNew}")
    check("恢复后有效候补 wNew 成交（过期者让位）",
          r.json()["status"] == "fulfilled", r.text)

    print("== 11. 冻结期取消既有候补允许（不占额，不影响他人） ==")
    # CN/shop 恢复时，冻结期积压的 w_shop 应在恢复事务内成交。
    freeze(c, "CN", "shop", "active")
    r = c.get(f"/api/v1/waitlists/{w_shop}")
    check("CN/shop 恢复后冻结期积压候补成交",
          r.json()["status"] == "fulfilled", r.text)
    # 独立通道验证：冻结期取消一笔此前已受理的 pending 候补。
    m_k = make_material(c, "fz-mk")
    a_k = make_auth(c, m_k, 1, region="CN", channel="kiosk")
    r = apply(c, [m_k], region="CN", channel="kiosk")
    check("kiosk 占用唯一额度", r.status_code == 201, r.text)
    r = waitlist(c, [m_k], ttl=60, region="CN", channel="kiosk")
    w_kiosk = r.json()["id"]
    check("kiosk 候补入队", r.json()["status"] == "pending", r.text)
    freeze(c, "CN", "kiosk", "frozen", reason="kiosk 取消演练")
    r = c.post(f"/api/v1/waitlists/{w_kiosk}/cancel")
    check("冻结期取消既有候补允许",
          r.status_code == 200 and r.json()["status"] == "cancelled", r.text)
    r = c.post(f"/api/v1/waitlists/{w_kiosk}/cancel")
    check("重复取消仍 409",
          r.status_code == 409
          and r.json()["error"]["code"]
          == "waitlist_already_cancelled", r.text)

    print("== 12. 再冻结保留新原因与新时刻（非重复同态） ==")
    r = freeze(c, "CN", "web", "active")
    check("CN/web 恢复", r.json()["status"] == "active", r.text)
    import time
    time.sleep(0.01)
    r = freeze(c, "CN", "web", "frozen", reason="第二轮冻结原因")
    check("再冻结 200", r.status_code == 200, r.text)
    b = r.json()
    check("再冻结原因更新", b["reason"] == "第二轮冻结原因", b)
    check("再冻结时刻晚于首次", b["frozen_at"] > frozen_at_1, b)
    freeze(c, "CN", "web", "active")

    print("== 13. 列表与过滤 ==")
    # 从未冻结的通道（即便存在授权）不出现在列表中。
    make_auth(c, make_material(c, "fz-never"), 1, region="MX",
              channel="web")
    r = c.get("/api/v1/channel-freezes")
    check("列表 200 数组", r.status_code == 200
          and isinstance(r.json(), list), r.text)
    rows = r.json()
    pairs = {(x["region"], x["channel"]) for x in rows}
    check("列表含已操作通道",
          ("CN", "app") in pairs and ("CN", "tv") in pairs, r.text[:200])
    check("从未冻结通道不入列表", ("MX", "web") not in pairs, r.text[:300])
    r = c.get("/api/v1/channel-freezes?status=active")
    check("active 过滤全部 active",
          all(x["status"] == "active" for x in r.json()), r.text)
    r = c.get("/api/v1/channel-freezes?status=frozen")
    check("frozen 过滤全部 frozen",
          all(x["status"] == "frozen" for x in r.json()), r.text)

    print("== 13b. channel_frozen 错误信封 details 带通道信息 ==")
    freeze(c, "MX", "web", "frozen", reason="信封校验")
    r = apply(c, [make_material(c, "fz-env")], region="MX", channel="web")
    check("冻结拒绝信封",
          r.status_code == 422
          and r.json()["error"]["code"] == "channel_frozen"
          and r.json()["error"]["details"]
          == [{"region": "MX", "channel": "web"}], r.text)
    freeze(c, "MX", "web", "active")

    print("== 14. 并发：冻结/恢复与发行按通道串行，不超额、状态一致 ==")
    m5 = make_material(c, "fz-m5")
    a5 = make_auth(c, m5, 1, region="EU", channel="web")

    gate: list[int] = []

    def wait_gate():
        while not gate:
            pass

    def do_freeze():
        wait_gate()
        return freeze(new_client(), "EU", "web", "frozen",
                      reason="并发冻结")

    def do_apply():
        wait_gate()
        return apply(new_client(), [m5], region="EU", channel="web")

    # 8 个线程：4 个尝试冻结、4 个尝试发行；唯一额度要么被某笔发行拿到、
    # 要么发行被 channel_frozen 拒绝（冻结先拿到锁）。
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = [ex.submit(do_apply) for _ in range(4)]
        futs += [ex.submit(do_freeze) for _ in range(4)]
        gate.append(1)  # 释放栅栏
        results = [f.result() for f in futs]

    apply_results = [
        r for r in results
        if r.request.url.path.endswith("/distributions")
    ]
    freeze_results = [
        r for r in results
        if r.request.url.path.endswith("/channel-freezes")
    ]
    succeeded = [r for r in apply_results if r.status_code == 201]
    rejected_frozen = [
        r for r in apply_results
        if r.json().get("error", {}).get("code") == "channel_frozen"
    ]
    rejected_quota = [
        r for r in apply_results
        if r.json().get("error", {}).get("code") == "distribution_rejected"
    ]
    check("发行尝试共 4 笔", len(apply_results) == 4,
          f"{len(apply_results)}")
    check("成功发行不超过 1 笔（绝不超额）", len(succeeded) <= 1,
          f"{len(succeeded)}")
    check("其余发行均为冻结拒绝或额度拒绝",
          len(succeeded) + len(rejected_frozen) + len(rejected_quota)
          == len(apply_results),
          f"success={len(succeeded)} frozen={len(rejected_frozen)} "
          f"quota={len(rejected_quota)}")
    check("冻结调用均 200",
          all(r.status_code == 200 for r in freeze_results),
          str([r.status_code for r in freeze_results]))
    ok, used, reserved = books(a5)
    check("并发后账实相符不超额", ok and used <= 1 and used + reserved <= 1,
          f"used={used} reserved={reserved}")
    # 冻结原因/时刻唯一（重复同态不改写）。
    r = get_freeze(c, "EU", "web")
    check("并发冻结后状态 frozen", r.json()["status"] == "frozen", r.text)
    frozen_ts = r.json()["frozen_at"]
    frozen_reason = r.json()["reason"]
    freeze(c, "EU", "web", "frozen", reason="不应生效的重复原因")
    r = get_freeze(c, "EU", "web")
    check("并发后重复冻结原因不变",
          r.json()["reason"] == frozen_reason, r.text)
    check("并发后重复冻结时刻不变",
          r.json()["frozen_at"] == frozen_ts, r.text)

    print("== 15. 恢复与并发受理：候补先于新申请占额 ==")
    # EU/web：恢复，撤销上面可能成交的申请使额度清空。
    db = SessionLocal()
    dist_ids = list(
        db.scalars(
            select(models.Distribution.id)
            .join(models.DistributionItem)
            .join(
                models.Authorization,
                models.DistributionItem.authorization_id
                == models.Authorization.id,
            )
            .where(
                models.Authorization.id == a5,
                models.Distribution.status == "approved",
            )
        )
    )
    db.close()
    freeze(c, "EU", "web", "active")
    for did in dist_ids:
        c.post(f"/api/v1/distributions/{did}/revoke")

    # 占满额度 → 候补入队 → 冻结 → 释放额度 → 并发（恢复 + 新直接申请）。
    r = apply(c, [m5], region="EU", channel="web")
    d_eu = r.json()["id"]
    r = waitlist(c, [m5], ttl=120, region="EU", channel="web")
    w_eu = r.json()["id"]
    freeze(c, "EU", "web", "frozen", reason="恢复并发演练2")
    c.post(f"/api/v1/distributions/{d_eu}/revoke")

    gate2: list[int] = []

    def do_unfreeze():
        while not gate2:
            pass
        return freeze(new_client(), "EU", "web", "active")

    def do_apply2():
        while not gate2:
            pass
        return apply(new_client(), [m5], region="EU", channel="web")

    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(do_unfreeze)
        f2 = ex.submit(do_apply2)
        gate2.append(1)
        r_unfreeze = f1.result()
        r_apply = f2.result()
    check("恢复成功", r_unfreeze.status_code == 200
          and r_unfreeze.json()["status"] == "active", r_unfreeze.text)
    check("恢复与新申请并发：候补优先，新申请要么 422 要么冻结拒绝",
          r_apply.status_code == 422
          and r_apply.json()["error"]["code"]
          in ("distribution_rejected", "channel_frozen"),
          r_apply.text)
    r = c.get(f"/api/v1/waitlists/{w_eu}")
    check("候补在恢复事务内成交",
          r.json()["status"] == "fulfilled", r.text)
    ok, used, _ = books(a5)
    check("最终账实相符 used=1", ok and used == 1, f"used={used}")

    print()
    print(f"结果：{passed} 通过，{failed} 失败")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
