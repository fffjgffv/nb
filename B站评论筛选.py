# -*- coding: utf-8 -*-
"""
B站评论抓取工具 - GUI 并发优化版
核心优化：
1. 楼中楼并发抓取（ThreadPoolExecutor）+ 主评论翻页流水线重叠
2. 全局令牌桶限流，精确控制请求速率，替代固定 sleep
3. 每线程独立 Session + 连接池，WBI keys 全局锁保护
4. GUI 提供三档速度模式
"""

import csv
import hashlib
import json
import queue
import random
import re
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import reduce

import requests
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext

# ============================================================
# 常量
# ============================================================

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

MIXIN_KEY_ENC_TAB = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
    27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
    37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
    22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
]

# 三档速度预设：rate=每秒请求数，workers=并发线程数，burst=令牌桶突发容量
SPEED_PRESETS = {
    "保守（稳）":      {"rate": 1.5, "workers": 3, "burst": 1.5},
    "平衡（推荐）":    {"rate": 3.0, "workers": 5, "burst": 2.0},
    "极速（可能风控）": {"rate": 5.0, "workers": 8, "burst": 2.5},
}

MAIN_URL = "https://api.bilibili.com/x/v2/reply/wbi/main"
SUB_URL = "https://api.bilibili.com/x/v2/reply/reply"
NAV_URL = "https://api.bilibili.com/x/web-interface/nav"
VIEW_URL = "https://api.bilibili.com/x/web-interface/view"


# ============================================================
# WBI 签名
# ============================================================

def _mixin_key(orig: str) -> str:
    return reduce(lambda s, i: s + orig[i], MIXIN_KEY_ENC_TAB, "")[:32]


def _enc_wbi(params: dict, img_key: str, sub_key: str) -> str:
    mixin = _mixin_key(img_key + sub_key)
    cleaned = {}
    for k, v in params.items():
        if k in ("w_rid", "wts"):
            continue
        cleaned[k] = re.sub(r"[!'()*]", "", str(v))
    cleaned["wts"] = int(time.time())
    cleaned = dict(sorted(cleaned.items()))
    query = urllib.parse.urlencode(
        cleaned, quote_via=urllib.parse.quote, safe=""
    )
    w_rid = hashlib.md5((query + mixin).encode()).hexdigest()
    return query + "&w_rid=" + w_rid


class StopCrawl(Exception):
    pass


# ============================================================
# 令牌桶限流器
# ============================================================

class TokenBucket:
    """全局共享，所有请求先 acquire 令牌，保证平均速率恒定"""

    def __init__(self, rate: float, burst: float = 2.0):
        self.rate = rate
        self.capacity = max(1.0, burst)
        self.tokens = self.capacity
        self.last = time.time()
        self.lock = threading.Lock()

    def acquire(self, stop_event: threading.Event):
        while True:
            if stop_event.is_set():
                raise StopCrawl()
            with self.lock:
                now = time.time()
                self.tokens = min(
                    self.capacity,
                    self.tokens + (now - self.last) * self.rate,
                )
                self.last = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                wait = (1.0 - self.tokens) / self.rate
            # 锁外等待，避免阻塞其他线程
            end = time.time() + wait
            while time.time() < end:
                if stop_event.is_set():
                    raise StopCrawl()
                time.sleep(0.02)


# ============================================================
# 爬虫核心
# ============================================================

class Crawler:
    def __init__(self, bvid, keyword, cookie, log_cb, stop_event, speed):
        cfg = SPEED_PRESETS[speed]
        self.bvid = bvid
        self.keyword = keyword.lower().strip()
        self.cookie = cookie
        self.log = log_cb
        self.stop = stop_event
        self.workers = cfg["workers"]
        self.limiter = TokenBucket(cfg["rate"], cfg["burst"])
        self.local = threading.local()      # 每线程独立 session
        self.keys_lock = threading.Lock()   # WBI keys 刷新锁
        self.img_key = ""
        self.sub_key = ""
        self.risk_lock = threading.Lock()
        self.risk_count = 0

    # ---------- 基础工具 ----------

    def _check_stop(self):
        if self.stop.is_set():
            raise StopCrawl()

    def _sleep(self, seconds):
        end = time.time() + seconds
        while time.time() < end:
            self._check_stop()
            time.sleep(0.05)

    def _session(self) -> requests.Session:
        """每线程一个 Session，避免连接池/状态竞争"""
        if not hasattr(self.local, "sess"):
            s = requests.Session()
            s.headers.update({
                "User-Agent": UA,
                "Referer": "https://www.bilibili.com/",
                "Cookie": self.cookie,
                "Accept": "application/json, text/plain, */*",
            })
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=10, pool_maxsize=10, max_retries=0
            )
            s.mount("https://", adapter)
            s.mount("http://", adapter)
            self.local.sess = s
        return self.local.sess

    # ---------- WBI keys ----------

    def _refresh_keys(self, force=False):
        with self.keys_lock:
            if not force and self.img_key:
                return
            resp = self._session().get(NAV_URL, timeout=15)
            resp.raise_for_status()
            wbi = resp.json()["data"]["wbi_img"]
            self.img_key = re.search(r"/([^/]+)\.png", wbi["img_url"]).group(1)
            self.sub_key = re.search(r"/([^/]+)\.png", wbi["sub_url"]).group(1)

    # ---------- 统一请求 ----------

    def _get(self, base_url, params, retries=4) -> dict:
        for attempt in range(1, retries + 1):
            self._check_stop()
            # ① 先抢令牌（限流核心）
            self.limiter.acquire(self.stop)

            # ② 取当前 keys 副本
            with self.keys_lock:
                img, sub = self.img_key, self.sub_key
            if not img:
                self._refresh_keys()
                with self.keys_lock:
                    img, sub = self.img_key, self.sub_key

            query = _enc_wbi(params, img, sub)
            try:
                resp = self._session().get(f"{base_url}?{query}", timeout=20)

                # 412 风控：全局计数，多次触发就集体冷静
                if resp.status_code == 412:
                    with self.risk_lock:
                        self.risk_count += 1
                        cnt = self.risk_count
                    if cnt >= 3:
                        self.log("log", f"[!] 连续风控，全局暂停 30s")
                        self._sleep(30)
                        with self.risk_lock:
                            self.risk_count = 0
                    else:
                        self.log("log", f"[!] 412 风控，暂停重试 ({attempt})")
                        self._sleep(random.uniform(5, 10))
                    continue

                resp.raise_for_status()
                data = resp.json()
                code = data.get("code", -1)

                if code == 0:
                    return data

                if code in (-400, -352):
                    self.log("log", f"[!] code={code}，刷新 keys 后重试 ({attempt})")
                    self._refresh_keys(force=True)
                    self._sleep(random.uniform(1, 3))
                    continue

                self.log("log", f"[!] 接口 code={code}: {data.get('message')}")
                return {}

            except (requests.RequestException, json.JSONDecodeError) as e:
                self.log("log", f"[!] 请求异常 ({attempt}/{retries}): {e}")
                self._sleep(random.uniform(3, 6))
        return {}

    # ---------- 主评论翻页 ----------

    def _get_aid(self):
        data = self._get(VIEW_URL, {"bvid": self.bvid})
        if not data:
            return None
        aid = data["data"]["aid"]
        self.log("log", f"[+] BV {self.bvid} -> aid={aid}")
        return aid

    def _make_pagination(self, offset):
        return json.dumps({"offset": offset}, separators=(",", ":"), ensure_ascii=False)

    def _iter_main_pages(self, aid):
        """生成器：逐页产出 (replies, cursor)"""
        offset = ""
        page = 1
        while True:
            self._check_stop()
            data = self._get(MAIN_URL, {
                "oid": aid,
                "type": 1,
                "mode": 3,
                "pagination_str": self._make_pagination(offset),
                "plat": 1,
                "web_location": 1315875,
            })
            if not data:
                return

            d = data.get("data", {})
            replies = d.get("replies") or []
            cursor = d.get("cursor", {})
            self.log("log", f"  一级评论第 {page} 页，本页 {len(replies)} 条")
            yield replies, cursor

            if cursor.get("is_end"):
                return
            nxt = (cursor.get("pagination_reply") or {}).get("next_offset") or cursor.get("next")
            if not nxt or nxt == offset:
                self.log("log", "  [!] 游标未前进，停止翻页")
                return
            offset = nxt
            page += 1

    # ---------- 楼中楼任务（在线程池中执行） ----------

    def _fetch_subs_task(self, aid, root):
        rpid = root.get("rpid")
        uname = (root.get("member") or {}).get("uname", "")
        msg = (root.get("content") or {}).get("message", "")
        subs, pn = [], 1
        while True:
            self._check_stop()
            data = self._get(SUB_URL, {
                "oid": aid, "type": 1, "root": rpid, "pn": pn, "ps": 20,
            })
            if not data:
                break
            replies = data.get("data", {}).get("replies") or []
            if not replies:
                break
            for r in replies:
                r["_root_rpid"] = rpid
                r["_root_username"] = uname
                r["_root_message"] = msg
            subs.extend(replies)
            page_info = data.get("data", {}).get("page", {})
            if pn >= page_info.get("count", 1):
                break
            pn += 1
        return subs

    # ---------- 扁平化 & 匹配 ----------

    @staticmethod
    def _flatten(reply, level):
        member = reply.get("member") or {}
        content = reply.get("content") or {}
        return {
            "层级": "一级评论" if level == 1 else "楼中楼",
            "rpid": reply.get("rpid"),
            "用户": member.get("uname", ""),
            "内容": content.get("message", "").replace("\n", " ").replace("\r", " "),
            "点赞数": reply.get("like", 0),
            "根评论用户": reply.get("_root_username", ""),
            "根评论内容": reply.get("_root_message", "").replace("\n", " ").replace("\r", " "),
            "根评论rpid": reply.get("_root_rpid", ""),
        }

    def _match(self, row):
        return (not self.keyword) or (self.keyword in row["内容"].lower())

    # ---------- 主流程 ----------

    def run(self):
        self.log("log", f"[*] 速度模式: rate={self.limiter.rate}/s, workers={self.workers}")
        self._refresh_keys()

        aid = self._get_aid()
        if not aid:
            raise RuntimeError("获取 aid 失败，请检查 BV 号 / Cookie")

        all_rows, hits = [], []
        total_roots = 0

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = []

            # 阶段一：翻页 + 提交楼中楼任务（流水线）
            for replies, cursor in self._iter_main_pages(aid):
                if total_roots == 0:
                    total_roots = cursor.get("all_count", 0)
                    self.log("progress_max", total_roots)

                for c in replies:
                    row = self._flatten(c, 1)
                    all_rows.append(row)
                    if self._match(row):
                        hits.append(row)
                        self.log("hit", row["内容"])

                    if c.get("rcount", 0) > 0:
                        futures.append(pool.submit(self._fetch_subs_task, aid, c))

                self.log("progress", (len(futures), max(total_roots, len(futures))))

            # 阶段二：等待所有楼中楼完成
            done = 0
            total_tasks = len(futures)
            self.log("log", f"[*] 主评论翻页完成，等待 {total_tasks} 条楼中楼任务 ...")

            for f in as_completed(futures):
                if self.stop.is_set():
                    break
                try:
                    subs = f.result()
                except StopCrawl:
                    break
                except Exception as e:
                    self.log("log", f"[!] 楼中楼任务异常: {e}")
                    done += 1
                    continue

                for s in subs:
                    row = self._flatten(s, 2)
                    all_rows.append(row)
                    if self._match(row):
                        hits.append(row)
                        self.log("hit", row["内容"])

                done += 1
                if done % 20 == 0 or done == total_tasks:
                    self.log("log", f"  楼中楼进度 {done}/{total_tasks}")
                    self.log("progress", (done, max(total_tasks, 1)))

        # 导出
        csv_path = f"bilibili_comments_{self.bvid}.csv"
        fieldnames = ["层级", "rpid", "用户", "内容", "点赞数",
                      "根评论用户", "根评论内容", "根评论rpid"]
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_rows)

        return all_rows, hits, csv_path


# ============================================================
# GUI
# ============================================================

class App:
    def __init__(self, root):
        self.root = root
        root.title("B站评论抓取工具（并发版）")
        root.geometry("880x720")
        root.minsize(760, 600)

        self.msg_queue = queue.Queue()
        self.stop_event = threading.Event()
        self.worker = None
        self.hit_count = 0

        self._build_ui()
        self._poll_queue()

    def _build_ui(self):
        pad = {"padx": 8, "pady": 4}

        frm = ttk.LabelFrame(self.root, text="抓取参数")
        frm.pack(fill="x", padx=10, pady=(10, 4))

        ttk.Label(frm, text="BV 号：").grid(row=0, column=0, sticky="e", **pad)
        self.entry_bv = ttk.Entry(frm, width=30)
        self.entry_bv.grid(row=0, column=1, sticky="w", **pad)
        self.entry_bv.insert(0, "BV1xxxxxxxxx")

        ttk.Label(frm, text="关键词：").grid(row=0, column=2, sticky="e", **pad)
        self.entry_kw = ttk.Entry(frm, width=20)
        self.entry_kw.grid(row=0, column=3, sticky="w", **pad)
        ttk.Label(frm, text="（留空=全部）", foreground="gray").grid(row=0, column=4, sticky="w", **pad)

        ttk.Label(frm, text="速度：").grid(row=1, column=0, sticky="e", **pad)
        self.combo_speed = ttk.Combobox(
            frm, values=list(SPEED_PRESETS.keys()), state="readonly", width=18
        )
        self.combo_speed.set("平衡（推荐）")
        self.combo_speed.grid(row=1, column=1, sticky="w", **pad)

        ttk.Label(frm, text="Cookie：").grid(row=2, column=0, sticky="ne", **pad)
        self.text_cookie = tk.Text(frm, height=4, wrap="char")
        self.text_cookie.grid(row=2, column=1, columnspan=4, sticky="ew", **pad)
        frm.columnconfigure(1, weight=1)
        frm.columnconfigure(3, weight=1)

        bar = ttk.Frame(self.root)
        bar.pack(fill="x", padx=10, pady=4)

        self.btn_start = ttk.Button(bar, text="开始抓取", command=self.start)
        self.btn_start.pack(side="left", padx=(0, 8))
        self.btn_stop = ttk.Button(bar, text="停止", command=self.stop, state="disabled")
        self.btn_stop.pack(side="left", padx=(0, 8))

        self.progress = ttk.Progressbar(bar, mode="indeterminate", length=260)
        self.progress.pack(side="left", padx=8)

        self.lbl_status = ttk.Label(bar, text="就绪")
        self.lbl_status.pack(side="left", padx=8)

        self.lbl_hit = ttk.Label(bar, text="命中：0")
        self.lbl_hit.pack(side="right", padx=8)

        frm_log = ttk.LabelFrame(self.root, text="运行日志")
        frm_log.pack(fill="both", expand=True, padx=10, pady=4)
        self.txt_log = scrolledtext.ScrolledText(frm_log, height=10, wrap="word", state="disabled")
        self.txt_log.pack(fill="both", expand=True, padx=6, pady=6)

        frm_hit = ttk.LabelFrame(self.root, text="关键词命中评论")
        frm_hit.pack(fill="both", expand=True, padx=10, pady=(4, 10))
        self.txt_hit = scrolledtext.ScrolledText(frm_hit, height=10, wrap="word", state="disabled")
        self.txt_hit.pack(fill="both", expand=True, padx=6, pady=6)

    # ---------- 线程安全更新 ----------

    def _append(self, widget, text):
        widget.configure(state="normal")
        widget.insert("end", text + "\n")
        widget.see("end")
        widget.configure(state="disabled")

    def _log(self, kind, payload):
        self.msg_queue.put((kind, payload))

    def _poll_queue(self):
        try:
            while True:
                kind, payload = self.msg_queue.get_nowait()
                if kind == "log":
                    self._append(self.txt_log, payload)
                elif kind == "hit":
                    self.hit_count += 1
                    self._append(self.txt_hit, f"{self.hit_count}. {payload}")
                    self.lbl_hit.config(text=f"命中：{self.hit_count}")
                elif kind == "progress":
                    self._update_progress(*payload)
                elif kind == "progress_max":
                    if self.progress.cget("mode") != "determinate":
                        self.progress.stop()
                        self.progress.config(mode="determinate", maximum=max(payload, 1), value=0)
                elif kind == "done":
                    self._on_done(payload)
                elif kind == "error":
                    self._on_error(payload)
        except queue.Empty:
            pass
        self.root.after(80, self._poll_queue)

    def _update_progress(self, done, total):
        if self.progress.cget("mode") != "determinate":
            return
        self.progress["maximum"] = max(total, 1)
        self.progress["value"] = done

    # ---------- 控制 ----------

    def start(self):
        if self.worker and self.worker.is_alive():
            return

        m = re.search(r"(BV[0-9A-Za-z]{10})", self.entry_bv.get().strip())
        if not m:
            messagebox.showwarning("提示", "请输入有效 BV 号")
            return
        bvid = m.group(1)

        cookie = self.text_cookie.get("1.0", "end").strip().replace("\n", "").replace("\r", "")
        if not cookie:
            messagebox.showwarning("提示", "请填写 Cookie")
            return

        keyword = self.entry_kw.get().strip()
        speed = self.combo_speed.get()

        # 重置界面
        for w in (self.txt_log, self.txt_hit):
            w.configure(state="normal")
            w.delete("1.0", "end")
            w.configure(state="disabled")
        self.hit_count = 0
        self.lbl_hit.config(text="命中：0")
        self.progress.stop()
        self.progress.config(mode="indeterminate", value=0)
        self.progress.start(12)

        self.stop_event.clear()
        self.btn_start.config(state="disabled")
        self.btn_stop.config(state="normal")
        self.lbl_status.config(text="抓取中 ...")

        crawler = Crawler(bvid, keyword, cookie, self._log, self.stop_event, speed)

        def task():
            try:
                rows, hits, path = crawler.run()
                self.msg_queue.put(("done",
                    f"完成：{len(rows)} 条评论，命中 {len(hits)} 条\nCSV：{path}"))
            except StopCrawl:
                self.msg_queue.put(("error", "已手动停止"))
            except Exception as e:
                self.msg_queue.put(("error", f"运行出错：{e}"))

        self.worker = threading.Thread(target=task, daemon=True)
        self.worker.start()

    def stop(self):
        self.stop_event.set()
        self.lbl_status.config(text="正在停止 ...")

    def _on_done(self, msg):
        self.progress.stop()
        self.btn_start.config(state="normal")
        self.btn_stop.config(state="disabled")
        self.lbl_status.config(text="完成")
        self._append(self.txt_log, "\n[+] " + msg)
        messagebox.showinfo("完成", msg)

    def _on_error(self, msg):
        self.progress.stop()
        self.btn_start.config(state="normal")
        self.btn_stop.config(state="disabled")
        self.lbl_status.config(text="已停止")
        self._append(self.txt_log, "\n[x] " + msg)
        messagebox.showerror("错误", msg)


def main():
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()