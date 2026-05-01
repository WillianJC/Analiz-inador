import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
import csv
import json
import re
import os
import datetime

try:
    from youtube_comment_downloader import YoutubeCommentDownloader, SORT_BY_POPULAR, SORT_BY_RECENT
    LIB_AVAILABLE = True
except ImportError:
    LIB_AVAILABLE = False

try:
    import requests as requests_lib
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False

try:
    from dotenv import load_dotenv
    load_dotenv()  # carga .env desde el directorio de trabajo
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False


def extract_video_id(url: str) -> str | None:
    """Extrae el ID del video de una URL de YouTube."""
    patterns = [
        r"(?:v=|\/)([0-9A-Za-z_-]{11}).*",
        r"(?:youtu\.be\/)([0-9A-Za-z_-]{11})",
        r"(?:embed\/)([0-9A-Za-z_-]{11})",
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


def extract_tiktok_video_id(url: str) -> str | None:
    """Extrae el ID del video de una URL de TikTok."""
    patterns = [
        r"tiktok\.com/@[\w.]+/video/(\d+)",
        r"tiktok\.com/video/(\d+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None  # URLs cortas se resuelven en el worker


def is_tiktok_url(url: str) -> bool:
    """Detecta si la URL pertenece a TikTok."""
    return bool(re.search(r"tiktok\.com|vm\.tiktok\.com|vt\.tiktok\.com", url, re.IGNORECASE))


def is_reddit_url(url: str) -> bool:
    """Detecta si la URL pertenece a Reddit."""
    return bool(re.search(r"reddit\.com|redd\.it", url, re.IGNORECASE))


def extract_reddit_post_info(url: str) -> tuple[str | None, str | None]:
    """Devuelve (subreddit, post_id) desde una URL de Reddit."""
    # Formato: reddit.com/r/sub/comments/ID[/...]
    match = re.search(r"reddit\.com/r/([\w]+)/comments/([A-Za-z0-9]+)", url)
    if match:
        return match.group(1), match.group(2)
    # Formato corto: redd.it/ID
    match = re.search(r"redd\.it/([A-Za-z0-9]+)", url)
    if match:
        return None, match.group(1)
    return None, None


class YouTubeScraperApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("YouTube Comment Scraper")
        self.root.geometry("1100x720")
        self.root.minsize(800, 550)

        self.comments_data: list[dict] = []
        self.scraping = False
        self._stop_event = threading.Event()
        self.platform = "youtube"
        self._db_conn_str: str | None = None
        self._db_conn_str = self._build_conn_str_from_env()

        self._apply_theme()
        self._build_ui()
        self._check_library()

    # ------------------------------------------------------------------ #
    #  Tema / Estilos                                                      #
    # ------------------------------------------------------------------ #
    def _apply_theme(self):
        style = ttk.Style(self.root)
        style.theme_use("clam")

        BG = "#1a1a2e"
        PANEL = "#16213e"
        ACCENT = "#e94560"
        FG = "#eaeaea"
        ENTRY_BG = "#0f3460"
        ROW_ODD = "#1e2a45"
        ROW_EVEN = "#16213e"
        SEL = "#e94560"

        self.root.configure(bg=BG)

        style.configure(".", background=BG, foreground=FG, font=("Segoe UI", 10))
        style.configure("TFrame", background=BG)
        style.configure("Panel.TFrame", background=PANEL)

        style.configure(
            "TLabel",
            background=BG,
            foreground=FG,
            font=("Segoe UI", 10),
        )
        style.configure("Title.TLabel", font=("Segoe UI", 18, "bold"), foreground=ACCENT)
        style.configure("Sub.TLabel", font=("Segoe UI", 9), foreground="#aaaaaa")

        style.configure(
            "Accent.TButton",
            background=ACCENT,
            foreground="#ffffff",
            font=("Segoe UI", 10, "bold"),
            borderwidth=0,
            padding=(14, 6),
        )
        style.map("Accent.TButton", background=[("active", "#c73652"), ("disabled", "#555555")])

        style.configure(
            "Secondary.TButton",
            background=ENTRY_BG,
            foreground=FG,
            font=("Segoe UI", 10),
            borderwidth=0,
            padding=(12, 6),
        )
        style.map("Secondary.TButton", background=[("active", "#1a4a80"), ("disabled", "#333333")])

        style.configure(
            "TEntry",
            fieldbackground=ENTRY_BG,
            foreground=FG,
            insertcolor=FG,
            borderwidth=0,
        )
        style.configure(
            "TCombobox",
            fieldbackground=ENTRY_BG,
            foreground=FG,
            selectbackground=SEL,
        )

        style.configure("TProgressbar", troughcolor=PANEL, background=ACCENT, borderwidth=0)

        style.configure(
            "Treeview",
            background=ROW_EVEN,
            foreground=FG,
            fieldbackground=ROW_EVEN,
            rowheight=28,
            borderwidth=0,
        )
        style.configure(
            "Treeview.Heading",
            background=PANEL,
            foreground=ACCENT,
            font=("Segoe UI", 10, "bold"),
            borderwidth=0,
        )
        style.map(
            "Treeview",
            background=[("selected", SEL)],
            foreground=[("selected", "#ffffff")],
        )

        style.configure("TScrollbar", background=PANEL, troughcolor=BG, borderwidth=0)
        style.configure("Status.TLabel", background=PANEL, foreground="#aaaaaa", padding=(8, 4))

        self._colors = {
            "bg": BG,
            "panel": PANEL,
            "accent": ACCENT,
            "fg": FG,
            "entry": ENTRY_BG,
            "row_odd": ROW_ODD,
            "row_even": ROW_EVEN,
        }

    # ------------------------------------------------------------------ #
    #  Construcción de la interfaz                                         #
    # ------------------------------------------------------------------ #
    def _build_ui(self):
        c = self._colors

        # ── Encabezado ──────────────────────────────────────────────────
        header = ttk.Frame(self.root, style="Panel.TFrame")
        header.pack(fill="x", padx=0, pady=0)

        ttk.Label(header, text="▶  YouTube Comment Scraper", style="Title.TLabel").pack(
            side="left", padx=20, pady=12
        )
        ttk.Label(
            header,
            text="Extrae y exporta comentarios de cualquier video",
            style="Sub.TLabel",
        ).pack(side="left", padx=4, pady=12)

        # ── Panel de entrada ─────────────────────────────────────────────
        input_frame = ttk.Frame(self.root)
        input_frame.pack(fill="x", padx=20, pady=(16, 4))

        ttk.Label(input_frame, text="URL del video:").grid(row=0, column=0, sticky="w", pady=4)
        self.url_var = tk.StringVar()
        url_entry = ttk.Entry(input_frame, textvariable=self.url_var, width=70, font=("Segoe UI", 11))
        url_entry.grid(row=0, column=1, sticky="ew", padx=(8, 8), pady=4)
        url_entry.bind("<Return>", lambda _: self._start_scraping())
        self.url_var.trace_add("write", self._on_url_change)

        ttk.Label(input_frame, text="Ordenar por:").grid(row=0, column=2, sticky="w", padx=(8, 0))
        self.sort_var = tk.StringVar(value="Populares")
        sort_cb = ttk.Combobox(
            input_frame,
            textvariable=self.sort_var,
            values=["Populares", "Recientes"],
            state="readonly",
            width=12,
        )
        sort_cb.grid(row=0, column=3, padx=(4, 8))

        ttk.Label(input_frame, text="Límite:").grid(row=0, column=4, sticky="w")
        self.limit_var = tk.StringVar(value="100")
        limit_entry = ttk.Entry(input_frame, textvariable=self.limit_var, width=7)
        limit_entry.grid(row=0, column=5, padx=(4, 12))

        self.scrape_btn = ttk.Button(
            input_frame,
            text="Obtener Comentarios",
            style="Accent.TButton",
            command=self._start_scraping,
        )
        self.scrape_btn.grid(row=0, column=6, padx=(0, 8))

        self.stop_btn = ttk.Button(
            input_frame,
            text="Detener",
            style="Secondary.TButton",
            command=self._stop_scraping,
            state="disabled",
        )
        self.stop_btn.grid(row=0, column=7)

        input_frame.columnconfigure(1, weight=1)

        # ── Barra de búsqueda / filtro ───────────────────────────────────
        filter_frame = ttk.Frame(self.root)
        filter_frame.pack(fill="x", padx=20, pady=(4, 0))

        ttk.Label(filter_frame, text="Filtrar:").pack(side="left")
        self.filter_var = tk.StringVar()
        self.filter_var.trace_add("write", self._apply_filter)
        filter_entry = ttk.Entry(filter_frame, textvariable=self.filter_var, width=40)
        filter_entry.pack(side="left", padx=8)

        self.count_label = ttk.Label(filter_frame, text="0 comentarios", style="Sub.TLabel")
        self.count_label.pack(side="right")
        self.platform_label = ttk.Label(filter_frame, text="● YouTube", style="Sub.TLabel")
        self.platform_label.pack(side="right", padx=(0, 12))

        # ── Barra de progreso ────────────────────────────────────────────
        self.progress = ttk.Progressbar(self.root, mode="indeterminate")
        self.progress.pack(fill="x", padx=20, pady=(8, 0))

        # ── Tabla de comentarios ─────────────────────────────────────────
        tree_frame = ttk.Frame(self.root)
        tree_frame.pack(fill="both", expand=True, padx=20, pady=10)

        columns = ("author", "comment", "likes", "replies", "time")
        self.tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="browse")

        self.tree.heading("author", text="Autor", command=lambda: self._sort_col("author"))
        self.tree.heading("comment", text="Comentario", command=lambda: self._sort_col("comment"))
        self.tree.heading("likes", text="Likes", command=lambda: self._sort_col("likes"))
        self.tree.heading("replies", text="Respuestas", command=lambda: self._sort_col("replies"))
        self.tree.heading("time", text="Tiempo", command=lambda: self._sort_col("time"))

        self.tree.column("author", width=160, minwidth=100)
        self.tree.column("comment", width=560, minwidth=200)
        self.tree.column("likes", width=70, minwidth=50, anchor="center")
        self.tree.column("replies", width=90, minwidth=60, anchor="center")
        self.tree.column("time", width=130, minwidth=90, anchor="center")

        # Colores alternos de fila
        self.tree.tag_configure("odd", background=c["row_odd"])
        self.tree.tag_configure("even", background=c["row_even"])

        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)

        # Doble clic → ver comentario completo
        self.tree.bind("<Double-1>", self._show_full_comment)

        # ── Panel inferior: exportar ─────────────────────────────────────
        bottom = ttk.Frame(self.root, style="Panel.TFrame")
        bottom.pack(fill="x", padx=0, pady=0)

        ttk.Button(
            bottom,
            text="Exportar CSV",
            style="Secondary.TButton",
            command=lambda: self._export("csv"),
        ).pack(side="left", padx=12, pady=8)
        ttk.Button(
            bottom,
            text="Exportar JSON",
            style="Secondary.TButton",
            command=lambda: self._export("json"),
        ).pack(side="left", padx=4, pady=8)
        ttk.Button(
            bottom,
            text="Limpiar",
            style="Secondary.TButton",
            command=self._clear,
        ).pack(side="left", padx=4, pady=8)
        ttk.Button(
            bottom,
            text="Configurar BD",
            style="Secondary.TButton",
            command=self._configure_db,
        ).pack(side="left", padx=(16, 4), pady=8)
        ttk.Button(
            bottom,
            text="Guardar en Azure SQL",
            style="Accent.TButton",
            command=self._save_to_sql,
        ).pack(side="left", padx=4, pady=8)

        self.status_var = tk.StringVar(value="Listo. Introduce una URL y presiona 'Obtener Comentarios'.")
        ttk.Label(bottom, textvariable=self.status_var, style="Status.TLabel").pack(
            side="right", padx=16
        )

    # ------------------------------------------------------------------ #
    #  Detección de plataforma                                            #
    # ------------------------------------------------------------------ #
    def _on_url_change(self, *_):
        url = self.url_var.get()
        if is_tiktok_url(url):
            self.platform = "tiktok"
            self.platform_label.configure(text="● TikTok", foreground="#ff0050")
        elif is_reddit_url(url):
            self.platform = "reddit"
            self.platform_label.configure(text="● Reddit", foreground="#ff6314")
        else:
            self.platform = "youtube"
            self.platform_label.configure(text="● YouTube", foreground="#ff4444")

    # ------------------------------------------------------------------ #
    #  Verificación de librería                                            #
    # ------------------------------------------------------------------ #
    def _check_library(self):
        if not LIB_AVAILABLE:
            messagebox.showwarning(
                "Dependencia faltante",
                "La librería 'youtube-comment-downloader' no está instalada.\n\n"
                "Ejecuta en la terminal:\n"
                "  pip install youtube-comment-downloader\n\n"
                "Luego reinicia la aplicación.",
            )
            self.scrape_btn.configure(state="disabled")
        if not REQUESTS_AVAILABLE:
            messagebox.showwarning(
                "Dependencia faltante",
                "La librería 'requests' no está instalada (necesaria para TikTok).\n\n"
                "Ejecuta en la terminal:\n"
                "  pip install requests\n\n"
                "Luego reinicia la aplicación.",
            )

    # ------------------------------------------------------------------ #
    #  Scraping                                                            #
    # ------------------------------------------------------------------ #
    def _start_scraping(self):
        if self.scraping:
            return

        url = self.url_var.get().strip()
        if not url:
            messagebox.showwarning("URL vacía", "Por favor introduce la URL del video.")
            return

        if self.platform == "tiktok":
            if not REQUESTS_AVAILABLE:
                messagebox.showerror(
                    "Dependencia faltante",
                    "La librería 'requests' es necesaria para TikTok.\n\nEjecuta: pip install requests",
                )
                return
            if not is_tiktok_url(url):
                messagebox.showerror("URL inválida", "No se reconoce como URL de TikTok.")
                return
        elif self.platform == "reddit":
            if not REQUESTS_AVAILABLE:
                messagebox.showerror(
                    "Dependencia faltante",
                    "La librería 'requests' es necesaria para Reddit.\n\nEjecuta: pip install requests",
                )
                return
            _, post_id = extract_reddit_post_info(url)
            if not post_id:
                messagebox.showerror("URL inválida", "No se pudo extraer el ID del post de Reddit.\nVerifica la URL.")
                return
        else:
            video_id = extract_video_id(url)
            if not video_id:
                messagebox.showerror("URL inválida", "No se pudo extraer el ID del video.\nVerifica la URL.")
                return

        try:
            limit = int(self.limit_var.get())
            if limit <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror("Límite inválido", "El límite debe ser un número entero positivo.")
            return

        self._clear(confirm=False)
        self.scraping = True
        self._stop_event.clear()
        self.scrape_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.progress.start(10)

        if self.platform == "tiktok":
            self._set_status("Obteniendo comentarios de TikTok…")
            thread = threading.Thread(
                target=self._scrape_tiktok_worker,
                args=(url, limit),
                daemon=True,
            )
        elif self.platform == "reddit":
            _, post_id = extract_reddit_post_info(url)
            self._set_status(f"Obteniendo comentarios de Reddit: {post_id} …")
            thread = threading.Thread(
                target=self._scrape_reddit_worker,
                args=(url, limit),
                daemon=True,
            )
        else:
            self._set_status(f"Obteniendo comentarios de: {extract_video_id(url)} …")
            sort_mode = SORT_BY_POPULAR if self.sort_var.get() == "Populares" else SORT_BY_RECENT
            thread = threading.Thread(
                target=self._scrape_worker,
                args=(url, sort_mode, limit),
                daemon=True,
            )
        thread.start()

    def _scrape_worker(self, url: str, sort_mode: int, limit: int):
        try:
            downloader = YoutubeCommentDownloader()
            generator = downloader.get_comments_from_url(url, sort_by=sort_mode)
            count = 0
            for comment in generator:
                if self._stop_event.is_set():
                    break
                self.root.after(0, self._add_comment_row, comment)
                count += 1
                if count % 10 == 0:
                    self.root.after(0, self._set_status, f"Descargados {count} comentarios…")
                if count >= limit:
                    break
            self.root.after(0, self._scrape_done, count, False)
        except Exception as exc:
            self.root.after(0, self._scrape_error, str(exc))

    def _add_comment_row(self, comment: dict):
        self.comments_data.append(comment)
        idx = len(self.comments_data)
        tag = "odd" if idx % 2 else "even"
        author = comment.get("author", "")
        text = comment.get("text", "").replace("\n", " ")
        likes = comment.get("votes", 0) or 0
        replies = comment.get("reply_count", 0) or 0
        time_str = comment.get("time", "")
        self.tree.insert("", "end", values=(author, text, likes, replies, time_str), tags=(tag,))
        self._update_count()

    def _scrape_done(self, count: int, stopped: bool):
        self.scraping = False
        self.progress.stop()
        self.scrape_btn.configure(state="normal")
        self.stop_btn.configure(state="disabled")
        if stopped:
            self._set_status(f"Detenido. {count} comentarios obtenidos.")
        else:
            self._set_status(f"Completado. {count} comentarios obtenidos.")

    def _scrape_error(self, msg: str):
        self.scraping = False
        self.progress.stop()
        self.scrape_btn.configure(state="normal")
        self.stop_btn.configure(state="disabled")
        self._set_status("Error al obtener comentarios.")
        messagebox.showerror("Error de scraping", f"Ocurrió un error:\n{msg}")

    def _stop_scraping(self):
        if self.scraping:
            self._stop_event.set()
            count = len(self.comments_data)
            self._scrape_done(count, stopped=True)

    # ------------------------------------------------------------------ #
    #  Scraping TikTok                                                     #
    # ------------------------------------------------------------------ #
    def _scrape_tiktok_worker(self, url: str, limit: int):
        try:
            # Resolver URL corta si es necesario
            video_id = extract_tiktok_video_id(url)
            if not video_id:
                headers_base = {
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"
                    )
                }
                resp = requests_lib.get(url, allow_redirects=True, timeout=10, headers=headers_base)
                video_id = extract_tiktok_video_id(resp.url)
                if not video_id:
                    raise ValueError(
                        f"No se pudo extraer el ID del video de TikTok.\nURL final: {resp.url}"
                    )

            session = requests_lib.Session()
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Referer": "https://www.tiktok.com/",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
            }

            # Obtener cookies iniciales
            session.get("https://www.tiktok.com/", headers=headers, timeout=15)

            cursor = 0
            count = 0

            while count < limit and not self._stop_event.is_set():
                resp = session.get(
                    "https://www.tiktok.com/api/comment/list/",
                    params={
                        "aweme_id": video_id,
                        "cursor": cursor,
                        "count": 20,
                        "aid": 1988,
                    },
                    headers=headers,
                    timeout=15,
                )

                if resp.status_code != 200:
                    raise Exception(f"Error HTTP {resp.status_code} al acceder a la API de TikTok.")

                data = resp.json()

                if data.get("status_code") not in (0, None):
                    raise Exception(
                        f"Error de API TikTok: {data.get('status_msg', 'Respuesta inesperada')}"
                    )

                comments_list = data.get("comments") or []
                if not comments_list:
                    break

                for comment in comments_list:
                    if self._stop_event.is_set() or count >= limit:
                        break
                    ts = comment.get("create_time", 0)
                    time_str = (
                        datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
                        if ts
                        else ""
                    )
                    normalized = {
                        "author": comment.get("user", {}).get("nickname", ""),
                        "text": comment.get("text", ""),
                        "votes": comment.get("digg_count", 0),
                        "reply_count": comment.get("reply_comment_total", 0),
                        "time": time_str,
                        "cid": comment.get("cid", ""),
                    }
                    self.root.after(0, self._add_comment_row, normalized)
                    count += 1
                    if count % 10 == 0:
                        self.root.after(0, self._set_status, f"Descargados {count} comentarios…")

                if not data.get("has_more", False):
                    break
                cursor = int(data.get("cursor", cursor + 20))

            self.root.after(0, self._scrape_done, count, False)
        except Exception as exc:
            self.root.after(0, self._scrape_error, str(exc))

    # ------------------------------------------------------------------ #
    #  Scraping Reddit                                                     #
    # ------------------------------------------------------------------ #
    def _scrape_reddit_worker(self, url: str, limit: int):
        try:
            subreddit, post_id = extract_reddit_post_info(url)

            # Si es URL corta (redd.it), resolver para obtener el subreddit
            if not subreddit:
                headers_base = {
                    "User-Agent": "PitonScraper/1.0 (comment scraper)",
                    "Accept": "application/json",
                }
                resp = requests_lib.get(url, allow_redirects=True, timeout=10, headers=headers_base)
                subreddit, post_id = extract_reddit_post_info(resp.url)
                if not post_id:
                    raise ValueError(f"No se pudo resolver la URL de Reddit.\nURL final: {resp.url}")

            headers = {
                "User-Agent": "PitonScraper/1.0 (comment scraper)",
                "Accept": "application/json",
            }

            # La API JSON de Reddit devuelve todos los comentarios de nivel superior.
            # Para comentarios anidados usamos `after` con paginación de la API.
            api_url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json"

            params = {"limit": min(limit, 500), "depth": 10, "showmore": 0}
            resp = requests_lib.get(api_url, params=params, headers=headers, timeout=20)

            if resp.status_code == 404:
                raise Exception("Post no encontrado (404). Verifica que el post sea público.")
            if resp.status_code == 403:
                raise Exception("Acceso denegado (403). El subreddit puede ser privado.")
            if resp.status_code != 200:
                raise Exception(f"Error HTTP {resp.status_code} al acceder a Reddit.")

            data = resp.json()
            # data[0] = post info, data[1] = comentarios
            if not isinstance(data, list) or len(data) < 2:
                raise Exception("Respuesta inesperada de la API de Reddit.")

            count = 0

            def process_comments(listing: dict):
                nonlocal count
                children = listing.get("data", {}).get("children", [])
                for child in children:
                    if self._stop_event.is_set() or count >= limit:
                        return
                    kind = child.get("kind", "")
                    if kind == "more":
                        continue  # ignorar "load more" links
                    cdata = child.get("data", {})
                    author = cdata.get("author", "[eliminado]")
                    body = cdata.get("body", "")
                    if not body or body in ("[deleted]", "[removed]"):
                        body = "[comentario eliminado]"
                    score = cdata.get("score", 0) or 0
                    created = cdata.get("created_utc", 0)
                    time_str = (
                        datetime.datetime.fromtimestamp(created).strftime("%Y-%m-%d %H:%M")
                        if created
                        else ""
                    )
                    num_replies = len(
                        cdata.get("replies", {}).get("data", {}).get("children", [])
                        if isinstance(cdata.get("replies"), dict)
                        else []
                    )
                    normalized = {
                        "author": author,
                        "text": body,
                        "votes": score,
                        "reply_count": num_replies,
                        "time": time_str,
                        "cid": cdata.get("id", ""),
                    }
                    self.root.after(0, self._add_comment_row, normalized)
                    count += 1
                    if count % 10 == 0:
                        self.root.after(0, self._set_status, f"Descargados {count} comentarios…")
                    # Procesar respuestas anidadas recursivamente
                    if isinstance(cdata.get("replies"), dict) and count < limit:
                        process_comments(cdata["replies"])

            process_comments(data[1])
            self.root.after(0, self._scrape_done, count, False)
        except Exception as exc:
            self.root.after(0, self._scrape_error, str(exc))

    # ------------------------------------------------------------------ #
    #  Filtro                                                              #
    # ------------------------------------------------------------------ #
    def _apply_filter(self, *_):
        query = self.filter_var.get().lower()
        for item in self.tree.get_children():
            self.tree.delete(item)

        filtered = (
            [c for c in self.comments_data if query in c.get("text", "").lower()
             or query in c.get("author", "").lower()]
            if query
            else self.comments_data
        )

        for idx, comment in enumerate(filtered, start=1):
            tag = "odd" if idx % 2 else "even"
            text = comment.get("text", "").replace("\n", " ")
            self.tree.insert(
                "",
                "end",
                values=(
                    comment.get("author", ""),
                    text,
                    comment.get("votes", 0) or 0,
                    comment.get("reply_count", 0) or 0,
                    comment.get("time", ""),
                ),
                tags=(tag,),
            )
        shown = len(filtered)
        total = len(self.comments_data)
        self.count_label.configure(
            text=f"{shown} de {total} comentarios" if query else f"{total} comentarios"
        )

    # ------------------------------------------------------------------ #
    #  Ordenar columnas                                                    #
    # ------------------------------------------------------------------ #
    _sort_reverse: dict = {}

    def _sort_col(self, col: str):
        reverse = self._sort_reverse.get(col, False)
        key_map = {
            "author": lambda c: c.get("author", "").lower(),
            "comment": lambda c: c.get("text", "").lower(),
            "likes": lambda c: int(c.get("votes", 0) or 0),
            "replies": lambda c: int(c.get("reply_count", 0) or 0),
            "time": lambda c: c.get("time", ""),
        }
        self.comments_data.sort(key=key_map.get(col, lambda c: ""), reverse=reverse)
        self._sort_reverse[col] = not reverse
        self._apply_filter()

    # ------------------------------------------------------------------ #
    #  Ver comentario completo                                             #
    # ------------------------------------------------------------------ #
    def _show_full_comment(self, event):
        selection = self.tree.selection()
        if not selection:
            return
        values = self.tree.item(selection[0], "values")
        if not values:
            return

        # Busca el comentario completo en los datos
        author = values[0]
        partial_text = values[1]
        full_text = partial_text
        for c in self.comments_data:
            if c.get("author", "") == author and c.get("text", "").replace("\n", " ").startswith(partial_text[:40]):
                full_text = c.get("text", "")
                break

        win = tk.Toplevel(self.root)
        win.title(f"Comentario de {author}")
        win.geometry("560x320")
        win.configure(bg=self._colors["bg"])
        win.resizable(True, True)

        ttk.Label(win, text=f"Autor: {author}", font=("Segoe UI", 11, "bold")).pack(anchor="w", padx=14, pady=(12, 2))
        ttk.Label(win, text=f"Likes: {values[2]}   Respuestas: {values[3]}   Tiempo: {values[4]}",
                  style="Sub.TLabel").pack(anchor="w", padx=14, pady=(0, 8))

        txt = tk.Text(
            win,
            wrap="word",
            bg=self._colors["entry"],
            fg=self._colors["fg"],
            font=("Segoe UI", 11),
            borderwidth=0,
            padx=10,
            pady=10,
        )
        txt.insert("1.0", full_text)
        txt.configure(state="disabled")
        txt.pack(fill="both", expand=True, padx=14, pady=(0, 14))

        ttk.Button(win, text="Cerrar", style="Accent.TButton", command=win.destroy).pack(pady=(0, 10))

    # ------------------------------------------------------------------ #
    #  Azure SQL – Conexión desde .env                                    #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _build_conn_str_from_env() -> str | None:
        """Construye la cadena de conexión leyendo variables del .env."""
        import os as _os
        server   = _os.getenv("AZURE_SQL_SERVER", "").strip()
        database = _os.getenv("AZURE_SQL_DATABASE", "").strip()
        username = _os.getenv("AZURE_SQL_USER", "").strip()
        password = _os.getenv("AZURE_SQL_PASSWORD", "")
        port     = _os.getenv("AZURE_SQL_PORT", "1433").strip() or "1433"

        if not server or not database or not username:
            return None

        return (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )

    # ------------------------------------------------------------------ #
    #  Azure SQL – Configuración                                           #
    # ------------------------------------------------------------------ #
    def _configure_db(self):
        """Abre un diálogo para introducir los datos de conexión a Azure SQL."""
        win = tk.Toplevel(self.root)
        win.title("Configurar conexión Azure SQL")
        win.geometry("540x380")
        win.resizable(False, False)
        win.configure(bg=self._colors["bg"])
        win.grab_set()

        c = self._colors
        pad = {"padx": 12, "pady": 5}

        ttk.Label(win, text="Conexión a Azure SQL Database", style="Title.TLabel").pack(
            anchor="w", padx=14, pady=(14, 4)
        )
        ttk.Label(
            win,
            text="Los datos se guardan solo en memoria hasta cerrar la app.",
            style="Sub.TLabel",
        ).pack(anchor="w", padx=14, pady=(0, 10))

        fields_frame = ttk.Frame(win)
        fields_frame.pack(fill="x", padx=14)

        labels = ["Servidor (FQDN):", "Base de datos:", "Usuario:", "Contraseña:", "Puerto:"]
        keys = ["server", "database", "username", "password", "port"]
        defaults = {"port": "1433"}
        shows = {"password": "*"}

        # Prioridad: .env > cadena ya guardada > vacío
        import os as _os
        env_vals = {
            "server":   _os.getenv("AZURE_SQL_SERVER", ""),
            "database": _os.getenv("AZURE_SQL_DATABASE", ""),
            "username": _os.getenv("AZURE_SQL_USER", ""),
            "password": _os.getenv("AZURE_SQL_PASSWORD", ""),
            "port":     _os.getenv("AZURE_SQL_PORT", "1433"),
        }
        current: dict[str, str] = {}
        if self._db_conn_str:
            for part in self._db_conn_str.split(";"):
                kv = part.split("=", 1)
                if len(kv) == 2:
                    current[kv[0].strip().lower()] = kv[1].strip()

        entries: dict[str, ttk.Entry] = {}
        for i, (lbl, key) in enumerate(zip(labels, keys)):
            ttk.Label(fields_frame, text=lbl).grid(row=i, column=0, sticky="w", **pad)
            # Prioridad: variable .env → cadena guardada → default
            initial = env_vals.get(key) or current.get(key, defaults.get(key, ""))
            var = tk.StringVar(value=initial)
            ent = ttk.Entry(
                fields_frame,
                textvariable=var,
                width=36,
                show=shows.get(key, ""),
            )
            ent.grid(row=i, column=1, sticky="ew", **pad)
            entries[key] = ent
        fields_frame.columnconfigure(1, weight=1)

        # Indicador de si .env fue encontrado
        import os as _os
        env_file = _os.path.join(_os.getcwd(), ".env")
        env_status = f"📄 .env cargado: {env_file}" if _os.path.exists(env_file) else "⚠ No se encontró archivo .env"
        ttk.Label(win, text=env_status, style="Sub.TLabel").pack(anchor="w", padx=14, pady=(4, 0))

        def _apply():
            server   = entries["server"].get().strip()
            database = entries["database"].get().strip()
            username = entries["username"].get().strip()
            password = entries["password"].get()
            port     = entries["port"].get().strip() or "1433"

            if not server or not database or not username:
                messagebox.showwarning(
                    "Campos requeridos",
                    "Servidor, base de datos y usuario son obligatorios.",
                    parent=win,
                )
                return

            self._db_conn_str = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={server},{port};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
                f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
            )
            self._set_status("Configuración de BD guardada.")
            win.destroy()

        def _test():
            _apply()
            if not self._db_conn_str:
                return
            try:
                conn = pyodbc.connect(self._db_conn_str, timeout=10)
                conn.close()
                messagebox.showinfo("Conexión exitosa", "La conexión a Azure SQL fue exitosa.", parent=win)
            except Exception as exc:
                messagebox.showerror("Error de conexión", str(exc), parent=win)

        def _reload_env():
            if DOTENV_AVAILABLE:
                from dotenv import load_dotenv
                load_dotenv(override=True)
            import os as _os
            entries["server"].delete(0, "end");   entries["server"].insert(0,   _os.getenv("AZURE_SQL_SERVER",   ""))
            entries["database"].delete(0, "end"); entries["database"].insert(0, _os.getenv("AZURE_SQL_DATABASE", ""))
            entries["username"].delete(0, "end"); entries["username"].insert(0, _os.getenv("AZURE_SQL_USER",     ""))
            entries["password"].delete(0, "end"); entries["password"].insert(0, _os.getenv("AZURE_SQL_PASSWORD", ""))
            entries["port"].delete(0, "end");     entries["port"].insert(0,     _os.getenv("AZURE_SQL_PORT",     "1433"))

        btn_frame = ttk.Frame(win)
        btn_frame.pack(fill="x", padx=14, pady=12)
        ttk.Button(btn_frame, text="Probar conexión", style="Secondary.TButton", command=_test).pack(side="left")
        ttk.Button(btn_frame, text="Recargar .env",   style="Secondary.TButton", command=_reload_env).pack(side="left", padx=(8, 0))
        ttk.Button(btn_frame, text="Guardar",  style="Accent.TButton",     command=_apply).pack(side="right")
        ttk.Button(btn_frame, text="Cancelar", style="Secondary.TButton",  command=win.destroy).pack(side="right", padx=(0, 8))

    # ------------------------------------------------------------------ #
    #  Azure SQL – Guardar comentarios                                     #
    # ------------------------------------------------------------------ #
    def _save_to_sql(self):
        if not PYODBC_AVAILABLE:
            messagebox.showerror(
                "Dependencia faltante",
                "La librería 'pyodbc' no está instalada.\n\nEjecuta: pip install pyodbc",
            )
            return

        if not self.comments_data:
            messagebox.showinfo("Sin datos", "No hay comentarios para guardar.")
            return

        if not self._db_conn_str:
            self._configure_db()
            if not self._db_conn_str:
                return

        platform_map = {"youtube": "YouTube", "tiktok": "TikTok", "reddit": "Reddit"}
        platform_id_map = {"youtube": 1, "tiktok": 2, "reddit": 3}
        platform_name = platform_map.get(self.platform, "YouTube")
        platform_id   = platform_id_map.get(self.platform, 1)

        url       = self.url_var.get().strip()
        sort_mode = self.sort_var.get().lower() if self.platform == "youtube" else None
        limit_val = int(self.limit_var.get()) if self.limit_var.get().isdigit() else None

        # Determinar source_id y subreddit
        if self.platform == "youtube":
            source_id = extract_video_id(url)
            subreddit = None
        elif self.platform == "tiktok":
            source_id = extract_tiktok_video_id(url)
            subreddit = None
        else:
            subreddit, source_id = extract_reddit_post_info(url)

        json_data = json.dumps(self.comments_data, ensure_ascii=False)

        self._set_status("Guardando en Azure SQL…")
        self.scrape_btn.configure(state="disabled")

        def _worker():
            try:
                conn = pyodbc.connect(self._db_conn_str, timeout=30)
                conn.autocommit = False
                cursor = conn.cursor()

                # 1. Crear sesión
                cursor.execute(
                    """
                    DECLARE @sid INT;
                    EXEC dbo.usp_begin_session
                        @platform_name  = ?,
                        @source_url     = ?,
                        @source_id      = ?,
                        @subreddit      = ?,
                        @sort_mode      = ?,
                        @comments_limit = ?,
                        @session_id     = @sid OUTPUT;
                    SELECT @sid;
                    """,
                    platform_name,
                    url,
                    source_id,
                    subreddit,
                    sort_mode,
                    limit_val,
                )
                row = cursor.fetchone()
                session_id = row[0] if row else None

                if session_id is None:
                    raise Exception("No se pudo crear la sesión en la base de datos.")

                # 2. Insertar comentarios en lote vía JSON
                cursor.execute(
                    "EXEC dbo.usp_insert_comments_json @session_id=?, @platform_id=?, @json_data=?",
                    session_id,
                    platform_id,
                    json_data,
                )
                conn.commit()
                cursor.close()
                conn.close()

                total = len(self.comments_data)
                self.root.after(
                    0,
                    lambda: (
                        self._set_status(f"Guardado en Azure SQL. Sesión #{session_id} — {total} comentarios."),
                        self.scrape_btn.configure(state="normal"),
                        messagebox.showinfo(
                            "Guardado exitoso",
                            f"{total} comentarios guardados en Azure SQL.\nSesión ID: {session_id}",
                        ),
                    ),
                )
            except Exception as exc:
                self.root.after(
                    0,
                    lambda e=str(exc): (
                        self._set_status("Error al guardar en Azure SQL."),
                        self.scrape_btn.configure(state="normal"),
                        messagebox.showerror("Error de base de datos", e),
                    ),
                )

        threading.Thread(target=_worker, daemon=True).start()

    # ------------------------------------------------------------------ #
    #  Exportar                                                            #
    # ------------------------------------------------------------------ #
    def _export(self, fmt: str):
        if not self.comments_data:
            messagebox.showinfo("Sin datos", "No hay comentarios para exportar.")
            return

        filetypes = [("CSV files", "*.csv")] if fmt == "csv" else [("JSON files", "*.json")]
        default_ext = f".{fmt}"
        path = filedialog.asksaveasfilename(
            defaultextension=default_ext,
            filetypes=filetypes,
            initialfile=f"comentarios_youtube{default_ext}",
        )
        if not path:
            return

        try:
            if fmt == "csv":
                with open(path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(
                        f,
                        fieldnames=["author", "text", "votes", "reply_count", "time", "cid"],
                        extrasaction="ignore",
                    )
                    writer.writeheader()
                    writer.writerows(self.comments_data)
            else:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(self.comments_data, f, ensure_ascii=False, indent=2)

            self._set_status(f"Exportado: {os.path.basename(path)}")
            messagebox.showinfo("Exportación exitosa", f"Archivo guardado en:\n{path}")
        except Exception as exc:
            messagebox.showerror("Error al exportar", str(exc))

    # ------------------------------------------------------------------ #
    #  Limpiar                                                             #
    # ------------------------------------------------------------------ #
    def _clear(self, confirm: bool = True):
        if confirm and self.comments_data:
            if not messagebox.askyesno("Limpiar", "¿Deseas borrar todos los comentarios cargados?"):
                return
        self.comments_data.clear()
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.filter_var.set("")
        self._update_count()
        self._set_status("Listo.")

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #
    def _set_status(self, text: str):
        self.status_var.set(text)

    def _update_count(self):
        self.count_label.configure(text=f"{len(self.comments_data)} comentarios")


# ──────────────────────────────────────────────────────────────────────── #
if __name__ == "__main__":
    root = tk.Tk()
    app = YouTubeScraperApp(root)
    root.mainloop()
