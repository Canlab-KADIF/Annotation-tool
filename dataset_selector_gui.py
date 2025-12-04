import tkinter as tk
from tkinter import ttk, messagebox
import requests
import webbrowser
import json

# Configuration
BASE_URL = "http://11.11.50.39:8190"
LOGIN_URL = f"{BASE_URL}/api/user/login"
DATASET_LIST_URL = f"{BASE_URL}/api/dataset/findByPage"
DEFAULT_USERNAME = "admin@example.com"
DEFAULT_PASSWORD = "Password123"

class DatasetSelectorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Xtreme1 Dataset Selector")
        self.root.geometry("600x700")
        
        # Configure Styles
        self.style = ttk.Style()
        self.style.theme_use('clam')  # 'clam' usually looks better than default on Linux
        
        # Colors
        self.bg_color = "#f0f2f5"
        self.primary_color = "#1890ff"
        self.text_color = "#333333"
        
        self.root.configure(bg=self.bg_color)
        self.style.configure("TFrame", background=self.bg_color)
        self.style.configure("TLabel", background=self.bg_color, foreground=self.text_color, font=("Segoe UI", 10))
        self.style.configure("Header.TLabel", font=("Segoe UI", 18, "bold"), foreground="#001529")
        self.style.configure("TButton", font=("Segoe UI", 10), padding=6)
        self.style.map("TButton", background=[('active', '#40a9ff')])

        self.token = None
        self.datasets = []
        self.filtered_datasets = []

        self.create_widgets()
        self.login_and_fetch()

    def create_widgets(self):
        # Main Container with padding
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Header
        header_frame = ttk.Frame(main_frame)
        header_frame.pack(fill=tk.X, pady=(0, 20))
        
        title_label = ttk.Label(header_frame, text="Dataset Selector", style="Header.TLabel")
        title_label.pack(side=tk.LEFT)
        
        subtitle_label = ttk.Label(header_frame, text="Select datasets to view in Xtreme1", font=("Segoe UI", 10, "italic"), foreground="#666")
        subtitle_label.pack(side=tk.LEFT, padx=(10, 0), pady=(8, 0))

        # Search Bar
        search_frame = ttk.Frame(main_frame)
        search_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Label(search_frame, text="Search:").pack(side=tk.LEFT, padx=(0, 5))
        self.search_var = tk.StringVar()
        self.search_var.trace("w", self.filter_list)
        search_entry = ttk.Entry(search_frame, textvariable=self.search_var, font=("Segoe UI", 10))
        search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Listbox Area
        list_frame = ttk.Frame(main_frame)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 20))

        # Scrollbar
        scrollbar = ttk.Scrollbar(list_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Listbox
        self.listbox = tk.Listbox(
            list_frame, 
            selectmode=tk.MULTIPLE, 
            yscrollcommand=scrollbar.set, 
            font=("Segoe UI", 11),
            bg="white",
            fg="#333",
            selectbackground="#e6f7ff",
            selectforeground="#1890ff",
            relief=tk.FLAT,
            borderwidth=1,
            highlightthickness=1,
            highlightcolor="#d9d9d9"
        )
        self.listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.listbox.yview)

        # Buttons
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X)

        refresh_btn = ttk.Button(btn_frame, text="↻ Refresh", command=self.login_and_fetch)
        refresh_btn.pack(side=tk.LEFT)

        generate_btn = ttk.Button(btn_frame, text="Open Filtered View ➔", command=self.generate_and_open_url)
        generate_btn.pack(side=tk.RIGHT)

        # Status Bar
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        status_bar = ttk.Label(self.root, textvariable=self.status_var, relief=tk.FLAT, anchor=tk.W, padding=(10, 5), background="#e6e6e6", font=("Segoe UI", 9))
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    def login_and_fetch(self):
        self.status_var.set("Logging in...")
        self.root.update_idletasks()
        
        try:
            # Login
            payload = {"username": DEFAULT_USERNAME, "password": DEFAULT_PASSWORD}
            response = requests.post(LOGIN_URL, json=payload)
            response.raise_for_status()
            data = response.json()
            
            if data.get("code") != "OK":
                raise Exception(f"Login failed: {data.get('message')}")
            
            self.token = data["data"]["token"]
            self.status_var.set("Fetching datasets...")
            self.root.update_idletasks()

            # Fetch Datasets
            headers = {"Authorization": f"Bearer {self.token}"}
            params = {"pageNo": 1, "pageSize": 100} 
            response = requests.get(DATASET_LIST_URL, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get("code") != "OK":
                raise Exception(f"Fetch failed: {data.get('message')}")

            self.datasets = data["data"]["list"]
            self.filter_list() # Populate list
            self.status_var.set(f"Ready - {len(self.datasets)} datasets loaded")

        except Exception as e:
            messagebox.showerror("Error", str(e))
            self.status_var.set("Error occurred")

    def filter_list(self, *args):
        search_term = self.search_var.get().lower()
        self.filtered_datasets = [d for d in self.datasets if search_term in d["name"].lower()]
        
        self.listbox.delete(0, tk.END)
        for dataset in self.filtered_datasets:
            self.listbox.insert(tk.END, f" {dataset['name']}") # Add space for padding effect

    def generate_and_open_url(self):
        selected_indices = self.listbox.curselection()
        if not selected_indices:
            messagebox.showwarning("Selection Required", "Please select at least one dataset to view.")
            return

        selected_names = [self.filtered_datasets[i]["name"] for i in selected_indices]
        names_param = ",".join(selected_names)
        
        url = f"{BASE_URL}/#/datasets/list?datasetNames={names_param}&token={self.token}"
        
        print(f"Generated URL: {url}")
        self.status_var.set("Opening browser...")
        self.root.update_idletasks()
        
        webbrowser.open(url)
        self.status_var.set("Opened in browser")

if __name__ == "__main__":
    root = tk.Tk()
    # Try to set icon if available, otherwise skip
    # root.iconbitmap('icon.ico') 
    app = DatasetSelectorApp(root)
    root.mainloop()
