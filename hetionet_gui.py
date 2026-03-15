#!/usr/bin/env python

import contextlib
from io import StringIO
import tkinter as tk
from tkinter import ttk, messagebox

from hetionet_cli import Neo4jBackend, MongoBackend


class HetioNetGUI(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("HetioNet Project Client")
        self.geometry("900x600")

        self.backend_var = tk.StringVar(value="neo4j")

        self._build_widgets()

    def _build_widgets(self) -> None:
        top_frame = ttk.Frame(self, padding=10)
        top_frame.pack(side=tk.TOP, fill=tk.X)

        ttk.Label(top_frame, text="Disease ID:").grid(row=0, column=0, sticky=tk.W)
        self.disease_entry = ttk.Entry(top_frame, width=40)
        self.disease_entry.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)

        # Backend selection
        backend_frame = ttk.LabelFrame(top_frame, text="Backend", padding=5)
        backend_frame.grid(row=0, column=2, padx=10, pady=5, sticky=tk.W)

        ttk.Radiobutton(
            backend_frame, text="Neo4j (Graph)", variable=self.backend_var, value="neo4j"
        ).pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(
            backend_frame,
            text="MongoDB (Document)",
            variable=self.backend_var,
            value="mongo",
        ).pack(side=tk.LEFT, padx=5)

        # Buttons
        btn_frame = ttk.Frame(self, padding=10)
        btn_frame.pack(side=tk.TOP, fill=tk.X)

        self.btn_q1 = ttk.Button(btn_frame, text="Run Query 1", command=self.run_query1)
        self.btn_q1.pack(side=tk.LEFT, padx=5)

        self.btn_q2 = ttk.Button(
            btn_frame,
            text="Run Query 2 (Neo4j only)",
            command=self.run_query2,
        )
        self.btn_q2.pack(side=tk.LEFT, padx=5)

        # Output area
        output_frame = ttk.Frame(self, padding=10)
        output_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        self.output_text = tk.Text(output_frame, wrap=tk.WORD)
        self.output_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(
            output_frame, orient=tk.VERTICAL, command=self.output_text.yview
        )
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.output_text.configure(yscrollcommand=scrollbar.set)

        # Hint at bottom
        hint = ttk.Label(
            self,
            text=(
                "Environment variables used:\n"
                "  Neo4j -> NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD\n"
                "  MongoDB -> MONGODB_URI, MONGODB_DB"
            ),
            foreground="gray",
        )
        hint.pack(side=tk.BOTTOM, fill=tk.X, padx=10, pady=5)

    def _get_disease_id(self) -> str:
        disease_id = self.disease_entry.get().strip()
        if not disease_id:
            messagebox.showerror("Error", "Please enter a disease id, e.g. Disease::DOID:263")
            return ""
        return disease_id

    def _display_output(self, text: str) -> None:
        self.output_text.delete("1.0", tk.END)
        self.output_text.insert(tk.END, text)

    def run_query1(self) -> None:
        disease_id = self._get_disease_id()
        if not disease_id:
            return

        backend_choice = self.backend_var.get()

        if backend_choice == "neo4j":
            messagebox.showinfo(
                "Info",
                "Query 1 is implemented using MongoDB only.\n"
                "Please select 'MongoDB (Document)' above.",
            )
            return

        buf = StringIO()
        try:
            backend = MongoBackend()
            with contextlib.redirect_stdout(buf):
                backend.query1(disease_id)
            backend.close()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Error", f"Query failed:\n{exc}")
            return

        self._display_output(buf.getvalue() or "No results.")

    def run_query2(self) -> None:
        if self.backend_var.get() != "neo4j":
            messagebox.showinfo(
                "Info",
                "Query 2 is defined only for the Neo4j graph backend.\n"
                "Please select 'Neo4j (Graph)' above.",
            )
            return

        disease_id = self._get_disease_id()
        if not disease_id:
            return

        buf = StringIO()
        try:
            backend = Neo4jBackend()
            with backend.driver.session() as _:
                with contextlib.redirect_stdout(buf):
                    backend.query2(disease_id)
            backend.close()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Error", f"Query failed:\n{exc}")
            return

        self._display_output(buf.getvalue() or "No candidate compounds found.")


def main() -> None:
    # Ensure a default size for text area that looks reasonable on demo screens.
    app = HetioNetGUI()
    app.mainloop()


if __name__ == "__main__":
    main()