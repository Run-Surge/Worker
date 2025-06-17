import customtkinter as ctk
import threading
import time
from PIL import Image, ImageDraw
import getpass
import tkinter as tk
import random
import colorsys
import os
import ctypes


ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")


class ToolkitApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.update_idletasks()
        self.title("Run Surge")
        self.geometry("400x550")
        self.resizable(False, False)
        self.iconbitmap("assets/images/logo.ico")
  
        self.bold_font = ctk.CTkFont(size=14, weight="bold")
        self.small_bold_font = ctk.CTkFont(size=12, weight="bold")
        self.italic_font = ctk.CTkFont(size=11, weight="bold", slant="italic")
        width = 400
        height = 550
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f"{width}x{height}+{x}+{y}")

        # Logo with white circular background
        original_logo = Image.open("assets/images/logo.png").convert("RGBA")
        size = int(max(original_logo.size) * 1.3)
        circle_bg = Image.new("RGBA", (size, size), (255, 255, 255, 0))
        white_circle = Image.new("RGBA", (size, size), (255, 255, 255, 255))
        mask = Image.new("L", (size, size), 0)
        ImageDraw.Draw(mask).ellipse((0, 0, size, size), fill=255)
        circle_bg.paste(white_circle, (0, 0), mask)
        offset = ((size - original_logo.width) // 2, (size - original_logo.height) // 2)
        circle_bg.paste(original_logo, offset, original_logo)
        self.logo_image = circle_bg

        self.logo_label = ctk.CTkLabel(self, text="")
        self.logo_label.pack(pady=30)
        self.logo_scale = 1.0
        self.zoom_in = True
        self.animate_logo()

        # Status frame with indicator
        status_frame = ctk.CTkFrame(self, fg_color="transparent")
        status_frame.pack(pady=10)

        status_inner_frame = tk.Frame(status_frame, bg=self.cget("bg"))
        status_inner_frame.pack()

        self.status_canvas = tk.Canvas(status_inner_frame, width=16, height=16, highlightthickness=0, bg=self.cget("bg"), bd=0)
        self.status_circle = self.status_canvas.create_oval(2, 2, 14, 14, fill="gray", outline="")
        self.status_canvas.grid(row=0, column=0, padx=(0, 8), pady=4, sticky="ns")  # ↑ nudge canvas down a bit

        self.status_label = ctk.CTkLabel(status_inner_frame, text="Status: Idle", font=self.bold_font)
        self.status_label.grid(row=0, column=1, sticky="w")

        # Start/Stop button
        self.start_button = ctk.CTkButton(self, text="Start", command=self.toggle_state, font=self.bold_font)
        self.start_button.pack(pady=10)

        # Resource selection label
        ctk.CTkLabel(self, text="Resource Selection", font=self.bold_font).pack(pady=(20, 5))
        resource_frame = ctk.CTkFrame(self, fg_color="transparent")
        resource_frame.pack(pady=5)

        self.value_spinbox = ctk.CTkEntry(resource_frame, justify="center", width=100, state="normal", font=self.bold_font)
        self.value_spinbox.insert(0, "1")
        self.value_spinbox.grid(row=0, column=0, padx=10)

        self.unit_combobox = ctk.CTkComboBox(resource_frame, values=["MB", "GB"], state="normal", width=100, font=self.bold_font)
        self.unit_combobox.set("MB")
        self.unit_combobox.grid(row=0, column=1, padx=10)

        # Particle burst canvas
        self.flash_canvas_width = 400
        self.flash_canvas = tk.Canvas(self, width=self.flash_canvas_width, height=20, bg=self.cget("bg"), highlightthickness=0, bd=0)
        self.flash_canvas.pack(pady=(20, 0))
        self.particles = []
        self.animate_particles()

        # Username and balance
        username = getpass.getuser()
        balance_amount = "125.00"
        user_balance_frame = ctk.CTkFrame(self, fg_color="transparent")
        user_balance_frame.pack(side="bottom", pady=(10, 0))

        ctk.CTkLabel(user_balance_frame, text=f"User: {username}", font=self.small_bold_font).grid(row=0, column=0, padx=10)
        ctk.CTkLabel(user_balance_frame, text=f"Balance: ${balance_amount}", font=self.small_bold_font).grid(row=0, column=1, padx=10)

        ctk.CTkLabel(self, text="© 2025 Run Surge Inc.", font=self.italic_font).pack(side="bottom", pady=(0, 5))

        self.app_state = "Idle"
        self.is_active = False
        if os.name == 'nt':
            icon_path = os.path.abspath("assets/images/logo.ico")
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(u"mycompany.myproduct.subproduct.version")
            self.iconbitmap(icon_path)

    def animate_logo(self):
        new_size = int(150 * self.logo_scale)
        resized_image = self.logo_image.resize((new_size, new_size), Image.Resampling.LANCZOS)
        self.tk_image = ctk.CTkImage(light_image=resized_image, dark_image=resized_image, size=(new_size, new_size))
        self.logo_label.configure(image=self.tk_image)

        self.logo_scale += 0.01 if self.zoom_in else -0.01
        if self.logo_scale >= 1.2:
            self.zoom_in = False
        elif self.logo_scale <= 1.0:
            self.zoom_in = True

        self.after(50, self.animate_logo)

    def toggle_state(self):
        if not self.is_active:
            threading.Thread(target=self.start_sequence, daemon=True).start()
        else:
            threading.Thread(target=self.stop_sequence, daemon=True).start()

    def start_sequence(self):
        self.set_status("Activating")
        self.value_spinbox.configure(state="disabled")
        self.unit_combobox.configure(state="disabled")
        self.start_button.configure(state="disabled", text="Starting...")
        time.sleep(5)
        self.is_active = True
        self.set_status("Active")
        self.start_button.configure(state="normal", text="Stop")

    def stop_sequence(self):
        self.set_status("Stopping")
        self.start_button.configure(state="disabled", text="Stopping...")
        time.sleep(5)
        self.is_active = False
        self.set_status("Idle")
        self.start_button.configure(state="normal", text="Start")
        self.unit_combobox.configure(state="normal")
        self.value_spinbox.configure(state="normal")

    def set_status(self, status):
        self.status_label.configure(text=f"Status: {status}")
        self.update_status_indicator(status)
        self.app_state = status

    def update_status_indicator(self, status):
        color_map = {
            "Idle": "gray",
            "Activating": "orange",
            "Active": "green",
            "Stopping": "red"
        }
        color = color_map.get(status, "gray")
        self.status_canvas.itemconfig(self.status_circle, fill=color)

    def get_gradient_color(self):
        brightness = random.uniform(0.4, 1.0)
        r, g, b = colorsys.hsv_to_rgb(0.6, 1.0, brightness)
        return f'#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}'

    def animate_particles(self):
        for _ in range(random.randint(1, 3)):
            y = random.randint(1, 15)
            color = self.get_gradient_color()
            lifetime = int(self.flash_canvas_width / 8) + 5
            particle = {"x": 0, "y": y, "r": 3, "color": color, "life": lifetime}
            self.particles.append(particle)

        self.flash_canvas.delete("all")
        new_particles = []
        for p in self.particles:
            p["x"] += 8
            p["life"] -= 1
            if p["x"] < self.flash_canvas_width and p["life"] > 0:
                self.flash_canvas.create_oval(p["x"]-p["r"], p["y"]-p["r"], p["x"]+p["r"], p["y"]+p["r"],
                                              fill=p["color"], outline="")
                new_particles.append(p)

        self.particles = new_particles
        self.after(500, self.animate_particles)


if __name__ == "__main__":
    app = ToolkitApp()
    app.mainloop()
