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
import webbrowser
import requests
import subprocess
import signal
import sys
from worker_app.config import Config

ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")


class ToolkitApp(ctk.CTk):
    def __init__(self):
        if os.name == 'nt':
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(u"mycompany.myproduct.subproduct.version")

        super().__init__()

        self.title("Run Surge")
        self.geometry("400x550")
        self.resizable(False, False)
        self.iconbitmap(os.path.abspath("assets/images/logo.ico"))

        self.bold_font = ctk.CTkFont(size=14, weight="bold")
        self.small_bold_font = ctk.CTkFont(size=12, weight="bold")
        self.italic_font = ctk.CTkFont(size=11, weight="bold", slant="italic")

        width = 400
        height = 550
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f"{width}x{height}+{x}+{y}")

        self.logined = False
        self.username = None
        self.password = None
        self.memory_size = 1024 * 1024 * 1024
        self.memory_unit = "GB"
        self.child_process = None
        
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

        if not self.logined:
            self.show_login_page()
        else:
            self.setup_main_ui()

    def show_login_page(self):
        self.login_frame = ctk.CTkFrame(self)
        self.login_frame.pack(expand=True)

        self.logo_label = ctk.CTkLabel(self.login_frame, text="")
        self.logo_label.pack(pady=30)
        self.display_static_logo()

        self.username_entry = ctk.CTkEntry(self.login_frame, placeholder_text="Username", width=300, height=45)
        self.username_entry.pack(pady=8)

        self.password_entry = ctk.CTkEntry(self.login_frame, placeholder_text="Password", show="*", width=300, height=45)
        self.password_entry.pack(pady=8)

        self.username_entry.bind("<Return>", lambda _: self.try_login())
        self.password_entry.bind("<Return>", lambda _: self.try_login())



        self.login_error_label = ctk.CTkLabel(self.login_frame, text="", text_color="red", font=self.small_bold_font)
        self.login_error_label.pack(pady=(0, 3))

        login_btn = ctk.CTkButton(self.login_frame, text="Login", command=self.try_login, font=self.bold_font)
        login_btn.pack(pady=8)

        signup_btn = ctk.CTkButton(self.login_frame, text="Sign Up", command=self.open_signup_page, font=self.bold_font)
        signup_btn.pack(pady=8)

    def display_static_logo(self):
        resized_image = self.logo_image.resize((150, 150), Image.Resampling.LANCZOS)
        self.tk_image = ctk.CTkImage(light_image=resized_image, dark_image=resized_image, size=(150, 150))
        self.logo_label.configure(image=self.tk_image)
    def validate_integer(self, value):
        """Validate that input is a positive integer or empty string"""
        if value == "":
            return True  # Allow empty string for deletion
        try:
            int_value = int(value)
            return int_value > 0  # Only allow positive integers
        except ValueError:
            return False

    def shake_widget(self, widget):
        original_x = widget.winfo_x()
        original_y = widget.winfo_y()

        def move(offsets, index=0):
            if index < len(offsets):
                widget.place(x=original_x + offsets[index], y=original_y)
                self.after(30, lambda: move(offsets, index + 1))
            else:
                widget.place_forget()
                widget.pack(pady=8)  # Restore original packing

        # Switch to place to allow precise x movement
        widget.pack_forget()
        widget.place(x=original_x, y=original_y)
        move([-5, 5, -4, 4, -3, 3, -2, 2, 0])

    

    def try_login(self):
        username = self.username_entry.get()
        password = self.password_entry.get()
        def _login(username: str, password: str) -> bool:
            url = f"http://{Config.master_ip_address}:{Config.maseter_backend_port}/api/auth/login"
            print(url)
            response = requests.post(
                url,
                json={
                    "username_or_email": username,
                    "password": password
                }
            )
            print(response.json())
            return response.status_code == 200
        
        if _login(username, password):
            self.logined = True
            self.username = username
            self.password = password
            self.login_frame.destroy()
            self.setup_main_ui()
        else:
            self.username_entry.configure(border_color="red")
            self.password_entry.configure(border_color="red")
            self.login_error_label.configure(text="Username or password is incorrect.")

    def open_signup_page(self):
        webbrowser.open("https://google.com/")

    def setup_main_ui(self):
        self.logo_label = ctk.CTkLabel(self, text="")
        self.logo_label.pack(pady=30)
        self.display_static_logo()

        status_frame = ctk.CTkFrame(self, fg_color="transparent")
        status_frame.pack(pady=10)

        status_inner_frame = tk.Frame(status_frame, bg=self.cget("bg"))
        status_inner_frame.pack()

        self.status_canvas = tk.Canvas(status_inner_frame, width=16, height=16, highlightthickness=0,
                                       bg=self.cget("bg"), bd=0)
        self.status_circle = self.status_canvas.create_oval(2, 2, 14, 14, fill="gray", outline="")
        self.status_canvas.grid(row=0, column=0, padx=(0, 8), pady=4, sticky="ns")

        self.status_label = ctk.CTkLabel(status_inner_frame, text="Status: Idle", font=self.bold_font)
        self.status_label.grid(row=0, column=1, sticky="w")

        self.start_button = ctk.CTkButton(self, text="Start", command=self.toggle_state, font=self.bold_font)
        self.start_button.pack(pady=10)

        ctk.CTkLabel(self, text="Resource Selection", font=self.bold_font).pack(pady=(20, 5))
        resource_frame = ctk.CTkFrame(self, fg_color="transparent")
        resource_frame.pack(pady=5)

        # Register validation function
        vcmd = (self.register(self.validate_integer), '%P')
        
        self.value_spinbox = ctk.CTkEntry(resource_frame, justify="center", width=100, state="normal", font=self.bold_font, validate='key', validatecommand=vcmd)
        self.value_spinbox.insert(0, "1")
        self.value_spinbox.grid(row=0, column=0, padx=10)
        def _on_unit_change(value):
            self.memory_unit = value
            self.memory_size = int(self.value_spinbox.get()) * 1024 * 1024 if self.memory_unit == "MB" \
              else int(float(self.value_spinbox.get())) * 1024 * 1024 * 1024
            
        self.unit_combobox = ctk.CTkComboBox(resource_frame, values=["MB", "GB"], state="normal", width=100, font=self.bold_font,command=_on_unit_change)
        self.unit_combobox.set("GB")
        self.unit_combobox.grid(row=0, column=1, padx=10)

        self.flash_canvas_width = 400
        self.flash_canvas = tk.Canvas(self, width=self.flash_canvas_width, height=20, bg=self.cget("bg"),
                                      highlightthickness=0, bd=0)
        self.flash_canvas.pack(pady=(20, 0))
        self.particles = []
        self.animate_particles()

        username = self.username
        balance_amount = "125.00"
        user_balance_frame = ctk.CTkFrame(self, fg_color="transparent")
        user_balance_frame.pack(side="bottom", pady=(10, 0))

        ctk.CTkLabel(user_balance_frame, text=f"User: {username}", font=self.small_bold_font).grid(row=0, column=0, padx=10)
        ctk.CTkLabel(user_balance_frame, text=f"Balance: ${balance_amount}", font=self.small_bold_font).grid(row=0, column=1, padx=10)

        ctk.CTkLabel(self, text="© 2025 Run Surge Inc.", font=self.italic_font).pack(side="bottom", pady=(0, 5))

        self.app_state = "Idle"
        self.is_active = False
        
        # Start periodic process monitoring
        self.monitor_process()

    def toggle_state(self):
        if not self.is_active:
            threading.Thread(target=self.start_sequence, daemon=True).start()
        else:
            threading.Thread(target=self.stop_sequence, daemon=True).start()


    def _launch_vm_directly(self) -> bool:
        """
        Launch VM directly as a subprocess of Python.
        """
        try:
            print("Launching VM directly...")

            cmd_list = [
                "venv/Scripts/python.exe",
                "main.py",
                "--username",
                self.username,
                "--password", 
                self.password,
                "--memory-bytes",
                str(self.memory_size)
            ]


            creation_flags = 0
            if os.name == 'nt': # If running on Windows
                # CREATE_NEW_PROCESS_GROUP makes the child process the root of a new process group.
                # When the parent dies, the OS can clean up this group more effectively.
                # It often works in conjunction with job objects for robust cleanup.
                creation_flags |= subprocess.CREATE_NEW_PROCESS_GROUP
                creation_flags |= subprocess.CREATE_NO_WINDOW # To prevent console window pop-up

            print(cmd_list)
            self.child_process = subprocess.Popen(
                cmd_list,
                # Use shell=True for Windows compatibility like the batch method
                shell=False,
                creationflags=creation_flags,
                # Don't capture QEMU output - let it run freely
                stdout=None,
                stderr=None
            )
            self.logger.info(f"VM process (PID: {self.vm_process.pid}) started directly.")
            return True

        except Exception as e:
            self.logger.error(f"Failed to launch VM directly: {e}")
            return False

    def _start_vm(self) -> bool:
        try:
            self._launch_vm_directly()
            # print('child process pid', self.child_process.pid)
            return True
        except Exception as e:
            print(e)
            return False
    
    def _stop_vm(self) -> bool:        
        try:
            import psutil
            current_process = psutil.Process()
            children = current_process.children(recursive=True)
            print(f"Children: {children}")
            for child in children:
                self.logger.info(f"Terminating process {child.pid}")
                try:
                    child.send_signal(signal.SIGTERM)
                    # child.terminate()  # Try graceful termination first
                    # child.kill()
                    # try:
                    #     # child.wait(timeout=5)  # Wait up to 5 seconds
                    # except psutil.TimeoutExpired:
                    #     self.logger.warning(f"Process {child.pid} did not terminate gracefully, forcing kill")
                    #     child.kill()  # Force kill if process doesn't respond to terminate
                except psutil.NoSuchProcess:
                    pass  # Process already terminated
                except Exception as e:
                    self.logger.error(f"Error killing process {child.pid}: {e}")
            # self.child_process.send_signal(signal.SIGTERM)
            # self.child_process.wait()
            time.sleep(50)
            self.child_process = None
            return True
        except Exception as e:
            print(e)
            return False
    
    def check_process_status(self):
        """Check if the child process is still running when state shows as active"""
        if self.is_active and self.app_state == "Active" and hasattr(self, 'child_process_pid') and self.child_process:
            # Check if process has terminated
            return_code = self.child_process.poll()
            if return_code is not None:  # Process has terminated
                print(f"Child process died unexpectedly with return code: {return_code}")
                # Update UI to reflect the process death
                self.is_active = False
                self.set_status("Failed")
                self.start_button.configure(state="normal", text="Start")
                self.unit_combobox.configure(state="normal")
                self.value_spinbox.configure(state="normal")
                self.child_process = None
                return False
        return True
    
    def monitor_process(self):
        """Periodically monitor the child process status"""
        # self.check_process_status()
        # # Schedule the next check in 2 seconds
        # self.after(2000, self.monitor_process)
    
    def start_sequence(self):
        self.set_status("Activating")
        self.value_spinbox.configure(state="disabled")
        self.unit_combobox.configure(state="disabled")
        self.start_button.configure(state="disabled", text="Starting...")
       
        self._start_vm()
        # if not self._start_vm():
        #     self.set_status("Failed to start VM")
        #     self.start_button.configure(state="normal", text="Start")
        #     return
        
        self.is_active = True
        self.set_status("Active")
        self.start_button.configure(state="normal", text="Stop")

    def stop_sequence(self):
        self.set_status("Stopping")
        self.start_button.configure(state="disabled", text="Stopping...")
        
        self._stop_vm()
        
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
