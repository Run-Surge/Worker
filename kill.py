import win32event
import win32api

# Use the same unique name as the target application
SHUTDOWN_EVENT_NAME = "Global\\MyPythonAppShutdownEvent"

def send_kill_signal():
    """Main function for the killer application."""
    print("Attempting to send shutdown signal...")
    try:
        # Open the existing event with rights to modify its state
        shutdown_event = win32event.OpenEvent(
            win32event.EVENT_MODIFY_STATE,  # We need permission to set the event
            False,                         # This handle should not be inherited
            SHUTDOWN_EVENT_NAME
        )
    except win32event.error as e:
        # This error (code 2) typically means the event doesn't exist,
        # which implies the target app isn't running.
        print(f"Error opening event: {e}")
        print("Is the target application running?")
        return

    # Signal the event
    try:
        win32event.SetEvent(shutdown_event)
        print("Shutdown signal sent successfully.")
    except Exception as e:
        print(f"Error setting event: {e}")
    finally:
        # Clean up the handle
        win32api.CloseHandle(shutdown_event)

if __name__ == "__main__":
    send_kill_signal()