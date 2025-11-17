import threading
import time
import uvicorn
from .config import load as load_config
from .db import Pg
from .opcua_nodes import OpcUaNodes
from .qr_listener import QrListener
from .asrs_mover import AsrsMover
from .api import app as fastapi_app

class AsrsServiceApp:
    def __init__(self, config: dict | None = None):
        self.cfg = config or load_config()
        self.pg = Pg()
        self.nodes = OpcUaNodes()
        self.qr = QrListener(self.cfg["OPCUA_ENDPOINT"], self.nodes, self.pg, interval=0.5)
        self.mover = AsrsMover(self.cfg["OPCUA_ENDPOINT"], self.nodes, self.pg)
        self.api_server = None
        # Share core components with FastAPI
        fastapi_app.state.pg = self.pg
        fastapi_app.state.mover = self.mover
        fastapi_app.state.qr = self.qr  # For reset operations

        # Operation timing tracking
        self.last_times = {"put": None, "pick": None}
        self.mover.on_cycle_done = self._on_cycle_done  # Update timing on completion
        fastapi_app.state.last_times = self.last_times  # Share timing with API
        self._stop = False
        self._t_qr = None
        self._t_mv = None
        self._t_api = None
        self._shutdown_event = threading.Event()

    def _on_qr(self, qr_code: str):
        """
        Callback for QR code detection - now handled directly in QrListener
        """
        # QR processing is now handled in QrListener._process_qr_code
        pass

    def _on_cycle_done(self, info: dict):
        """
        Callback invoked by the mover when a put or pick command completes
        (successfully or not).  Updates the last_times dictionary and logs
        the event.

        Args:
            info (dict): Information about the completed cycle.  Expected
                keys: 'kind' (str), 'basket' (str|None), 'seconds' (float),
                'success' (bool).
        """
        kind = (info.get("kind") or "").lower()
        secs = info.get("seconds")
        success = info.get("success", False)
        # Update last_times only when the operation kind is put or pick
        if kind in ("put", "pick") and secs is not None:
            self.last_times[kind] = secs
        # Log the event for diagnostics
        try:
            print(f"[TIME_LOG] {kind.upper() if kind else '?'} {info.get('basket', '')} -> {secs}s (" + ("ok" if success else "fail") + ")")
        except Exception:
            pass

    def _stop_flag(self):
        return self._stop

    def start(self):
        def run_qr():
            self.qr.start()
            self.qr.loop(callback=self._on_qr, edge_only=True, validate=True)

        def run_mover():
            self.mover.connect()
            self.mover.loop(self._stop_flag)

        def run_api():
            import asyncio
            config = uvicorn.Config(
                app=fastapi_app,
                host=self.cfg["API_HOST"],
                port=self.cfg["API_PORT"],
                log_level="info"
            )
            self.api_server = uvicorn.Server(config)
            # สร้าง event loop ใหม่สำหรับ thread นี้
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            # รัน serve() ใน event loop
            loop.run_until_complete(self.api_server.serve())

        self._t_qr = threading.Thread(target=run_qr, daemon=True)
        self._t_mv = threading.Thread(target=run_mover, daemon=True)
        self._t_api = threading.Thread(target=run_api, daemon=True)

        self._t_qr.start()
        self._t_mv.start()
        self._t_api.start()
        print(f"[System] started. API on :{self.cfg['API_PORT']}")

    def stop(self):
        print("[System] stopping gracefully...")
        # Signal all components to stop
        self._stop = True
        self._shutdown_event.set()

        # Set shutting_down flag for WebSocket connections
        fastapi_app.state.shutting_down = True

        # Stop QR listener
        try: 
            self.qr.stop()
            print("[System] QR listener stopped")
        except Exception as e: 
            print(f"[System] Error stopping QR listener: {e}")

        # Disconnect mover
        try: 
            self.mover.disconnect()
            print("[System] Mover disconnected")
        except Exception as e:
            print(f"[System] Error disconnecting mover: {e}")

        # Stop API server gracefully
        if self.api_server:
            try:
                self.api_server.should_exit = True
                print("[System] API server stopping...")
            except Exception as e:
                print(f"[System] Error stopping API server: {e}")

        # Give WebSocket connections and API server time to close gracefully
        time.sleep(2.0)

        # Wait for threads to finish (with timeout)
        threads = [
            (self._t_qr, "QR Thread"),
            (self._t_mv, "Mover Thread"),
            (self._t_api, "API Thread")
        ]
        
        for thread, name in threads:
            if thread and thread.is_alive():
                try:
                    thread.join(timeout=2.0)  # Wait up to 2 seconds for each thread
                    if thread.is_alive():
                        print(f"[System] Warning: {name} did not stop cleanly")
                except Exception as e:
                    print(f"[System] Error waiting for {name}: {e}")

        print("[System] stopped.")

def main():
    app = AsrsServiceApp()
    try:
        app.start()
        # Use event to wait instead of sleep loop
        while not app._shutdown_event.wait(0.5):
            pass
    except KeyboardInterrupt:
        print("\n[System] Received shutdown signal (Ctrl+C)")
    finally:
        app.stop()
