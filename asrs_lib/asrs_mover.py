# ASRS movement control and command execution
import time
import threading
from typing import Optional
from opcua import Client, ua

try:
    from .opcua_nodes import OpcUaNodes
    from .utils import encoder_to_position
except ImportError:
    from opcua_nodes import OpcUaNodes
    from utils import encoder_to_position


class AsrsMover:

    def __init__(self, endpoint: str, nodes: OpcUaNodes, db):
        self.endpoint = endpoint
        self.nodes = nodes
        self.db = db
        self.client: Optional[Client] = None

        self._send_lock = threading.Lock()
        self._last_cmd_info = None
        self._current_xyz = (1, 1, 0)   # Current position (column, row, depth)
        self._enc_raw = (None, None, None)
        # Clear-request monitoring
        self.n_plc_req_clear = None
        self.n_wms_clear_reply_node = None
        self._clear_monitor_thread = None
        self._clear_monitor_stop = False
        self._pending_clear = False
        
        self.on_cycle_done = None
        self.last_durations = {"put": None, "pick": None}

    # Data value helpers
    @staticmethod
    def _dv_bool(v: bool):
        dv = ua.DataValue(ua.Variant(v, ua.VariantType.Boolean))
        # DO NOT set dv.ServerTimestamp or dv.SourceTimestamp
        return dv

    @staticmethod
    def _dv_str(s: str):
        dv = ua.DataValue(ua.Variant(s, ua.VariantType.String))
        # DO NOT set dv.ServerTimestamp or dv.SourceTimestamp
        return dv


    # -------- low-level --------
    def _pulse(self, node, width_s: float = 0.05):
        try:
            node.set_value(self._dv_bool(True))
            time.sleep(width_s)
        finally:
            try:
                node.set_value(self._dv_bool(False))
            except Exception:
                pass

    def _wait(self, node, want: bool, timeout_s: float) -> bool:
        t0 = time.time()
        while time.time() - t0 < timeout_s:
            try:
                if bool(node.get_value()) == want:
                    return True
            except Exception:
                pass
            time.sleep(0.02)
        return False

    def _system_ready(self) -> bool:
        try:
            ready = bool(self.n_ready.get_value())
            auto  = bool(self.n_auto.get_value())
            alarm = bool(self.n_alarm.get_value())
            return ready and auto and not alarm
        except Exception:
            return False

    # -------- connect / disconnect --------
    def connect(self, max_retry=999, delay=2.0):
        for i in range(max_retry):
            try:
                self.client = Client(self.endpoint)
                self.client.application_uri = "urn:wms-asrs-client"
                self.client.session_timeout = 3600000
                self.client.connect()

                # command handshake
                self.n_cmd  = self.client.get_node(self.nodes.cmd)
                self.n_send = self.client.get_node(self.nodes.wms_send_cmd)
                self.n_ack  = self.client.get_node(self.nodes.plc_receive_cmd)
                self.n_complete     = self.client.get_node(self.nodes.cmd_complete)
                self.n_complete_rpy = self.client.get_node(self.nodes.wms_complete_reply)

                # QR handshake
                self.n_basket_qr   = self.client.get_node(self.nodes.basket_qr)
                self.n_plc_req_qr  = self.client.get_node(self.nodes.plc_send_basket_qr)
                self.n_wms_recv_qr = self.client.get_node(self.nodes.wms_receive_basket_qr)

                # (!!!) FIX: ใช้ node จาก opcua_nodes.py (ซึ่งควรจะเป็น None) (!!!)
                try:
                    if self.nodes.plc_req_wms_clear:
                        self.n_plc_req_clear = self.client.get_node(self.nodes.plc_req_wms_clear)
                        self.n_wms_clear_reply_node = self.client.get_node(self.nodes.wms_clear_reply)
                    else:
                         self.n_plc_req_clear = None
                         self.n_wms_clear_reply_node = None
                except Exception:
                    self.n_plc_req_clear = None
                    self.n_wms_clear_reply_node = None

                try:
                    self._clear_monitor_stop = False
                    self._clear_monitor_thread = threading.Thread(target=self._monitor_clear_request, daemon=True)
                    self._clear_monitor_thread.start()
                except Exception:
                    self._clear_monitor_thread = None

                # status
                self.n_ready = self.client.get_node(self.nodes.asrs_ready)
                self.n_auto  = self.client.get_node(self.nodes.asrs_auto_mode)
                self.n_alarm = self.client.get_node(self.nodes.asrs_alarm)

                # encoders
                self.n_cx = self.client.get_node(self.nodes.crane_x)
                self.n_cy = self.client.get_node(self.nodes.crane_y)
                self.n_cz = None

                # init outputs low
                for n in (self.n_send, self.n_complete_rpy, self.n_wms_recv_qr):
                    try: n.set_value(self._dv_bool(False))
                    except Exception: pass
                
                if self.n_wms_clear_reply_node:
                    try: self.n_wms_clear_reply_node.set_value(self._dv_bool(False))
                    except Exception: pass

                print("[Mover] connected")
                return
            except Exception as e:
                print(f"[Mover] connect failed ({i+1}): {e}")
                time.sleep(delay)
        raise RuntimeError("ASRS mover: cannot connect to OPC UA")

    def disconnect(self):
        if self.client:
            try: self.client.disconnect()
            except Exception: pass
            self.client = None
        try:
            self._clear_monitor_stop = True
            if self._clear_monitor_thread and self._clear_monitor_thread.is_alive():
                self._clear_monitor_thread.join(timeout=1.0)
        except Exception:
            pass
            
    def reset_current_command(self):
        try:
            self._clear_cmd_exact()
            try: self.n_send.set_value(self._dv_bool(False))
            except Exception: pass
            
            try:
                self._pulse(self.n_complete_rpy, width_s=0.05)
                time.sleep(0.1)
                self._pulse(self.n_complete_rpy, width_s=0.05)
            except Exception: pass
            
            return True
        except Exception as e:
            print("[ASRS] Reset command error:", e)
            raise Exception(f"Failed to reset command: {str(e)}")

    # -------- encoders --------
    def _read_crane_position(self):
        x_col, y_row, z_depth = self._current_xyz
        rx = ry = None
        try:
            rx = int(self.n_cx.get_value())
            ry = int(self.n_cy.get_value())
        except Exception:
            pass
        self._enc_raw = (rx, ry, None)
        if rx is not None and ry is not None:
            x_col, y_row = encoder_to_position(rx, ry)
            z_depth = 0
        self._current_xyz = (x_col, y_row, z_depth)

    # -------- QR: ตอบเมื่อ PLC ขอ --------
    def _serve_qr_if_requested(self, basket: str, wait_timeout: float = 0.0):
        if wait_timeout > 0:
            t0 = time.time()
            while time.time() - t0 < wait_timeout:
                try:
                    if bool(self.n_plc_req_qr.get_value()):
                        break
                except Exception:
                    pass
                time.sleep(0.02)
        try:
            if bool(self.n_plc_req_qr.get_value()):
                self.n_basket_qr.set_value(self._dv_str(basket or ""))
                time.sleep(0.02)
                self._pulse(self.n_wms_recv_qr, 0.06)
                ok = self._wait(self.n_plc_req_qr, False, 2.0)
                print(f"[QR] replied {basket or '(empty)'} ({'ok' if ok else 'no drop'})")
        except Exception as e:
            print("[QR] reply error:", e)

    # -------- เคลียร์สตริง/แฟล็กหลังจบงาน --------
    def _clear_cmd_exact(self):
        try:
            self.n_cmd.set_value(self._dv_str(""))
        except Exception as e:
            print("[WARN] clear cmd failed:", e)
        time.sleep(0.02)
        try: self.n_basket_qr.set_value(self._dv_str(""))
        except Exception: pass
        try: self.n_wms_recv_qr.set_value(self._dv_bool(False))
        except Exception: pass
        try: self.n_send.set_value(self._dv_bool(False))
        except Exception: pass

    def _monitor_clear_request(self):
        while not self._clear_monitor_stop:
            try:
                if self.n_plc_req_clear is None or self.n_wms_clear_reply_node is None:
                    time.sleep(0.25)
                    continue

                try:
                    req = bool(self.n_plc_req_clear.get_value())
                except Exception:
                    req = False

                if req:
                    try:
                        self._pulse(self.n_wms_clear_reply_node, width_s=0.05)
                        print("[ASRS] PLC requested clear -> sent WMS clear reply")
                    except Exception as e:
                        print("[ASRS] Failed to pulse WMS clear reply:", e)

                    acquired = self._send_lock.acquire(blocking=False)
                    try:
                        if acquired:
                            try:
                                self._clear_cmd_exact()
                                self._pending_clear = False
                                print("[ASRS] Cleared WMS data on PLC request")
                            except Exception as e:
                                print("[ASRS] Error clearing WMS data:", e)
                        else:
                            self._pending_clear = True
                    finally:
                        if acquired:
                            self._send_lock.release()
                else:
                    if self._pending_clear:
                        acquired = self._send_lock.acquire(blocking=False)
                        if acquired:
                            try:
                                self._clear_cmd_exact()
                                self._pending_clear = False
                                print("[ASRS] Cleared pending WMS data")
                            except Exception as e:
                                print("[ASRS] Error clearing pending WMS data:", e)
                            finally:
                                self._send_lock.release()

            except Exception:
                pass
            time.sleep(0.15)

    # -------- (!!!) MODIFICATION: ลบ x, y, z ออกจาก signature (!!!) --------
    def send_job_blocking(self, cmd_str: str, methode: str, row: dict, shelf_id: int):
        """
        (!!!) REVERTED TO 2.5 WORKAROUND (!!!)
        1) system ready?
        2) ส่งคำสั่ง + รอ ACK
        3) (!!!) อัปเดต DB (Fire-and-Forget) (!!!)
        4) บริการ QR
        5) รอ complete (เพื่อล้าง Handshake)
        """
        
        t_start_job = time.time()
        job_success = False # (!!!) เราจะตั้งเป็น True หลังจากอัปเดต DB (!!!)
        basket_id = (row.get("basket") or "").strip()
        
        with self._send_lock:
            if not self._system_ready():
                print("[ASRS] System not ready/auto/alarm; skip")
                return False # (!!!) ลบ logging (!!!)

            # เคลียร์สถานะเดิม
            max_clear_attempts = 3
            ack_clear = False
            complete_clear = False
            for _ in range(max_clear_attempts):
                ack_clear = self._wait(self.n_ack, False, 2.0)
                complete_clear = self._wait(self.n_complete, False, 2.0)
                if ack_clear and complete_clear:
                    break
                try:
                    self._clear_cmd_exact()
                    self._pulse(self.n_complete_rpy, width_s=0.05)
                    time.sleep(0.5)
                except Exception:
                    pass
            
            if not (ack_clear and complete_clear):
                print("[ASRS] Cannot clear previous command state")
                return False # (!!!) ลบ logging (!!!)

            # 2) ส่งคำสั่ง + รอ ACK
            try:
                self.n_cmd.set_value(self._dv_str(cmd_str))
                self.n_send.set_value(self._dv_bool(True))
            except Exception as e:
                print(f"[ASRS] CRITICAL: Failed to send command to PLC: {e}")
                try: self.n_send.set_value(self._dv_bool(False))
                except Exception: pass
                return False # (!!!) ลบ logging (!!!)

            if not self._wait(self.n_ack, True, 5.0):
                self.n_send.set_value(self._dv_bool(False))
                print("[ASRS] ACK timeout")
                return False # (!!!) ลบ logging (!!!)

            # ลบออกจากคิวหลัง ACK
            try:
                if row and "id" in row:
                    self.db.delete_queue_row(methode, int(row["id"]))
            except Exception as e:
                print("[ASRS] delete_queue_row error:", e)

            self.n_send.set_value(self._dv_bool(False))

            # (!!!) START OF 2.5 WORKAROUND (!!!)
            # 3) Update DB ทันที (Fire-and-Forget)
            try:
                if methode == "PUT":
                    info = self.db.move_put(shelf_id, basket_id, allow_overwrite_dest=False)
                    print(f"[ASRS] DB_UPDATE (Sent) -> shelf {shelf_id} <= {basket_id} | cleared_from={info.get('cleared_from')}")
                else:
                    self.db.mark_pick(shelf_id)
                    print(f"[ASRS] DB_UPDATE (Sent) -> shelf {shelf_id} empty")
                
                job_success = True # (!!!) ถือว่างานสำเร็จแล้ว ณ จุดนี้ (!!!)
                
            except Exception as e:
                print(f"[ASRS] CRITICAL DB UPDATE ERROR (after ACK): {e}")
                job_success = False # ถ้า DB ล้มเหลว ถือว่าล้มเหลว
                # (!!!) ลบ logging (!!!)
            # (!!!) END OF 2.5 WORKAROUND (!!!)


            # 4) QR ระหว่างทาง (ถ้ามีคำขอ)
            self._serve_qr_if_requested(basket_id, wait_timeout=0.5)

            # 5) รอ complete (บล็อกจนเสร็จ)
            t0 = time.time()
            complete_timeout = 120.0
            done = False
            last_status_time = 0
            check_interval = 10.0
            print(f"[ASRS] Starting operation (now waiting for complete): {methode} {basket_id}")
            
            system_not_ready_flag = False
            
            while time.time() - t0 < complete_timeout:
                current_time = time.time()
                try:
                    if bool(self.n_complete.get_value()):
                        time.sleep(1.0)
                        
                        try:
                            self._pulse(self.n_complete_rpy, width_s=0.1)
                            print(f"[ASRS] Operation completed (PLC signal received): {methode} {basket_id}")
                        except Exception as e:
                            print(f"[ASRS] Failed to send complete reply: {e}")
                        
                        time.sleep(0.5)
                        
                        try:
                            self._clear_cmd_exact()
                            print("[ASRS] Command cleared")
                        except Exception as e:
                            print(f"[ASRS] Failed to clear command: {e}")
                            
                        done = True
                        break 

                    # รายงานสถานะ
                    if current_time - last_status_time >= check_interval:
                        elapsed = current_time - t0
                        remaining = complete_timeout - elapsed
                        print(f"[ASRS] Operation in progress: {methode} {basket_id} ({remaining:.0f}s remaining)")
                        last_status_time = current_time
                        
                except Exception as e:
                    print(f"[ASRS] Error reading complete signal: {e}")
                    time.sleep(0.1)
                    continue

                # ตอบ QR (ถ้าระหว่างทาง)
                try:
                    if bool(self.n_plc_req_qr.get_value()):
                        self._serve_qr_if_requested(basket_id, wait_timeout=0.0)
                except Exception:
                    pass

                # ตรวจสอบสถานะระบบ
                if not self._system_ready():
                    if not system_not_ready_flag:
                        print("[ASRS] System not ready during operation")
                        system_not_ready_flag = True
                    time.sleep(0.5)
                    continue

                time.sleep(0.03)

            
            if not done:
                reason = "complete_timeout"
                if system_not_ready_flag:
                    reason = "system_not_ready_timeout"
                print(f"[ASRS] {reason} - no signal received after", complete_timeout, "seconds")
                # (!!!) ลบ logging (!!!)
            
            # 6) เคลียร์ข้อมูล (ทำเสมอ ไม่ว่าจะ done หรือ timeout)
            try:
                self._pulse(self.n_complete_rpy, width_s=0.05)
            except Exception as e:
                print(f"[ASRS] Error pulsing complete_rpy (1): {e}")

            self._clear_cmd_exact()
            self._wait(self.n_complete, False, 5.0)
            time.sleep(0.15)

            try:
                self._pulse(self.n_complete_rpy, width_s=0.05)
            except Exception as e:
                print(f"[ASRS] Error pulsing complete_rpy (2): {e}")

            self._wait(self.n_ack, False, 5.0)
            self._wait(self.n_ready, True, 3.0)
            
            t_end_job = time.time()
            duration = t_end_job - t_start_job
            kind = methode.lower()
            if kind in self.last_durations:
                self.last_durations[kind] = duration
            if self.on_cycle_done:
                try:
                    self.on_cycle_done({
                        "kind": kind,
                        "basket": basket_id,
                        "seconds": duration,
                        "success": job_success 
                    })
                except Exception as e:
                    print(f"[ASRS] Error in on_cycle_done callback: {e}")

            return job_success

    # -------- เลือกคิวแบบ FIFO (ไม่มีคะแนน) --------
    def _select_next(self, window_each: int = 20):
        picks, puts = self.db.next_command_window(limit_each=window_each)

        def first_usable(jobs):
            for r in jobs:
                try:
                    mapping = self.db.get_mapping_for_basket(r["basket"])
                except Exception as e:
                    print(f"[ASRS] mapping lookup error for {r.get('basket')}: {e}")
                    continue
                if not mapping:
                    print(f"[ASRS] No mapping found for basket {r.get('basket')}, skipping.")
                    try: self.db.delete_queue_row(r["methode"], int(r["id"]))
                    except Exception: pass
                    continue

                shelf_id = mapping[0]
                try:
                    if self.db.shelf_can_use(shelf_id):
                        return (r, mapping)
                except Exception as e:
                    print(f"[ASRS] shelf_can_use error shelf {shelf_id}: {e}")
                    continue
            return (None, None)

        p_job, p_map = first_usable([dict(r, methode="PICK") for r in picks])
        q_job, q_map = first_usable([dict(r, methode="PUT") for r in puts])


        if p_job and not q_job:
            return ("PICK", p_job, p_map)
        if q_job and not p_job:
            return ("PUT", q_job, q_map)
        if p_job and q_job:
            try:
                if p_job["created_at"] <= q_job["created_at"]:
                    return ("PICK", p_job, p_map)
                else:
                    return ("PUT", q_job, q_map)
            except Exception:
                return ("PICK", p_job, p_map)
        return (None, None, None)

    # -------- สร้างสตริงคำสั่ง 20 ตัวอักษร --------
    @staticmethod
    def _id4(n: int) -> str:
        s = str(n); return ("0000" + s)[-4:]

    @staticmethod
    def _d2(n: int) -> str:
        s = str(n); return s if len(s) > 1 else ("0" + s)

    # -------- main loop (บล็อกทีละงาน) --------
    def loop(self, stop_flag):
        get_stop = getattr(stop_flag, "is_set", None)
        checker = get_stop if callable(get_stop) else (stop_flag if callable(stop_flag) else (lambda: False))

        current_id = 0
        while not checker():
            try:
                self._read_crane_position()

                methode, row, mapping = self._select_next(window_each=20)
                if not row:
                    time.sleep(0.1)
                    continue
                
                basket_id = row["basket"]
                shelf_id, x, y, z = mapping

                # 20-chars: iiii M XX YY Z bbbbbbbbbb
                M = "0" if methode == "PUT" else "1"
                X = self._d2(int(x))
                Y = self._d2(int(y))
                Z = str(int(z))
                cmd_str = f"{self._id4(current_id)}{M}{X}{Y}{Z}{basket_id}"
                if len(cmd_str) != 20:
                    print("[ERR] CMD length not 20:", cmd_str)
                    time.sleep(0.1)
                    continue

                # (!!!) MODIFICATION: ลบ x, y, z ออกจากการเรียก (!!!)
                ok = self.send_job_blocking(cmd_str, methode, row, shelf_id)
                
                if ok:
                    self._last_cmd_info = {
                        "id": current_id,
                        "methode": methode,
                        "basket": basket_id,
                        "x": int(x), "y": int(y), "z": int(z),
                        "shelf_id": shelf_id
                    }
                    current_id += 1
                else:
                    print("[ASRS] Job failed (Critical Error), waiting 2s before next attempt.")
                    time.sleep(2.0)

            except Exception as e:
                print("[ASRS loop error]", e)
                time.sleep(0.2)