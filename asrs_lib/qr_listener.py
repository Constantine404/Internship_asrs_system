import time
from opcua import Client, ua
from .opcua_nodes import OpcUaNodes

class QrListener:
    def __init__(self, endpoint: str, nodes: OpcUaNodes, db, interval=0.5):
        self.endpoint = endpoint
        self.nodes = nodes
        self.interval = interval
        self.db = db
        # QR acknowledgment node for PLC handshake
        self.n_ack_basket = None
        self.client = None
        self._last_flag = None
        self._last_qr = None
        self._stop = False

    # (!!!) START OF FIX (!!!)
    # เพิ่ม Helper function ตัวเดียวกับใน asrs_mover.py
    @staticmethod
    def _dv_bool(v: bool):
        """Helper to create a DataValue without timestamps."""
        dv = ua.DataValue(ua.Variant(v, ua.VariantType.Boolean))
        # DO NOT set dv.ServerTimestamp or dv.SourceTimestamp
        return dv
    # (!!!) END OF FIX (!!!)

    def start(self, max_retry=999, delay=2.0):
        for i in range(max_retry):
            try:
                self.client = Client(self.endpoint)
                self.client.application_uri = "urn:python-asrs-client"
                self.client.connect()
                self.n_qr   = self.client.get_node(self.nodes.basket_qr)
                self.n_flag = self.client.get_node(self.nodes.plc_send_basket_qr)
                # Get QR acknowledgment node if configured
                try:
                    ack_id = getattr(self.nodes, 'wms_receive_basket_qr', None)
                    if ack_id:
                        self.n_ack_basket = self.client.get_node(ack_id)
                except Exception:
                    self.n_ack_basket = None
                # optional ASRS status node to avoid enqueue while busy
                try:
                    asrs_ready_id = getattr(self.nodes, 'asrs_ready', None)
                    if asrs_ready_id:
                        self.n_asrs_ready = self.client.get_node(asrs_ready_id)
                    else:
                        self.n_asrs_ready = None
                except Exception:
                    self.n_asrs_ready = None
                print("[QR] connected")
                return
            except Exception as e:
                print(f"[QR] connect failed ({i+1}): {e}")
                time.sleep(delay)
        raise RuntimeError("QR listener: cannot connect to OPC UA")

    def stop(self):
        self._stop = True
        if self.client:
            try: self.client.disconnect()
            except: pass
            self.client = None

    def reset_state(self):
        """
        Reset internal state of the QR listener.
        """
        self._last_qr = None
        self._last_flag = None
        print("[QR] State reset complete")
        
    def _send_error_acknowledgment(self):
        """Send error acknowledgment signal to PLC"""
        if self.n_ack_basket is not None:
            try:
                # (!!!) FIX: ใช้วิธี _dv_bool (!!!)
                print("[QR] Sending ERROR ACK")
                self.n_ack_basket.set_value(self._dv_bool(True))
                time.sleep(0.2)  # pulse ยาวกว่าปกติ (200ms)
                self.n_ack_basket.set_value(self._dv_bool(False))
            except Exception as e:
                print(f"[ERROR] Failed to send error acknowledgment: {e}")

    def _process_qr_code(self, qr_code: str):
        """
        Process a QR code by adding it to the appropriate queue in the database
        """
        print("[QR]", qr_code)
        # Get basket mapping data
        try:
            mapping = self.db.get_mapping_for_basket(qr_code)
            if not mapping:
                print(f"[ERROR] ⚠️ Basket '{qr_code}' not registered in system database")
                print("[INFO] Please add basket data to the system before using")
                self._send_error_acknowledgment()  # ส่งสัญญาณแจ้งเตือนไปยัง PLC
                return
            shelf_id, x, y, z = mapping
        except Exception as e:
            print(f"[ERROR] Failed to get mapping for basket '{qr_code}': {e}")
            self._send_error_acknowledgment()
            return

        # Verify basket storage status
        try:
            occupied_shelf = self.db.get_shelf_of_basket(qr_code)
        except Exception as e:
            occupied_shelf = None
            print(f"[WARN] error checking current shelf for '{qr_code}': {e}")

        # Skip if basket already stored
        if occupied_shelf is not None:
            if occupied_shelf == shelf_id:
                print(f"[INFO] Basket {qr_code} is already stored on shelf {shelf_id}")
            else:
                print(f"[ERROR] Basket {qr_code} is recorded on shelf {occupied_shelf} but mapped to {shelf_id}")
            return

        # Check target shelf status and availability
        try:
            with self.db.cursor() as c:
                c.execute("""
                    SELECT basket_id, active 
                    FROM shelf_data 
                    WHERE shelf_id = %s
                """, (shelf_id,))
                shelf_info = c.fetchone()
                
                if shelf_info:
                    current_basket = shelf_info['basket_id']
                    is_active = shelf_info['active']
                    
                    if is_active:
                        print(f"[ERROR] Shelf {shelf_id} is currently active and cannot be used")
                        return
                    
                    if current_basket:
                        if current_basket == qr_code:
                            print(f"[INFO] Basket {qr_code} already occupies shelf {shelf_id}")
                        else:
                            print(f"[ERROR] Shelf {shelf_id} is occupied by {current_basket}")
                        return
                else:
                    print(f"[ERROR] Shelf {shelf_id} not found in database")
                    return
                    
        except Exception as e:
            print(f"[WARN] Error checking shelf {shelf_id} status: {e}")
            return

        # Check shelf usability
        try:
            if not self.db.shelf_can_use(shelf_id):
                print("This shelf can't use now.")
                return
        except Exception as e:
            print(f"[WARN] error checking shelf usability: {e}")
            return

        # เพิ่มการล็อคเพื่อป้องกันการเพิ่มคำสั่งซ้ำ
        with self.db.cursor() as c:
            try:
                # ตรวจสอบว่ามีคำสั่ง PUT ค้างอยู่หรือไม่
                c.execute(
                    "SELECT 1 FROM queue_put WHERE basket = %s LIMIT 1",
                    (qr_code,)
                )
                if c.fetchone():
                    print(f"[PUT-ENQ] duplicate ignored for {qr_code}")
                    return
                
                # ถ้าระบบ ASRS ไม่พร้อม ให้ข้ามการ enqueue เพื่อไม่ให้รบกวนงานปัจจุบัน
                try:
                    if getattr(self, 'n_asrs_ready', None) is not None:
                        try:
                            ready = bool(self.n_asrs_ready.get_value())
                        except Exception:
                            ready = True
                    else:
                        ready = True
                except Exception:
                    ready = True

                if not ready:
                    print(f"[PUT-ENQ] ASRS busy, skipping enqueue for {qr_code}")
                    return

                c.execute(
                    "INSERT INTO queue_put (basket, x, y, z) VALUES (%s, %s, %s, %s)",
                    (qr_code, x, y, z)
                )
                print(f"[PUT-ENQ] Added {qr_code} to queue for shelf {shelf_id}")
            except Exception as e:
                print(f"[ERROR] Failed to process PUT for {qr_code}: {e}")

    def loop(self, callback=None, edge_only=True, validate=True):
        current_qr = None 
        while not self._stop:
            try:
                # Get PLC send flag status
                try:
                    flag = bool(self.n_flag.get_value())
                except Exception:
                    flag = False
                    time.sleep(self.interval)
                    continue

                # Reset states on flag transition to low
                if not flag and self._last_flag:
                    self._last_qr = None
                    current_qr = None
                    print("[QR] Flag dropped, reset states")
                    time.sleep(0.2) 

                if flag:
                    # Get QR code from PLC
                    try:
                        qr = str(self.n_qr.get_value() or "").strip()
                    except Exception as e:
                        print(f"[ERROR] Failed to read QR code: {e}")
                        time.sleep(self.interval)
                        continue

                    # Validate QR format
                    ok = (not validate) or (len(qr) == 10 and qr.startswith("B"))
                    
                    # Handle valid QR code
                    if ok and qr:
                        if current_qr != qr: 
                            current_qr = qr
                            self._last_qr = qr
                            print(f"[QR] Processing new code: {qr}")
                            
                            self._process_qr_code(qr)
                            
                            if callback:
                                callback(qr)
                                
                            # Send acknowledgment to PLC with current QR
                            if self.n_ack_basket is not None:
                                try:
                                    # (!!!) FIX: ใช้วิธี _dv_bool (!!!)
                                    print(f"[QR] Sending ACK for: {qr}")
                                    self.n_ack_basket.set_value(self._dv_bool(True))
                                    time.sleep(0.1)  # เพิ่มเวลา pulse
                                    self.n_ack_basket.set_value(self._dv_bool(False))
                                except Exception as e:
                                    print(f"[ERROR] Failed to send ACK: {e}")

                self._last_flag = flag
                time.sleep(self.interval)
            except Exception as e:
                print("[QR loop error]", e)
                time.sleep(self.interval)