from dataclasses import dataclass

@dataclass(frozen=True)
class OpcUaNodes:
    # QR read + flag from PLC
    basket_qr: str = "ns=4;i=2"
    plc_send_basket_qr: str = "ns=4;i=14"  # PLC -> WMS trigger

    # WMS receive basket qr acknowledgement
    wms_receive_basket_qr: str = "ns=4;i=17"

    # Main command + handshake
    cmd: str = "ns=4;i=3"               # 20-char command string
    wms_send_cmd: str = "ns=4;i=15"     # WMS -> PLC strobe
    plc_receive_cmd: str = "ns=4;i=16"  # PLC ack

    # No longer used in the simplified command flow; set to None
    plc_req_wms_clear: str = "ns=4;i=12"
    wms_clear_reply: str = "ns=4;i=13"

    # PLC sets cmd_complete when a command finishes; WMS pulses wms_complete_reply to acknowledge
    cmd_complete: str = "ns=4;i=12"
    wms_complete_reply: str = "ns=4;i=13"

    # Status flags
    asrs_ready: str = "ns=4;i=18"
    # asrs_auto_mode: set to True when the ASRS is in automatic mode
    asrs_auto_mode: str = "ns=4;i=19"
    # asrs_alarm: set to True when an alarm condition exists
    asrs_alarm: str = "ns=4;i=20"

    # Encoders for crane position
    crane_x: str = "ns=4;i=21"  # Int32
    crane_y: str = "ns=4;i=22"  # Int32
