import os
import re

# Basket ID normalization
_DIGITS = re.compile(r"^\d+$")
_BASKET = re.compile(r"^[bB](\d{1,9})$")  # B + up to 9 digits

def normalize_basket_id(value) -> str:
    """Convert int/str to standard 10-char basket ID: 'B' + 9 digits (zero-padded)"""
    if value is None:
        raise ValueError("basket_id is required")

    s = str(value).strip()
    if _DIGITS.match(s):
        n = int(s)
        if not (0 <= n <= 999_999_999):
            raise ValueError("basket number must be 0..999999999")
        return f"B{n:09d}"

    m = _BASKET.match(s)
    if m:
        n = int(m.group(1))
        if not (0 <= n <= 999_999_999):
            raise ValueError("basket number must be 0..999999999")
        return f"B{n:09d}"

    raise ValueError("invalid basket id/number format")

# Environment variable helpers
def _get_int_env(name: str, default: int) -> int:
    """Get integer value from environment variable with fallback"""
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default

def _get_float_env(name: str, default: float) -> float:
    """Get float value from environment variable with fallback"""
    try:
        return float(os.getenv(name, str(default)).strip())
    except Exception:
        return default

# Standard encoder values for home position (health-check/offset validation)
ENC_HOME_X = _get_int_env("ENC_HOME_X", 442200)
ENC_HOME_Y = _get_int_env("ENC_HOME_Y", 29500)

# Reference point for position (1,1)
REF_COL = _get_int_env("REF_COL", 1)
REF_ROW = _get_int_env("REF_ROW", 1)
ENC_REF_X = _get_int_env("ENC_REF_X", 470200)
ENC_REF_Y = _get_int_env("ENC_REF_Y", 28720)

# step ต่อ 1 ช่อง (pulse per cell)
STEP_X = _get_float_env("STEP_X", 20000.0)
STEP_Y = _get_float_env("STEP_Y", 17127.0)  # เฉลี่ยจากข้อมูลที่วัดจริง

def encoder_to_position(ex: int, ey: int) -> tuple[int, int]:
    """
    แปลง Encoder X,Y -> พิกัดช่อง (x_column, y_row)
    ใช้ "REF (1,1)" เป็น anchor ของ lattice เพื่อความแม่นยำกว่า Home
    """
    dx = ex - ENC_REF_X
    dy = ey - ENC_REF_Y
    x_col = REF_COL + round(dx / STEP_X)
    y_row = REF_ROW + round(dy / STEP_Y)
    if x_col < 1: x_col = 1
    if y_row < 1: y_row = 1
    return int(x_col), int(y_row)

def position_to_encoder(x_col: int, y_row: int) -> tuple[int, int]:
    """
    พิกัดช่อง -> encoder ศูนย์กลางโดยประมาณ (ใช้ตรวจ/แสดงผล)
    """
    ex = ENC_REF_X + (x_col - REF_COL) * STEP_X
    ey = ENC_REF_Y + (y_row - REF_ROW) * STEP_Y
    return int(round(ex)), int(round(ey))

def home_offset() -> tuple[int, int]:
    """
    Offset ของ Home เทียบกับจุดอ้างอิง (1,1) — ใช้ตรวจสุขภาพระบบได้
    """
    return (ENC_REF_X - ENC_HOME_X, ENC_REF_Y - ENC_HOME_Y)
