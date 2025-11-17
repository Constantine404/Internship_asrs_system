from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import Optional, Tuple
import asyncio
from .utils import normalize_basket_id

app = FastAPI(title="ASRS WMS API", version="1.3.0")

class PickRequest(BaseModel):
    number: Optional[int] = None
    basket_id: Optional[str] = None

class PickResponse(BaseModel):
    basket_id: str
    shelf_id: int
    x: int
    y: int
    z: int
    queue_id: int
    message: str = "enqueued"

class BasketStatus(BaseModel):
    basket_id: str
    mapped_shelf_id: Optional[int] = None
    mapped_xyz: Optional[Tuple[int,int,int]] = None
    occupied_shelf_id: Optional[int] = None

# System status exposed via WebSocket

def _resolve_basket_id(req: PickRequest | None, path_number: Optional[int] = None) -> str:
    """Convert request parameters to normalized basket ID string"""
    if path_number is not None:
        return normalize_basket_id(path_number)
    if req is None:
        raise HTTPException(400, "missing request body")
    try:
        if req.number is not None:
            return normalize_basket_id(req.number)
        if req.basket_id is not None:
            return normalize_basket_id(req.basket_id)
    except ValueError as e:
        raise HTTPException(400, str(e))
    raise HTTPException(400, "either 'number' or 'basket_id' is required")

@app.post("/wms/pick", response_model=PickResponse)
def wms_pick(req: PickRequest):
    pg = getattr(app.state, "pg", None)
    if pg is None:
        raise HTTPException(500, "DB not ready")

    basket_id = _resolve_basket_id(req)
    mapping = pg.get_mapping_for_basket(basket_id)
    if not mapping:
        raise HTTPException(404, f"basket '{basket_id}' not found in mapping")
    shelf_id, x, y, z = mapping
    # Check shelf usability before enqueuing pick
    try:
        # If the shelf is marked unusable, do not enqueue and return an error
        if not pg.shelf_can_use(shelf_id):
            raise HTTPException(400, "This shelf can't use now.")
    except Exception as e:
        # If the DB check fails, return an internal error
        raise HTTPException(500, f"Error checking shelf usability: {e}")

    qid = pg.enqueue_pick(basket_id, x, y, z)
    return PickResponse(basket_id=basket_id, shelf_id=shelf_id, x=x, y=y, z=z, queue_id=qid)

@app.post("/wms/pick/{number}", response_model=PickResponse)
def wms_pick_number(number: int):
    pg = getattr(app.state, "pg", None)
    if pg is None:
        raise HTTPException(500, "DB not ready")
    basket_id = _resolve_basket_id(None, path_number=number)
    mapping = pg.get_mapping_for_basket(basket_id)
    if not mapping:
        raise HTTPException(404, f"basket '{basket_id}' not found in mapping")
    shelf_id, x, y, z = mapping
    # Check shelf usability before enqueuing pick by number
    try:
        if not pg.shelf_can_use(shelf_id):
            raise HTTPException(400, "This shelf can't use now.")
    except Exception as e:
        raise HTTPException(500, f"Error checking shelf usability: {e}")

    qid = pg.enqueue_pick(basket_id, x, y, z)
    return PickResponse(basket_id=basket_id, shelf_id=shelf_id, x=x, y=y, z=z, queue_id=qid)

@app.get("/wms/status/basket/{basket}", response_model=BasketStatus)
def basket_status(basket: str):
    pg = getattr(app.state, "pg", None)
    if pg is None:
        raise HTTPException(500, "DB not ready")
    try:
        norm_id = normalize_basket_id(basket)
    except ValueError as e:
        raise HTTPException(400, str(e))
    mapping = pg.get_mapping_for_basket(norm_id)
    occ = pg.get_shelf_of_basket(norm_id)
    return BasketStatus(
        basket_id=norm_id,
        mapped_shelf_id=(mapping[0] if mapping else None),
        mapped_xyz=(mapping[1], mapping[2], mapping[3]) if mapping else None,
        occupied_shelf_id=occ,
    )

# Removed REST endpoint for system status; status is provided via WebSocket (/ws/status/system).

@app.get("/wms/normalize/{value}")
def normalize(value: str):
    try:
        return {"basket_id": normalize_basket_id(value)}
    except ValueError as e:
        raise HTTPException(400, str(e))

# --- Reset endpoints ---
@app.post("/wms/reset/queue")
def reset_queue():
    """
    Clear all pending commands from the queues in database.
    This is a soft reset that only affects pending operations.

    Returns:
        dict: Status of the queue reset operation.
    """
    pg = getattr(app.state, "pg", None)
    if pg is None:
        raise HTTPException(500, "DB not ready")

    try:
        # Clear both PICK and PUT queues
        pg.clear_all_queues()
        return {
            "status": "success",
            "message": "All queues cleared successfully",
            "details": {
                "pick_queue": "cleared",
                "put_queue": "cleared"
            }
        }
    except Exception as e:
        raise HTTPException(500, f"Failed to clear queues: {str(e)}")

@app.post("/wms/reset/system")
def reset_system():
    """
    Complete system reset including:
    - Clear all queues in database
    - Reset mover state and commands
    - Reset QR listener state
    - Reset all ongoing operations

    Returns:
        dict: Detailed status of all reset operations.
    """
    error_messages = []
    success_messages = []

    # Access the database and mover from FastAPI state
    pg = getattr(app.state, "pg", None)
    mover = getattr(app.state, "mover", None)
    qr = getattr(app.state, "qr", None)

    if pg is None:
        raise HTTPException(500, "DB not ready")
    # 1. Clear database queues
    try:
        pg.clear_all_queues()
        success_messages.append("Queue cleared successfully")
    except Exception as e:
        error_messages.append(f"Error clearing queue: {str(e)}")

    # 2. Reset mover state
    if mover is not None:
        try:
            mover.reset_current_command()
            success_messages.append("Mover reset successfully")
        except Exception as e:
            error_messages.append(f"Error resetting mover: {str(e)}")
    
    # 3. Reset QR listener state
    if qr is not None:
        try:
            qr.reset_state()
            success_messages.append("QR listener reset successfully")
        except Exception as e:
            error_messages.append(f"Error resetting QR listener: {str(e)}")
    
    # Return status
    if error_messages:
        return {
            "status": "partial",
            "success": success_messages,
            "errors": error_messages,
            "message": "Reset completed with some errors"
        }
    else:
        return {
            "status": "success",
            "success": success_messages,
            "message": "Reset completed successfully"
        }

# WebSocket endpoint to stream system status (ready, auto, alarm) to WMS.
@app.websocket("/ws/status/system")
async def ws_system_status(ws: WebSocket):
    """
    A WebSocket endpoint that streams the ASRS system status to the client.

    Upon connection, the server will repeatedly read the ready, auto mode and
    alarm flags from the ASRS mover and send them as JSON objects to the
    connected WebSocket client.  The loop runs until the client disconnects
    or an error occurs.  Read operations are executed in a threadpool to
    avoid blocking the event loop.

    The JSON message format is:

        {"ready": true/false, "auto_mode": true/false, "alarm": true/false}

    """
    mover = getattr(app.state, "mover", None)
    # Reject connection if the mover is unavailable
    if mover is None:
        # Accept then immediately close so that the client receives a valid
        # WebSocket handshake response before termination
        await ws.accept()
        await ws.close()
        return
    await ws.accept()
    loop = asyncio.get_event_loop()
    try:
        while True:
            if getattr(app.state, "shutting_down", False):
                await ws.close()
                break
            try:
                # Run the blocking OPC UA reads in the default thread pool
                ready = await loop.run_in_executor(None, lambda: bool(mover.n_ready.get_value()))
                auto  = await loop.run_in_executor(None, lambda: bool(mover.n_auto.get_value()))
                alarm = await loop.run_in_executor(None, lambda: bool(mover.n_alarm.get_value()))
            except asyncio.CancelledError:
                await ws.close()
                break
            except Exception as e:
                # Send error then break out of loop on read failure
                await ws.send_json({"error": str(e)})
                break
            # Include timing information for last put/pick commands (rounded to 2 decimals)
            try:
                last_put  = mover.last_durations.get("put") if hasattr(mover, "last_durations") else None
                last_pick = mover.last_durations.get("pick") if hasattr(mover, "last_durations") else None
                payload = {
                    "ready": ready,
                    "auto_mode": auto,
                    "alarm": alarm,
                    "last_put_seconds": None if last_put is None else round(last_put, 2),
                    "last_pick_seconds": None if last_pick is None else round(last_pick, 2),
                }
            except Exception:
                # Fallback if reading durations fails
                payload = {
                    "ready": ready,
                    "auto_mode": auto,
                    "alarm": alarm,
                    "last_put_seconds": None,
                    "last_pick_seconds": None,
                }
            await ws.send_json(payload)
            # Wait a short period before reading again. Adjust interval as needed.
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        # Client disconnected; nothing to do
        pass
    except Exception:
        # Silently ignore other errors; connection will be closed automatically
        pass
