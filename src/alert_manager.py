import json
from nats.aio.client import Client as NATS
import logging
import os

logger = logging.getLogger(__name__)
    
async def send_alert(subject: str, body: str):
    try:

        nc = NATS()
        await nc.connect(os.getenv("NATS_URL"), user=os.getenv("NATS_USER"), password=os.getenv("NATS_PASS"))
        subject = f"alert.{subject}"
        msg_id = f"{subject}"

        js = nc.jetstream()
        ack = await js.publish(
            subject,
            json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
            headers={"Nats-Msg-Id": msg_id},
        )
        
        return ack
    except Exception as e:
        logger.error(
                json.dumps({
                        "EventCode": -1,
                        "Message": f"NATS error - Alert Manager: {e}"
                    })
            )
        return None
    